(ns inspect-hql.extract
  (:require
    [clojure.set :as set]
    [inspect-hql.parse :as parse]))

(defmacro let-v [binding & body]
  `(let [~binding ~'v] ~@body))

(defn filter-v [m]
  (into {}
        (for [[k v] m :when v]
          [k v])))
(defn value-map [f m]
  (->> m vals (map f) (zipmap (keys m))))

(defn some-let [f s]
  (some #(when (f %) %) s))
(defn some-exception [f s msg]
  (or (some f s) (throw (Exception. msg))))
(defn find-in [v [k & ks]]
  (cond
    (number? k) (find-in (nth v k) ks)
    k
    (when-let [child (some-let #(and (coll? %) (-> % first (= k))) v)]
      (find-in child ks))
    :else v))

(defn _get-cols [v alias select?]
  (when (vector? v)
    (case (v 0)
      "TOK_SELEXPR"
      (let-v [_ v [alias]]
             (_get-cols v alias true))
      "."
      (when select?
        [{:col (get-in v [2 0])
          :table (get-in v [1 1 0])
          :alias alias}])
      "TOK_TABLE_OR_COL"
      (when select?
        [{:col (get-in v [1 0])
          :alias alias}])
      "TOK_ALLCOLREF"
      (when select?
        [{:col "*"
          :table (get-in v [1 1 0])
          :alias alias}])
      "TOK_SUBQUERY" nil ;;don't go in there
      (mapcat #(_get-cols % alias select?) v))))

(defn get-cols [v]
  (->> (_get-cols v nil true) (map filter-v) distinct))

(defn get-select [v]
  (->> (_get-cols v nil false) (map filter-v) distinct))

(defn _get-tables [v]
  (when (vector? v)
    (case (v 0)
      "TOK_TABREF"
      (let-v [_ [_ [table]] [alias]]
             [[(or alias table) table]])
      "TOK_SUBQUERY"
      (let-v [_ subquery [alias]]
             [[alias alias]])
      (mapcat _get-tables v))))

(defn get-tables [v]
  (->> v _get-tables (into {})))

(declare get-full)

(defn _get-subqueries [v]
  (when (vector? v)
    (case (v 0)
      "TOK_SUBQUERY"
      (let-v [_ subquery [alias]]
             [[alias (get-full subquery)]])
      (mapcat _get-subqueries v))))

(defn get-subqueries [v]
  (->> v _get-subqueries (into {})))

(defn get-full [v]
  {:select (get-select v)
   :cols (get-cols v)
   :tables (get-tables v)
   :subqueries (get-subqueries v)})

(defn _get-schema [v]
  (when (vector? v)
    (case (v 0)
      "TOK_TABCOLLIST"
      (for [[_ [col]] (rest v)]
        {:col col})
      (mapcat _get-schema v))))
(defn get-schema [v]
  [(find-in v ["TOK_TABNAME" 1 0])
   (_get-schema v)])
(defn get-schemas [vs]
  (into {}
        (for [[type :as v] vs
              :when (= "TOK_CREATETABLE" type)]
          (get-schema v))))

(defmacro cond-let [v cond1 res1 & rest]
  (if rest
    `(if-let [~v ~cond1]
      ~res1
      (cond-let ~v ~@rest))
    `(when-let [~v ~cond1] ~res1)))

(defn _expand-stars [select tables subqueries regular-schema]
  (for [{:keys [col alias table] :as select-info} select
        table-alias (cond
                      (not= "*" col) [nil] ;;placeholder
                      table [table]
                      :else (keys tables))
        col (cond-let x
                      (not= "*" col) [select-info]
                      (subqueries table-alias)
                      (for [{:keys [col alias]} (:select x)]
                        {:col (or alias col) :table table-alias})
                      (regular-schema table-alias)
                      (for [info x]
                        (assoc info :table table-alias))
                      :else (throw (Exception. (str "no table for " table-alias))))]
    col))

(defn expand-stars [{:keys [subqueries tables] :as m} regular-schema]
  (let [subqueries (value-map #(expand-stars % regular-schema) subqueries)
        ;; this must come after
        regular-schema (comp regular-schema tables)]
    (-> m
        (assoc :subqueries subqueries)
        (update :select _expand-stars tables subqueries regular-schema)
        (update :cols _expand-stars tables subqueries regular-schema))))

(defn _assign-tables [select all-colls]
  (for [{:keys [table col] :as info} select]
    (if table
      info
      (some-exception
       #(when (or
               (-> % :alias (= col))
               (-> % :col (= col))) (assoc info :table (:src-table %)))
       all-colls
       (str "No col found for " info)))))
(defn assign-tables [{:keys [subqueries tables] :as m} regular-schema]
  (let [subqueries (value-map #(assign-tables % regular-schema) subqueries)
        all-colls (for [[alias table] tables
                        col (or
                             (some-> alias subqueries :select)
                             (regular-schema table))]
                    (assoc col :src-table alias))]
    (-> m
        (assoc :subqueries subqueries)
        (update :select _assign-tables all-colls)
        (update :cols _assign-tables all-colls))))

(defn trim-cols [{:keys [select cols] :as m}]
  (let [select-set (->> select (map #(select-keys % [:col :table])) set)
        remover #(-> % (select-keys [:col :table]) select-set)]
    (assoc m
           :cols (remove remover cols)
           :required #{})))

(defn get-queries [vs schemas]
  (into {}
        (for [[type :as v] vs
              :when (= "TOK_QUERY" type)]
          [(or
            (find-in v ["TOK_INSERT" "TOK_DESTINATION" "TOK_TAB" "TOK_TABNAME" 1 0])
            "tmp")
           (-> v get-full (expand-stars schemas) (assign-tables schemas) trim-cols)])))

(defn _populate-required [queries ks]
  (let [{:keys [subqueries cols select tables]} (get-in queries ks)
        queries (-> queries
                    (assoc :deps #{})
                    (update :done conj ks))
        queries
        (reduce
         (fn [queries {:keys [col table]}]
           (if (subqueries table)
             (let [new-dep (conj ks table)]
               (-> queries
                   (update :deps conj new-dep)
                   (update-in (conj ks :required) conj col)))
             (let [real-table (tables table)]
               (assert real-table "no real table found")
               (-> queries
                   (update :deps conj [real-table])
                   (update-in [real-table :required] conj col)))))
         queries
         (concat cols select))]
    (reduce _populate-required
            queries
            (set/difference (:deps queries) (:done queries)))))

(defn populate-required [queries roots]
  (->> roots (map vector) (reduce _populate-required queries)))

(use 'clojure.pprint)
(let [parsed (-> "c.hql" slurp (parse/parse-all parse/m))
      schemas (get-schemas parsed)
      queries (get-queries parsed schemas)]
  (pprint queries))
