(ns inspect-hql.extract
  (:require
    [clojure.set :as set]
    [inspect-hql.parse :as parse]))

(defmacro let-v [binding & body]
  `(let [~binding ~'v] ~@body))
(defmacro km [& syms]
  (zipmap (map keyword syms) syms))

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

(defn _get-cols [v]
  (when (vector? v)
    (case (v 0)
      "."
      [{:col (get-in v [2 0])
        :table (get-in v [1 1 0])}]
      "TOK_TABLE_OR_COL"
      [{:col (get-in v [1 0])}]
      "TOK_ALLCOLREF"
      [{:col "*"
        :table (get-in v [1 1 0])}]
      "TOK_SUBQUERY" nil
      (mapcat _get-cols v))))

(defn get-cols [v]
  (->> v _get-cols (map filter-v) distinct))

(defn get-select [v]
  (when (vector? v)
    (case (v 0)
      "TOK_SELEXPR"
      (let [[_ v [alias]] v
            cols (_get-cols v)]
        [{:cols cols
          :alias (or alias (-> cols first :col))}])
      "TOK_SUBQUERY" nil
      (mapcat get-select v))))

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

(defn _expand-stars [cols tables subqueries regular-schema]
  (for [{:keys [col table] :as col-info} cols
        table-alias (cond
                      (not= "*" col) [nil] ;;placeholder
                      table [table]
                      :else (keys tables))
        col (cond-let x
                      (not= "*" col) [col-info]
                      (subqueries table-alias)
                      (for [{:keys [alias]} (:select x)]
                        {:col alias :table table-alias})
                      (regular-schema table-alias)
                      (for [info x]
                        (assoc info :table table-alias))
                      :else (throw (Exception. (str "no table for " table-alias))))]
    col))

(defn expand-stars-select [select tables subqueries regular-schema]
  (map #(update % :cols _expand-stars tables subqueries regular-schema) select))

(defn expand-stars [{:keys [subqueries tables] :as m} regular-schema]
  (let [subqueries (value-map #(expand-stars % regular-schema) subqueries)
        ;; this must come after
        regular-schema (comp regular-schema tables)]
    (-> m
        (assoc :subqueries subqueries)
        (update :select expand-stars-select tables subqueries regular-schema)
        (update :cols _expand-stars tables subqueries regular-schema))))

(defn _assign-tables [cols all-cols]
  (for [{:keys [table col] :as info} cols]
    (assoc info :table
           (or
            table
            (some
             #(when (-> % :col (= col)) (:src-table %)) all-cols)
            (throw (Exception. (str "no col found for " info)))))))

(defn assign-tables-select [select all-cols]
  (map #(update % :cols _assign-tables all-cols) select))

(defn- alias->col [{:keys [alias]}]
  {:col alias})
(defn assign-tables [{:keys [subqueries tables] :as m} regular-schema]
  (let [subqueries (value-map #(assign-tables % regular-schema) subqueries)
        all-cols (for [[alias table] tables
                        col (or
                             (some->> alias subqueries :select (map alias->col))
                             (regular-schema table)
                             (throw (Exception. (str "no schema found for table " table))))]
                    (assoc col :src-table alias))]
    (-> m
        (assoc :subqueries subqueries)
        (update :select assign-tables-select all-cols)
        (update :cols _assign-tables all-cols))))

(defn merge-schema [{:keys [select] :as m} k schemas]
  (if-let [schema (schemas k)]
    (do
      (assert (= (count select) (count schema)) (str "schema mismatch for " k))
      (assoc m :select (map #(assoc %1 :alias (:col %2)) select schema)))
    m))

(defn trim-cols [{:keys [select cols subqueries] :as m}]
  (let [subqueries (value-map trim-cols subqueries)
        remover (->> select (mapcat :cols) set)]
    (assoc m
           :subqueries subqueries
           :cols (remove remover cols)
           :required #{})))

(defn get-queries [vs schemas]
  (into {}
        (for [[type :as v] vs
              :when (= "TOK_QUERY" type)
              :let [k
                    (or
                     (find-in v ["TOK_INSERT" "TOK_DESTINATION" "TOK_TAB" "TOK_TABNAME" 1 0])
                     "tmp")]]
          [k
           (-> v
               get-full
               (expand-stars schemas)
               (assign-tables schemas)
               (merge-schema k schemas)
               trim-cols)])))

(defn _populate-required [queries kv]
  (let [{:keys [subqueries cols select tables required]} (get-in queries kv)
        queries (-> queries
                    (assoc :deps #{}))
        actual-required
        (concat (filter #(-> % :alias required) select) cols)
        queries
        (reduce
         (fn [queries {:keys [col table]}]
           (if (subqueries table)
             (let [new-dep (conj kv :subqueries table)]
               (-> queries
                   (update :deps conj new-dep)
                   (update-in (conj new-dep :required) conj col)))
             (let [real-table (tables table)]
               (assert real-table (format "%s not in tables for kv %s" table kv))
               (if (queries real-table)
                 (-> queries
                     (update :deps conj [real-table])
                     (update-in [real-table :required] conj col))
                 (do
                   (printf "Warning: table %s not found\n" real-table)
                   queries)))))
         queries
         actual-required)]
    (reduce _populate-required
            queries
            (:deps queries))))

(defn _initial-required [queries root]
  (-> queries
      (assoc-in [root :required] (->> (get-in queries [root :select]) (map :alias) set))
      (_populate-required [root])))

(defn populate-required [queries roots]
  (reduce _initial-required queries roots))

(use 'clojure.pprint)
(let [parsed (-> "c.hql" slurp (parse/parse-all parse/m))
      ;schemas (get-schemas parsed)
      ;queries (get-queries parsed schemas)
      ]
  (-> parsed second get-select pprint)
  ;(populate-required queries ["tmp"])
  )
