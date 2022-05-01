(ns inspect-hql.extract
  (:require
    [inspect-hql.parse :as parse]))

(defmacro let-v [binding & body]
  `(let [~binding ~'v] ~@body))

(defn filter-v [m]
  (into {}
        (for [[k v] m :when v]
          [k v])))
(defn zipmap-by [f1 f2 s] (zipmap (map f1 s) (map f2 s)))
(defn value-map [f m]
  (->> m vals (map f) (zipmap (keys m))))

(defn some-let [f s]
  (some #(when (f %) %) s))
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

(defn get-tables [v]
  (when (vector? v)
    (case (v 0)
      "TOK_TABREF"
      (let-v [_ [_ [table]] [alias]]
        [{:table table
          :alias (or alias table)}])
      "TOK_SUBQUERY"
      (let-v [_ subquery [alias]]
             [{:table alias :alias alias}])
      (mapcat _get-tables v))))

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
      (->> v rest (map #(get-in % [1 0])))
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

(defn _expand-stars [select tables subqueries get-real-table regular-schema]
  (for [{:keys [col alias table] :as select-info} select
        table-alias (cond
                      (not= "*" col) [nil] ;;placeholder
                      table [table]
                      :else (map :alias tables))
        col (cond-let x
                      (not= "*" col) [select-info]
                      (subqueries table-alias)
                      (for [{:keys [col alias]} (:select x)]
                        {:col (or alias col) :table table-alias})
                      (-> table-alias get-real-table regular-schema)
                      (for [col x]
                        {:col col :table table-alias})
                      :else (throw (Exception. (str "no table for " table-alias))))]
    col))

(defn expand-stars [{:keys [select subqueries tables] :as m} regular-schema]
  (let [get-real-table (zipmap-by :alias :table tables)
        subqueries (value-map #(expand-stars % regular-schema) subqueries)]
    (-> m
        (assoc :subqueries subqueries)
        (update :select _expand-stars tables subqueries get-real-table regular-schema)
        (update :cols _expand-stars tables subqueries get-real-table regular-schema))))

(use 'clojure.pprint)
(let [parsed (-> "b.hql" slurp (parse/parse-all parse/m))
      schemas (get-schemas parsed)
      subquery (-> parsed last get-full)]
  (pprint (expand-stars subquery schemas)))
