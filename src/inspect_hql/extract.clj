(ns inspect-hql.trim
  (:require
    [inspect-hql.parse :as parse]))

(def parsed (parse/parse (slurp "b.hql") {}))
(use 'clojure.pprint)
(pprint parsed)

(defmacro let-v [binding & body]
  `(let [~binding ~'v] ~@body))

(defn filter-v [m]
  (into {}
        (for [[k v] m :when v]
          [k v])))

(defn _get-select [v alias]
  (when (vector? v)
    (case (v 0)
      "TOK_SELEXPR"
      (let-v [_ v [alias]]
             (_get-select v alias))
      "." [{:col (get-in v [2 0])
            :table (get-in v [1 1 0])
            :alias alias}]
      "TOK_TABLE_OR_COL" [{:col (get-in v [1 0])
                           :alias alias}]
      "TOK_ALLCOLREF" [{:col "*"
                        :table (get-in v [1 1 0])
                        :alias alias}]
      "TOK_SUBQUERY" nil ;;don't go in there
      (mapcat #(_get-select % alias) v))))

(defn get-select [v]
  (->> (_get-select v nil) (map filter-v) distinct))

(defn _get-tables [v]
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

(defn get-tables [v]
  (distinct (_get-tables v)))

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
   :tables (get-tables v)
   :subqueries (get-subqueries v)})

(defn get-schema [v]
  (when (vector? v)
    (case (v 0)
      "TOK_TABCOLLIST"
      (->> v rest (map #(get-in % [1 0])))
      (mapcat get-schema v))))
