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

(defn _get-cols [v]
  (when (vector? v)
    (case (v 0)
      "TOK_SELEXPR"
      (let-v [_ v [alias]]
        (case (v 0)
          "." [{:col (get-in v [2 0])
                :table (get-in v [1 1 0])
                :alias alias}]
          "TOK_TABLE_OR_COL" [{:col (get-in v [1 0])
                               :alias alias}]
          "TOK_ALLCOLREF" [{:col "*"
                            :alias alias}]
          (throw (Exception. "unimplemented"))))
      "." [{:col (get-in v [2 0])
            :table (get-in v [1 1 0])}]
      "TOK_TABLE_OR_COL" [{:col (get-in v [1 0])}]
      "TOK_ALLCOLREF" [{:col "*"}]
      "TOK_SUBQUERY" nil ;;don't go in there
      (mapcat _get-cols v))))

(defn get-cols [v]
  (->> v _get-cols (map filter-v) distinct))

(defn _get-tables [v]
  (when (vector? v)
    (case (v 0)
      "TOK_TABREF"
      (let-v [_ [_ [table]] [alias]]
        [{:table table
          :alias (or alias table)}])
      "TOK_SUBQUERY" nil ;; don't look here
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
  {:cols (get-cols v)
   :tables (get-tables v)
   :subqueries (get-subqueries v)})

(pprint (get-full parsed))

["nil"
 ["TOK_QUERY"
  ["TOK_FROM"
   ["TOK_JOIN"
    ["TOK_JOIN"
     ["TOK_SUBQUERY"
      ["TOK_QUERY"
       ["TOK_FROM"
        ["TOK_JOIN"
         ["TOK_SUBQUERY"
          ["TOK_QUERY"
           ["TOK_FROM" ["TOK_TABREF" ["TOK_TABNAME" ["table3"]]]]
           ["TOK_INSERT"
            ["TOK_DESTINATION" ["TOK_DIR" ["TOK_TMP_FILE"]]]
            ["TOK_SELECT" ["TOK_SELEXPR" ["TOK_ALLCOLREF"]]]]]
          ["table2"]]
         ["TOK_TABREF" ["TOK_TABNAME" ["side_table3"]]]
         ["="
          ["." ["TOK_TABLE_OR_COL" ["table2"]] ["id"]]
          ["." ["TOK_TABLE_OR_COL" ["side_table3"]] ["id"]]]]]
       ["TOK_INSERT"
        ["TOK_DESTINATION" ["TOK_DIR" ["TOK_TMP_FILE"]]]
        ["TOK_SELECT"
         ["TOK_SELEXPR"
          ["." ["TOK_TABLE_OR_COL" ["table2"]] ["col2"]]
          ["col1"]]]]]
      ["table1"]]
     ["TOK_TABREF" ["TOK_TABNAME" ["side_table1"]]]
     ["="
      ["." ["TOK_TABLE_OR_COL" ["table1"]] ["id"]]
      ["." ["TOK_TABLE_OR_COL" ["side_table1"]] ["id"]]]]
    ["TOK_TABREF" ["TOK_TABNAME" ["side_table2"]] ["side_alias"]]
    ["="
     ["." ["TOK_TABLE_OR_COL" ["table1"]] ["id"]]
     ["." ["TOK_TABLE_OR_COL" ["side_table2"]] ["id"]]]]]
  ["TOK_INSERT"
   ["TOK_DESTINATION" ["TOK_DIR" ["TOK_TMP_FILE"]]]
   ["TOK_SELECT"
    ["TOK_SELEXPR"
     ["." ["TOK_TABLE_OR_COL" ["table1"]] ["col1"]]
     ["alias1"]]
    ["TOK_SELEXPR" ["TOK_TABLE_OR_COL" ["col1"]]]
    ["TOK_SELEXPR" ["TOK_ALLCOLREF"]]]]]
 ["<EOF>"]]
