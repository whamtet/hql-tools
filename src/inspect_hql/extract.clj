(ns inspect-hql.extract
  (:require
    [clojure.string :as string]
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
      (let-v [_ v [alias]]
             [{:cols (get-cols v)
               :alias alias}])
      "TOK_SUBQUERY" nil
      (mapcat get-select v))))

(defn join-tabnames [[_ & names]]
  (->> names (map first) (string/join ".")))
(defn _get-tables [v]
  (when (vector? v)
    (case (v 0)
      "TOK_TABREF"
      (let [[_ tabnames [alias]] v
            table (join-tabnames tabnames)]
        [[(or alias table) table]])
      "TOK_SUBQUERY"
      (let-v [_ subquery [alias]]
             [[alias alias]])
      (mapcat _get-tables v))))

(defn get-tables [v]
  (->> v _get-tables (into {})))

(declare get-full)

(defn merge-union-select [{alias1 :alias cols1 :cols}
                          {alias2 :alias cols2 :cols}]
  {:alias (or alias1 alias2)
   :cols (distinct (concat cols1 cols2))})
(defn merge-union [{select1 :select cols1 :cols tables1 :tables subqueries1 :subqueries}
                   {select2 :select cols2 :cols tables2 :tables subqueries2 :subqueries}]
  (assert (= (count select1) (count select2)))
  {:select (map merge-union-select select1 select2)
   :cols (distinct (concat cols1 cols2))
   :tables (merge tables1 tables2)
   :subqueries (merge subqueries1 subqueries2)})

(defn _get-subqueries [v]
  (when (vector? v)
    (case (v 0)
      "TOK_SUBQUERY"
      (let [[_ subquery [alias]] v
            union-statements
            (if-let [union (find-in subquery ["TOK_FROM" "TOK_SUBQUERY" "TOK_UNIONALL"])]
              (rest union)
              [subquery])]
        [[alias (->> union-statements (map get-full) (reduce merge-union))]])
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
      "TOK_TABLEPARTCOLS"
      (for [[_ [col]] (rest v)]
        {:col col :partition? true})
      (mapcat _get-schema v))))
(defn get-schema [v]
  [(join-tabnames (find-in v ["TOK_TABNAME"]))
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

(defn expand-star-select [{:keys [cols alias]} tables subqueries alias-schema]
  (if (every? #(-> % :col (not= "*")) cols)
    [{:cols cols :alias (or alias (-> cols first :col))}]
    (if (= 1 (count cols))
      (for [{:keys [table]} cols
            table-alias (if table [table] (keys tables))
            col (cond-let x
                          (subqueries table-alias)
                          (for [{:keys [alias]} (:select x)] ;; alias predefined recursively
                            {:cols [{:col alias :table table-alias}] :alias alias})
                          (alias-schema table-alias)
                          (for [synthetic-col x]
                            {:cols [{:col (:col synthetic-col) :table table-alias}] :alias synthetic-col})
                          :else (throw (Exception. (str "no table for " table-alias))))]
        col)
      (throw (Exception. "multi stars unimplemented")))))

(defn expand-stars-select [select tables subqueries alias-schema]
  (mapcat #(expand-star-select % tables subqueries alias-schema) select))

(defn expand-stars-cols [cols]
  (remove #(-> % :col (= "*")) cols))

(defn expand-stars [{:keys [subqueries tables] :as m} regular-schema]
  (let [subqueries (value-map #(expand-stars % regular-schema) subqueries)
        ;; this must come after
        alias-schema (comp regular-schema tables)]
    (-> m
        (assoc :subqueries subqueries)
        (update :select expand-stars-select tables subqueries alias-schema)
        (update :cols expand-stars-cols))))

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
(defn- synthetic->col [synthetic]
  {:col (:col synthetic)})
(defn assign-tables [{:keys [subqueries tables] :as m} regular-schema]
  (let [subqueries (value-map #(assign-tables % regular-schema) subqueries)
        all-cols (for [[alias table] tables
                        col (or
                             (some->> alias subqueries :select (map alias->col))
                             (some->> table regular-schema (map synthetic->col))
                             (throw (Exception. (str "no schema found for table " table))))]
                    (assoc col :src-table alias))]
    (-> m
        (assoc :subqueries subqueries)
        (update :select assign-tables-select all-cols)
        (update :cols _assign-tables all-cols))))

(defn merge-schema [{:keys [select] :as m} k schemas]
  (if-let [schema (some->> k schemas (remove :partition?))]
    (do
      (assert (= (count select) (count schema)) (format "schema mismatch for %s: %s" k (pr-str schema)))
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
                     (some-> v (find-in ["TOK_INSERT" "TOK_DESTINATION" "TOK_TAB" "TOK_TABNAME"]) join-tabnames)
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
        actual-required
        (->> select
             (filter #(-> % :alias required))
             (mapcat :cols)
             (concat cols))
        excess (remove #(-> % :alias required) select)
        queries (-> queries
                    (assoc :deps #{})
                    (assoc-in (conj kv :excess) excess))
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
      schemas (get-schemas parsed)
      queries (get-queries parsed schemas)
      ]
  (pprint
   (populate-required queries ["tmp"])))

;(-> "d.hql" slurp (parse/parse-all parse/m) pprint)
