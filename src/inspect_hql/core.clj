(ns inspect-hql.core
  (:require
    [clojure.string :as string]
    [rhizome.viz :as rhizome])
  (:import
    java.io.File))

(def files (->> "../slct-if-resources/hql/if_rkarte_inflow_search_word_daily"
            File.
            file-seq
            (filter #(.endsWith (.getName %) ".hql"))))

(defn- start-group? [^String s]
  (let [s (.toLowerCase s)]
    (and (.contains s "insert") (.contains s "table"))))
(defn- end-group? [^String s]
  (.contains s ";"))

(defn- group-lines [lines]
  (loop [done []
         curr []
         [line & rest] lines]
    (if line
      (cond
        (end-group? line)
        (if (empty? curr)
          (recur done [] rest)
          (recur (conj done (conj curr line)) [] rest))
        (start-group? line)
        (recur done (conj curr line) rest)
        (not-empty curr)
        (recur done (conj curr line) rest)
        :else
        (recur done [] rest))
      (if (not-empty curr)
        (conj done curr)
        done))))

(defn- slurp-groups [f]
  (-> f slurp (.split "\n") group-lines))
  
(defn- group->table-name [group]
  (->> group first (re-find #"TABLE (\S+)") second))

(def raw (for [f files
               group (slurp-groups f)]
               {:f f
                :group group
                :table (group->table-name group)}))

(defn- max-by [f [x1 & rest]]
  (first
    (reduce
      (fn [[x1 y1] x2]
        (let [y2 (f x2)]
          (if (> y2 y1)
            [x2 y2]
            [x1 y1])))
      [x1 (f x1)]
      rest)))
(defn- group-max [f1 f2 s]
  (for [[_ group] (group-by f1 s)]
    (max-by f2 group)))

(defn- raw->node [raw]
  (select-keys raw [:table :f]))
(def all-tables (->> raw (map raw->node) distinct))

(defn connected? [{:keys [table group]}]
  (let [s (->> group 
            (drop-while #(not (.contains (.toLowerCase %) "from")))
            (string/join "\n"))]
    #(and 
      (.contains s (:table %)) 
      (not= table (:table %)))))

(defn max-internal [{:keys [f]}]
  #(if (-> % :f (= f)) 1 0))

(defn reduce-substrings [[a & rest]]
  (when a
    (reduce
      (fn [v next]
        (conj
          (if (->> v peek :table (.contains (:table next)))
            (pop v)
            v)
          next))
      [a]
      rest)))

(def g
  (into {}
    (for [item raw]
      [(raw->node item)
       (->> all-tables
            (filter (connected? item))
            (group-max :table (max-internal item))
            (sort-by :table)
            reduce-substrings)])))


(def node->cluster
  (into {}
    (for [item raw]
      [(raw->node item) (-> item :f .getName)])))

(defn- node->descriptor [{:keys [table]}] {:label table})
(defn- cluster->descriptor [s] {:label s})

(spit "sql.svg"
  (rhizome/graph->svg (keys g) g
     :node->descriptor node->descriptor
     :node->cluster node->cluster
     :cluster->descriptor cluster->descriptor))