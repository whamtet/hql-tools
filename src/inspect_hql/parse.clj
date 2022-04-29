(ns inspect-hql.parse
  (:import
    org.apache.hadoop.hive.ql.parse.ParseDriver
    org.apache.hadoop.hive.ql.lib.Node))

(defn node->tree [^Node node]
  (when (instance? Node node)
    (->> node
         .getChildren
         (map node->tree)
         (list* (str node))
         vec)))

(defn parse [^String src]
  (println "parsing" src)
  (-> (ParseDriver.)
      (.parse src)
      .getTree
      node->tree))

(def sub-regex #"\$\{([^\}]+)}")

(defn replace-env [s m]
  (->> s
       (re-seq #"\$\{([^\}]+)}")
       distinct
       (reduce
         (fn [s [find k]]
           (if-let [replacement (m k)]
             (.replace s find replacement)
             s))
         s)))

(defn get-hivevar [^String s]
  (when-let [set-statement (some-> s .trim (.split "set hivevar:") second)]
    (let [[a b] (.split set-statement "=")]
      [(.trim a) (-> b .trim (.replace "'" ""))])))

(defn get-hivevars [^String s]
  (->> (.split s ";")
       (map get-hivevar)
       (into {})))

(defn get-environment [^String s]
  (let [hivevars (get-hivevars s)]
    [hivevars
     (->> s
          (re-seq sub-regex)
          distinct
          (map second)
          (remove hivevars))]))


(defn parse-all [^String s m]
  (-> s
      (replace-env m)
      (.split ";")
      (->> (remove #(-> % .trim (.startsWith "set"))) (map parse))))

(def s (slurp "a.hql"))

(prn (get-environment s))
