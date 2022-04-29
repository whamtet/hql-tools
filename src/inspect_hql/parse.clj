(ns inspect-hql.parse
  (:require
    [clojure.string :as string])
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
      [(.trim a) (.trim b)])))

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

(defn remove-comments [s]
  (->> (.split s "\n")
       (remove #(-> % .trim (.startsWith "#")))
       (string/join "\n")))

(defn parse-all [^String s m]
  (-> s
      remove-comments
      (replace-env m)
      (.split ";")
      (->> (remove #(-> % .trim (.startsWith "set"))) (map parse))))

(def s (slurp "a.hql"))
(def m (assoc (get-hivevars s)
              "SDATE" "2022-04-11"
              "EDATE" "2022-04-29"))

(dorun (parse-all s m))
