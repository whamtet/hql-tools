(ns inspect-hql.core
  (:require
    [clojure.string :as string]
    [inspect-hql.parse :as parse]
    [rhizome.viz :as rhizome])
  (:import
    java.io.File))

(def files ["a.hql"])

(def parsed
  (mapcat #(-> % slurp parse/parse-all-hivevars) files))

(defmacro let-v [binding & body]
  `(when (vector? ~'v)
    (let [~binding ~'v] ~@body)))

(defn get-tab-references [v]
  (let-v [tab-ref? & components]
         (if (= "TOK_TABNAME" tab-ref?)
           [(->> components (map first) (string/join "."))]
           (mapcat get-tab-references v))))

(defn create-as [parsed]
  (into {}
        (for [[create-token? [_ [tab-name]] & rest] parsed
              :when (= "TOK_CREATETABLE" create-token?)
              :let [references (get-tab-references (vec rest))]
              :when (not-empty references)]
          [tab-name (distinct references)])))

(defn inserts [parsed]
  (into {}
        (for [[query-token? from insert] parsed
              :when (= "TOK_QUERY" query-token?)
              :let [references (get-tab-references from)
                    [into] (get-tab-references insert)]
              :when (and into (not-empty references)) ]
          [into (distinct references)])))

(def g
  (merge-with
   #(distinct (concat %1 %2))
   (create-as parsed)
   (inserts parsed)))

(defn- node->descriptor [table] {:label table})

(spit "sql.svg"
  (rhizome/graph->svg (keys g) g
     :node->descriptor node->descriptor))
