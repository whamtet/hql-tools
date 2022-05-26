(ns inspect-hql.context
  (:require
    [clojure.xml :as xml]
    [rhizome.viz :as rhizome])
  (:import
    java.io.File))

(defn _next [{:keys [tag attrs content]}]
  (case tag
    :next (:to attrs)
    (some _next content)))

(defn _extract-deps [{:keys [tag attrs content]}]
  (case tag
    :split
    (let [children (map _extract-deps content)
          split-children (map ffirst children)]
      (apply concat
             [(cons (:id attrs) split-children)]
             children))
    :step
    [[(:id attrs) (some _next content)]]
    (mapcat _extract-deps content)))

(defn extract-deps [m]
  (let [m (_extract-deps m)]
    (zipmap (map first m) (map #(->> % rest (filter identity)) m))))

(def g
  (-> "context.xml" File. xml/parse extract-deps))

(defn- node->descriptor [table] {:label table})

(spit "context.svg"
      (rhizome/graph->svg (keys g) g
                          :node->descriptor node->descriptor))
