(ns spark-definitive-guide.util)

(defn str-array
  [& args]
  (into-array String args))

(defn group-by*
  [dataset & columns]
  (.groupBy dataset (first columns) (apply str-array (rest columns))))


(defn sum*
  [dataset & colm-names]
  (.sum dataset (apply str-array colm-names)))


(defn with-column-renamed
  [dataset existing-name new-name]
  (.withColumnRenamed dataset existing-name new-name))


(defn show
  [dataset n]
  (.show dataset n))