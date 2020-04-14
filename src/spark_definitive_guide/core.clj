(ns spark-definitive-guide.core
  (:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.serialization]
            [sparkling.destructuring :as des]
            [clojure.java.io :as io])
  (:import (org.apache.spark.sql SparkSession)
           (org.apache.spark.api.java JavaSparkContext)))

(def c (-> (conf/spark-conf)
           (conf/master "local")
           (conf/app-name "sparkling-example")))

(def sc (spark/spark-context ^JavaSparkContext c))

(def spark-session (SparkSession. (.sc sc)))

(defn str-array
  [& args]
  (into-array String (vec args)))

(let [my-range (.toDF (.range spark-session 100) (str-array "number"))]
  (-> my-range
      (. where "number %2 = 0")
      .count))

(def data (-> spark-session
              .read
              (.option "inferSchema" "true")
              (.option "header" "true")
              (.csv "resources/2015-summary.csv")))

(-> data
    (.sort "count" (str-array))
    .explain)


(.set (.conf spark-session) "spark.sql.shuffle.partitions" "5")
(.get (.conf spark-session) "spark.sql.shuffle.partitions")

(-> data
    (.sort "count" (str-array))
    (.take 200))
