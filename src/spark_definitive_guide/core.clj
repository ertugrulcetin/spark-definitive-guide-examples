(ns spark-definitive-guide.core
  (:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.serialization]
            [sparkling.destructuring :as des]
            [sparkling.sql :as ssql]
            [clojure.java.io :as io])
  (:import (org.apache.spark.sql SparkSession functions)
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
              (.csv "resources/data/flight-data/csv/2015-summary.csv")))
data
(-> data
    (.sort "count" (str-array))
    .explain)


(.set (.conf spark-session) "spark.sql.shuffle.partitions" "5")
(.get (.conf spark-session) "spark.sql.shuffle.partitions")

(-> data
    (.sort "count" (str-array))
    (.take 200))

(-> data
    (.createOrReplaceTempView "flight_data_2015"))

(def sql-way (.sql spark-session
                   "SELECT DEST_COUNTRY_NAME, count(1)
                    FROM flight_data_2015
                    GROUP BY DEST_COUNTRY_NAME"))
(.explain sql-way)

(def data-frame-way (-> data
                        (.groupBy "DEST_COUNTRY_NAME" (str-array))
                        (.count)))
(.explain data-frame-way)

(-> spark-session
    (.sql "SELECT max(count) from flight_data_2015")
    (.take 1)
    (vec)
    first
    (.get 0))

(def max-sql (-> spark-session
                 (.sql "SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
                 FROM flight_data_2015
                 GROUP BY DEST_COUNTRY_NAME
                 ORDER BY sum(count) DESC
                 LIMIT 5")))


(defn group-by*
  [dataset & columns]
  (.groupBy dataset (first columns) (apply str-array (rest columns))))

(defn sum*
  [dataset & colm-names]
  (.sum dataset (apply str-array colm-names)))

(defn with-column-renamed
  [dataset existing-name new-name]
  (.withColumnRenamed dataset existing-name new-name))

(-> data
    (group-by* "DEST_COUNTRY_NAME")
    (sum* "count")
    (with-column-renamed "sum(count)", "destination_total")
    (.sort (into-array [(functions/desc "destination_total")]))
    (.limit 5)
    .show
    )