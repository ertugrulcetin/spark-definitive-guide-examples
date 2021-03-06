(ns spark-definitive-guide.core
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.serialization]
            [sparkling.scalaInterop :as si]
            [spark-definitive-guide.util :as util])
  (:import (org.apache.spark.ml Pipeline PipelineStage)
           (org.apache.spark.ml.clustering KMeans)
           (org.apache.spark.ml.feature StringIndexer OneHotEncoder VectorAssembler)
           (org.apache.spark.sql SparkSession Encoders functions)
           (org.apache.spark.api.java JavaSparkContext)))


;; ~~~~~~~~~~~~~~~~~~~~~ Chapter 2 - A Gentle Introduction to Spark ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


(def config (-> (conf/spark-conf)
                (conf/master "local")
                (conf/app-name "sparkling-example")))

(def spark-context (spark/spark-context ^JavaSparkContext config))

;; The SparkSession
(def spark-session (SparkSession. (.sc spark-context)))


(let [my-range (.toDF (.range spark-session 1000) (util/str-array "number"))
      ;;Transformations
      transformation-form (. my-range where "number %2 = 0")]
  ;; Action form
  (.count transformation-form))


;; Check Spark UI -> http://localhost:4040

;; An End-to-End Example
(def data (-> spark-session
              .read
              (.option "inferSchema" "true")
              (.option "header" "true")
              (.csv "resources/data/flight-data/csv/2015-summary.csv")))

(-> data
    (.sort "count" (util/str-array))
    .explain)


(.set (.conf spark-session) "spark.sql.shuffle.partitions" "5")
(.get (.conf spark-session) "spark.sql.shuffle.partitions")


(-> data
    (.sort "count" (util/str-array))
    (.take 200))


;; DataFrames and SQL
(-> data
    (.createOrReplaceTempView "flight_data_2015"))


(def sql-way (.sql spark-session
                   "SELECT DEST_COUNTRY_NAME, count(1)
                    FROM flight_data_2015
                    GROUP BY DEST_COUNTRY_NAME"))

(.explain sql-way)


(def data-frame-way (-> data
                        (.groupBy "DEST_COUNTRY_NAME" (util/str-array))
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


(-> data
    (util/group-by* "DEST_COUNTRY_NAME")
    (util/sum* "count")
    (util/with-column-renamed "sum(count)", "destination_total")
    (.sort (into-array [(functions/desc "destination_total")]))
    (.limit 5)
    .show)


;; ~~~~~~~~~~~~~~~~~~~~~ Chapter 3 - A Tour of Spark’s Toolset ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


;; Datasets: Type-Safe Structured APIs
(defrecord Flight [DEST_COUNTRY_NAME ORIGIN_COUNTRY_NAME count])


(def flights-df (-> spark-session
                    .read
                    (.parquet "resources/data/flight-data/parquet/2010-summary.parquet/")))


(def flights (.as flights-df "Flight"))


(-> flights
    (.filter (si/function1 (fn [flight-row] (not= (.getAs flight-row "DEST_COUNTRY_NAME") "Canada"))))
    (.map (si/function1 (fn [flight-row] (->Flight (.getAs flight-row "DEST_COUNTRY_NAME")
                                                   (.getAs flight-row "ORIGIN_COUNTRY_NAME")
                                                   (+ 5 (.getAs flight-row "count")))))
          (Encoders/javaSerialization spark_definitive_guide.core.Flight))
    (.take 5)
    vec)


;; Structured Streaming
(def static-data-frame (-> spark-session
                           .read
                           (.format "csv")
                           (.option "header" "true")
                           (.option "inferSchema" "true")
                           (.load "resources/data/retail-data/by-day/*.csv")))

(.createOrReplaceTempView static-data-frame "retail_data")


(def static-schema (.schema static-data-frame))


(-> static-data-frame
    (.selectExpr (util/str-array "CustomerId",
                            "(UnitPrice * Quantity) as total_cost",
                            "InvoiceDate"))
    (.groupBy (into-array [(functions/col "CustomerId")
                           (functions/window (functions/col "InvoiceDate") "1 day")]))
    (util/sum* "total_cost")
    (util/show 5))


(def streaming-data-frame (-> spark-session
                              .readStream
                              (.schema static-schema)
                              (.option "maxFilesPerTrigger" 1)
                              (.format "csv")
                              (.option "header" "true")
                              (.load "/data/retail-data/by-day/*.csv")))

(.isStreaming streaming-data-frame)


(def purchase-by-customer-per-hour (-> streaming-data-frame
                                       (.selectExpr (util/str-array "CustomerId",
                                                               "(UnitPrice * Quantity) as total_cost",
                                                               "InvoiceDate"))
                                       (.groupBy (into-array [(functions/col "CustomerId")
                                                              (functions/window (functions/col "InvoiceDate") "1 day")]))
                                       (util/sum* "total_cost")))


(-> purchase-by-customer-per-hour
    .writeStream
    (.format "memory")
    (.queryName "customer_purchases")
    (.outputMode "complete")
    .start)


(-> spark-session
    (.sql "SELECT *
            FROM customer_purchases
            ORDER BY `sum(total_cost)` DESC")
    (.show 5))


(-> purchase-by-customer-per-hour
    .writeStream
    (.format "console")
    (.queryName "customer_purchases_2")
    (.outputMode "complete")
    .start)

;; Machine Learning and Advanced Analytics
(.printSchema static-data-frame)


(def prepped-data-frame (-> static-data-frame
                            .na
                            (.fill 0)
                            (.withColumn "day_of_week" (functions/date_format (functions/col "InvoiceDate") "EEEE"))
                            (.coalesce 5)))


(def train-data-frame (-> prepped-data-frame
                          (.where "InvoiceDate < '2011-07-01'")))

(def test-data-frame (-> prepped-data-frame
                         (.where "InvoiceDate >= '2011-07-01'")))


(.count train-data-frame)
(.count test-data-frame)


(def indexer (doto (StringIndexer.)
               (.setInputCol "day_of_week")
               (.setOutputCol "day_of_week_index")))

(def encoder (doto (OneHotEncoder.)
               (.setInputCol "day_of_week_index")
               (.setOutputCol "day_of_week_encoded")))

(def vector-assembler (doto (VectorAssembler.)
                        (.setInputCols (util/str-array "UnitPrice", "Quantity", "day_of_week_encoded"))
                        (.setOutputCol "features")))


(def transformation-pipeline (doto (Pipeline.)
                               (.setStages (into-array PipelineStage [indexer encoder vector-assembler]))))


(def fitted-pipeline (.fit transformation-pipeline train-data-frame))


(def transformed-training (.transform fitted-pipeline train-data-frame))


(.cache transformed-training)


(def k-means (doto (KMeans.)
               (.setK 20)
               (.setSeed 1)))

(def km-model (.fit k-means transformed-training))

(.computeCost km-model transformed-training)


(def transformed-test (.transform fitted-pipeline test-data-frame))
;kmModel.computeCost(transformedTest)
(.computeCost km-model transformed-test)


;spark.sparkContext.parallelize(Seq(1, 2, 3)).toDF()
(.toDF (.parallelize (.sparkContext spark-session) [1 2 3]))


;; ~~~~~~~~~~~~~~~~~~~~~ Chapter 4 - Structured API Overview ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

(let [df (-> spark-session
             (.range 500)
             (.toDF (util/str-array "number")))]
 (.select df (into-array [(.col df "number")])))