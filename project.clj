(defproject spark-definitive-guide "0.1.0-SNAPSHOT"

  :description "FIXME: write description"

  :url "http://example.com/FIXME"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [[org.clojure/clojure "1.10.0"]
                 [gorillalabs/sparkling "2.1.3"]]

  :aot [#".*" sparkling.serialization sparkling.destructuring]

  :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.10 "2.2.3"]
                                       [org.apache.spark/spark-sql_2.10 "2.2.3"]]}
             :dev      {:plugins [[lein-dotenv "RELEASE"]]}}

  ;:repl-options {:init-ns spark-definitive-guide.core}
  )
