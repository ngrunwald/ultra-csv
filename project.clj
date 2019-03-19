(defproject ultra-csv "0.2.3"
  :description "A smart reader for CSV files"
  :url "https://github.com/ngrunwald/ultra-csv"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [net.sf.supercsv/super-csv "2.4.0"]
                 [com.ibm.icu/icu4j "63.1"]
                 [prismatic/schema "1.1.10"]
                 [org.clojure/tools.logging "0.4.1"]]
  :profiles {:dev {:dependencies [[metosin/testit "0.3.0"]]}})
