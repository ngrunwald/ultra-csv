(defproject ultra-csv "0.2.0-SNAPSHOT"
  :description "A smart reader for CSV files"
  :url "https://github.com/ngrunwald/ultra-csv"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [net.sf.supercsv/super-csv "2.2.0"]
                 [com.ibm.icu/icu4j "53.1"]
                 [prismatic/schema "0.2.4"]
                 [org.clojure/tools.logging "0.3.0"]]
  :plugins [[codox "0.8.10"]]
  :codox {:src-dir-uri "http://github.com/ngrunwald/ultra-csv/blob/master/"
          :src-linenum-anchor-prefix "L"
          :defaults {:doc/format :markdown}})
