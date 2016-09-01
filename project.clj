(defproject ultra-csv "0.2.1"
  :description "A smart reader for CSV files"
  :url "https://github.com/ngrunwald/ultra-csv"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [net.sf.supercsv/super-csv "2.4.0"]
                 [com.ibm.icu/icu4j "57.1"]
                 [prismatic/schema "1.1.2"]
                 [org.clojure/tools.logging "0.3.1"]]
  :profiles {:dev {:dependencies [[expectations "2.1.8"]]
                   :plugins [[lein-expectations "0.0.8"]
                             [codox "0.8.10"]]
                   :codox {:src-dir-uri "http://github.com/ngrunwald/ultra-csv/blob/master/"
                           :src-linenum-anchor-prefix "L"
                           :defaults {:doc/format :markdown}}}})
