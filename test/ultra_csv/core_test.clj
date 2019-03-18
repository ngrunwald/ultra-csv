(ns ultra-csv.core-test
  (:require [ultra-csv.core :refer :all]
            [clojure.java.io :as io]))

;; (def csv-path (io/file (io/resource "census.csv")))

;; (expect true (> (count (read-csv csv-path)) 0))
;; (expect 25 (-> (read-csv csv-path) (first) (keys) (count)))
;; (expect 1 (-> (read-csv csv-path) (first) (:COUNTY)))
