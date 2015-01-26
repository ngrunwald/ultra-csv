(ns ultra-csv.core-test
  (:require [expectations :refer :all]
            [ultra-csv.core :refer :all]))

(def csv-path "census.csv")

(expect true (> (count (read-csv csv-path)) 0))
(expect 25 (-> (read-csv csv-path) (first) (keys) (count)))
(expect 1 (-> (read-csv csv-path) (first) (:COUNTY)))
