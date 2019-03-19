(ns ultra-csv.core-test
  (:require [ultra-csv.core :refer :all]
            [clojure.test :refer :all]
            [testit.core :refer :all]))

(deftest full-auto-read-lazy
  (let [data (read-csv "./dev-resources/sample01.csv" {:header? true})]
    (facts
     (first data) =in=> {"Nom du POC" "Zéro Déchet",
                         "Porteur"    "Ville de Roubaix"}
     (count data) => 10
     (count (keys (first data))) => 3
     (close! data) => true)))

(deftest full-auto-read-greedy
  (let [data (read-csv "./dev-resources/sample01.csv" {:header? true :greedy? true})]
    (facts
     (data) =in=> {"Nom du POC" "Zéro Déchet",
                   "Porteur"    "Ville de Roubaix"}
     (count (keys (data))) => 3
     (close! data) => true)))
