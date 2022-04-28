(ns witan.population.ncy-test
  (:require [clojure.test :refer :all]
            [witan.population.ncy :refer :all]))

(deftest ncy->age-test
  (testing "ncy 0 is 5"
    (is (= 5 (ncy->age 0))))
  (testing "ncy -5 is 0"
    (is (= 0 (ncy->age -5)))))

(deftest age->ncy-test
  (testing "ncy 0 is 5"
    (is (= 0 (age->ncy 5 ))))
  (testing "ncy -5 is 0"
    (is (= -5 (age->ncy 0)))))
