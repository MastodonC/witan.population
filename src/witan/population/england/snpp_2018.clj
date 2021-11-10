(ns witan.population.england.snpp-2018
  "This ns reads in the 2018 SNPP Population projection from 2018-2041
  and converts it into a format suitable for use in the witan.send
  model."
  (:require [clojure.java.io :as io]
            [tablecloth.api :as tc]
            [tech.v3.dataset :as ds]
            [witan.population.ncy :as ncy]))

(def snpp-2018-file-name "2018snpppopulation/2018 SNPP Population persons.csv")

(def output-columns [:calendar-year :academic-year :population])
(def start-age 0)
(def end-age 26)

(def snpp-2018-data
  (delay (with-open [in (-> snpp-2018-file-name
                            io/resource
                            io/file
                            io/input-stream)]
           (ds/->dataset in {:file-type :csv}))))

(defn snpp-2018->witan-send-population [snpp-data la-name max-year]
  (-> snpp-data
      (tc/select-rows #(= la-name (get % "AREA_NAME")))
      (tc/drop-rows #(#{"90 and over" "All ages"} (get % "AGE_GROUP")))
      (tc/convert-types "AGE_GROUP" :int16)
      (tc/select-rows #(<= start-age (get % "AGE_GROUP") end-age))
      (tc/drop-columns ["AREA_CODE" "AREA_NAME" "COMPONENT" "SEX"])
      (tc/pivot->longer (complement #{"AGE_GROUP"})
                        {:target-columns "year" :value-column-name "population"})
      (tc/rename-columns {"AGE_GROUP" :age
                          "year" :calendar-year
                          "population" :population})
      (tc/select-rows #(<= (:calendar-year %) max-year))
      (tc/map-columns :academic-year [:age] (fn [age] (ncy/age->ncy age)))))

(defn write-witan-send-population! [witan-send-population file-name]
  (-> witan-send-population
      (tc/order-by [:calendar-year :academic-year])
      (tc/select-columns output-columns)
      (tc/write! file-name)))

(defn create-send-population-file! [la-name max-year file-name]
  (-> @snpp-2018-data
      (snpp-2018->witan-send-population la-name max-year)
      (write-witan-send-population! file-name)))
