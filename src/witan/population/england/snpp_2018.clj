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

(defn snpp-2018->witan-send-population [snpp-data
                                        {:keys [la-name
                                                max-year
                                                start-age end-age]
                                         :or {start-age 0
                                              end-age 26}
                                         :as _opts}]
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
      (tc/map-columns :academic-year [:age] (fn [age] (ncy/age->ncy age)))
      (tc/order-by [:academic-year :calendar-year])))

(defn write-witan-send-population! [witan-send-population file-name]
  (-> witan-send-population
      (tc/order-by [:calendar-year :academic-year])
      (tc/select-columns output-columns)
      (tc/write! file-name)))

(defn create-send-population-file! [{:keys [la-name
                                            max-year
                                            file-name
                                            start-age end-age]
                                     :as opts}]
  (-> @snpp-2018-data
      (snpp-2018->witan-send-population opts)
      (write-witan-send-population! file-name)))

(comment

  (-> (snpp-2018->witan-send-population @snpp-2018-data
                                        {:la-name "Norfolk" :max-year 2031 :start-age 4 :end-age 6})
      (tc/head 42));; => _unnamed [42 4]:
  ;;    | :age | :calendar-year | :population | :academic-year |
  ;;    |-----:|---------------:|------------:|---------------:|
  ;;    |    4 |           2018 |    9585.000 |             -1 |
  ;;    |    4 |           2019 |    9684.787 |             -1 |
  ;;    |    4 |           2020 |    9743.447 |             -1 |
  ;;    |    4 |           2021 |    9492.942 |             -1 |
  ;;    |    4 |           2022 |    9067.316 |             -1 |
  ;;    |    4 |           2023 |    8987.414 |             -1 |
  ;;    |    4 |           2024 |    9047.318 |             -1 |
  ;;    |    4 |           2025 |    9011.331 |             -1 |
  ;;    |    4 |           2026 |    8960.247 |             -1 |
  ;;    |    4 |           2027 |    8937.431 |             -1 |
  ;;    |    4 |           2028 |    8980.071 |             -1 |
  ;;    |    4 |           2029 |    8988.258 |             -1 |
  ;;    |    4 |           2030 |    8996.412 |             -1 |
  ;;    |    4 |           2031 |    9007.839 |             -1 |
  ;;    |    5 |           2018 |   10035.000 |              0 |
  ;;    |    5 |           2019 |    9681.281 |              0 |
  ;;    |    5 |           2020 |    9774.959 |              0 |
  ;;    |    5 |           2021 |    9834.346 |              0 |
  ;;    |    5 |           2022 |    9580.223 |              0 |
  ;;    |    5 |           2023 |    9158.044 |              0 |
  ;;    |    5 |           2024 |    9070.508 |              0 |
  ;;    |    5 |           2025 |    9128.899 |              0 |
  ;;    |    5 |           2026 |    9092.653 |              0 |
  ;;    |    5 |           2027 |    9041.234 |              0 |
  ;;    |    5 |           2028 |    9018.342 |              0 |
  ;;    |    5 |           2029 |    9060.106 |              0 |
  ;;    |    5 |           2030 |    9068.072 |              0 |
  ;;    |    5 |           2031 |    9075.969 |              0 |
  ;;    |    6 |           2018 |   10231.000 |              1 |
  ;;    |    6 |           2019 |   10151.088 |              1 |
  ;;    |    6 |           2020 |    9796.700 |              1 |
  ;;    |    6 |           2021 |    9884.588 |              1 |
  ;;    |    6 |           2022 |    9945.722 |              1 |
  ;;    |    6 |           2023 |    9687.510 |              1 |
  ;;    |    6 |           2024 |    9268.406 |              1 |
  ;;    |    6 |           2025 |    9172.862 |              1 |
  ;;    |    6 |           2026 |    9231.400 |              1 |
  ;;    |    6 |           2027 |    9194.763 |              1 |
  ;;    |    6 |           2028 |    9142.956 |              1 |
  ;;    |    6 |           2029 |    9119.893 |              1 |
  ;;    |    6 |           2030 |    9160.801 |              1 |
  ;;    |    6 |           2031 |    9168.545 |              1 |

  )
