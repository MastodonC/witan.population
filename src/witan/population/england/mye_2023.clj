(ns witan.population.england.mye-2023
  "Functions to read and process ONS Subnational Mid-Year Population Estimates (MYE)
   for LAs by single year of age and sex from
   https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales
   into the form required for `witan.send` modelling."
  (:require [clojure.java.io :as io]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.libs.fastexcel :as fst]))

;;; # Parameters
;;; ## Defaults
(def default-resource-file-name
  "Name of resource file containing subnational mid-year population estimates
   (for LAs by single year of age and sex) to use by default."
  "2023mye/myebtablesenglandwales20112023.xlsx")

(def output-columns
  "Output columns for `witan.send` population.csv file."
  [:calendar-year :academic-year :population])

(def default-min-academic-year -4)
(def default-max-academic-year 20)

(defn ->map-of-datasets
  "Read MYE data from xlsx file specified by either `resource-file-name` or `file-path`,
  defaulting to `default-resource-file-name` if neither specified."
  [& {::keys [resource-file-name file-path]
      :or    {resource-file-name default-resource-file-name}}]
  (with-open [in (-> (or file-path (io/resource resource-file-name))
                     io/file
                     io/input-stream)]
    (->> (fst/workbook->datasets in {:n-initial-skip-rows 1
                                     :header-row?         true
                                     :key-fn              keyword
                                     :parser-fn           {:age :int16}})
         (reduce (fn [m coll] (assoc m (tc/dataset-name coll) coll)) {}))))

(defn ->ds
  "Load dataset with `dataset-name` from either `resource-file-name` or `file-path`,
  defaulting to `default-resource-file-name` if neither specified."
  [dataset-name & {::keys [resource-file-name file-path]
                   :or    {resource-file-name default-resource-file-name}}]
  (get (->map-of-datasets resource-file-name file-path) dataset-name))

(defn ->MYE-ds
  [& {::keys [resource-file-name file-path]
      :or    {resource-file-name default-resource-file-name}}]
  (->ds "MYEB1"))

(comment

  (-> "MYEB1"
      ->ds
      (tc/head 10))
  ;; MYEB1 [10 18]:

  ;; | :ladcode23 |  :laname23 | :country | :sex | :age | :population_2011 | :population_2012 | :population_2013 | :population_2014 | :population_2015 | :population_2016 | :population_2017 | :population_2018 | :population_2019 | :population_2020 | :population_2021 | :population_2022 | :population_2023 |
  ;; |------------|------------|----------|------|-----:|-----------------:|-----------------:|-----------------:|-----------------:|-----------------:|-----------------:|-----------------:|-----------------:|-----------------:|-----------------:|-----------------:|-----------------:|-----------------:|
  ;; |  E06000001 | Hartlepool |        E |    F |  0.0 |            555.0 |            565.0 |            508.0 |            513.0 |            517.0 |            507.0 |            508.0 |            463.0 |            495.0 |            455.0 |            446.0 |            430.0 |            423.0 |
  ;; |  E06000001 | Hartlepool |        E |    F |  1.0 |            584.0 |            557.0 |            561.0 |            506.0 |            515.0 |            522.0 |            526.0 |            501.0 |            462.0 |            498.0 |            477.0 |            461.0 |            452.0 |
  ;; |  E06000001 | Hartlepool |        E |    F |  2.0 |            561.0 |            570.0 |            565.0 |            556.0 |            509.0 |            518.0 |            521.0 |            516.0 |            527.0 |            459.0 |            506.0 |            489.0 |            470.0 |
  ;; |  E06000001 | Hartlepool |        E |    F |  3.0 |            565.0 |            567.0 |            578.0 |            564.0 |            552.0 |            532.0 |            534.0 |            523.0 |            531.0 |            522.0 |            463.0 |            512.0 |            521.0 |
  ;; |  E06000001 | Hartlepool |        E |    F |  4.0 |            546.0 |            552.0 |            557.0 |            582.0 |            564.0 |            557.0 |            530.0 |            538.0 |            514.0 |            520.0 |            523.0 |            468.0 |            534.0 |
  ;; |  E06000001 | Hartlepool |        E |    F |  5.0 |            568.0 |            551.0 |            553.0 |            563.0 |            582.0 |            569.0 |            571.0 |            543.0 |            536.0 |            508.0 |            514.0 |            557.0 |            497.0 |
  ;; |  E06000001 | Hartlepool |        E |    F |  6.0 |            546.0 |            580.0 |            553.0 |            546.0 |            552.0 |            590.0 |            566.0 |            571.0 |            544.0 |            525.0 |            518.0 |            522.0 |            566.0 |
  ;; |  E06000001 | Hartlepool |        E |    F |  7.0 |            491.0 |            555.0 |            591.0 |            544.0 |            548.0 |            564.0 |            585.0 |            578.0 |            563.0 |            548.0 |            537.0 |            538.0 |            545.0 |
  ;; |  E06000001 | Hartlepool |        E |    F |  8.0 |            483.0 |            492.0 |            556.0 |            587.0 |            544.0 |            561.0 |            566.0 |            599.0 |            580.0 |            575.0 |            551.0 |            559.0 |            561.0 |
  ;; |  E06000001 | Hartlepool |        E |    F |  9.0 |            461.0 |            490.0 |            496.0 |            554.0 |            584.0 |            549.0 |            569.0 |            571.0 |            602.0 |            586.0 |            564.0 |            563.0 |            576.0 |


  )

;;; # Format for `witan.send` use
;; For `witan.send` use, want:
;; - long format dataset with MYEs for different years in separate rows
;; - `witan.send` columns added:
;;   - `:calendar-year` - Year of corresponding SEN2 census date.
;;   - `:academic-year` - NCY corresponding to age group.
;; - …and possibly filtered for NCYs & `:calendar-year`s

(defn ds->witan-send-population
  "Given MYE dataset `ds` returns a long dataset with SNPP `:population` estimates
   by `:snpp-year` with `witan.send` variables `:calendar-year` and `:academic-year`
   added.
   Dataset can be filtered by specifying (optional) values for:
   - LA: via code (string) `ladcode23` or name (string) `ladname23`.
   - NCYs: via (integer) `min-academic-year` and/or `max-academic-year`.
   - `:calendar-year`s: via (integer) `min-calendar-year` and/or `max-calendar-year`,
     or (for backwards compatibility) via (integer) `max-year`."
  ;; Note that compared to `witan.population.england.snpp-2018/snpp-2018->witan-send-population`
  ;; `:calendar-year` differs by +1 and `:academic-year` by +1,
  ;; due to consideration of SNPPs as mid-year estimates.
  [ds & {:keys [la-code la-name
                min-academic-year max-academic-year
                min-calendar-year max-calendar-year max-year]
         :or   {min-academic-year default-min-academic-year
                max-academic-year default-max-academic-year}}]
  (as-> ds $
    ;; Derive `:academic-year` from `:age-group`:
    ;; - The MYEs are mid-year estiamtes.
    ;; - Therefore the age is (almost) the age on 31st August, i.e. at the start of the school year.
    ;; - Per https://www.gov.uk/national-curriculum,
    ;;   children aged 5 at the start of the school year should be in NCY 1.
    ;; - Thus the offset between age at the start of the school year and NCY is -4.
    (tc/drop-rows $ (comp (into (sorted-set) (range 26 91)) :age))
    (tc/group-by $ [:ladcode23 :laname23 :country :age])
    (tc/aggregate $ (reduce (fn [m coll]
                              (assoc m
                                     coll
                                     (fn [k] (dfn/sum (get k coll)))))
                            {} (remove #{:sex :ladcode23 :laname23 :country :age} (tc/column-names ds))))
    (tc/map-columns $ :academic-year :int8 [:age] #(- % 4))
    ;; Select required `:academic-year`s (if specified)
    (cond-> $
      min-academic-year (tc/select-rows #(-> % :academic-year (>= min-academic-year)))
      max-academic-year (tc/select-rows #(-> % :academic-year (<= max-academic-year))))
    (cond-> $
      la-code  (tc/select-rows #(-> % :ladcode23 (= la-code)))
      la-name  (tc/select-rows #(-> % :laname23 (= la-name))))
    (tc/pivot->longer $ #":population_(.*)" {:target-columns :mye-year
                                             :value-column-name :population
                                             :splitter #":population_(.*)"
                                             :datatypes {:mye-year   :int16
                                                         :population :int16}})
    ;; Derive `:calendar-year` from `:mye-year`:
    ;; - Mid-year estimates (MYE) are the population going into the next school year.
    ;; - Which will be reported in the following year's SEN2 census.
    ;; - So `:calendar-year` (the year for the corresponding SEN2 census date) is one more than `:snpp-year`.
    (tc/map-columns $ :calendar-year :int16 [:mye-year] inc)
    ;; Select `:calendar-year`s (if specified)
    (cond-> $
      min-calendar-year (tc/select-rows #(-> % :calendar-year (>= min-calendar-year)))
      max-calendar-year (tc/select-rows #(-> % :calendar-year (<= max-calendar-year)))
      max-year          (tc/select-rows #(-> % :calendar-year (<= max-year))))
    ;; Arrange dataset
    (tc/reorder-columns $ [:ladcode23 :laname23
                           :country :mye-year
                           :calendar-year :age
                           :academic-year
                           :population])
    (tc/order-by $ [:mye-year :age])
    (tc/set-dataset-name $ "MYE 2023 by LA by NCY and SEN2 calendar year")))

(defn ->witan-send-population
  "Given MYE dataset `ds` returns a long dataset with SNPP `:population` estimates
   by `:snpp-year` with `witan.send` variables `:calendar-year` and `:academic-year`
   added.
   Dataset can be filtered by specifying (optional) values for:
   - LA: via code (string) `ladcode23` or name (string) `ladname23`.
   - NCYs: via (integer) `min-academic-year` and/or `max-academic-year`.
   - `:calendar-year`s: via (integer) `min-calendar-year` and/or `max-calendar-year`,
     or (for backwards compatibility) via (integer) `max-year`."
  [& {::keys [resource-file-name file-path dataset-name]
      :keys  [la-name min-academic-year max-academic-year
              min-calendar-year max-calendar-year max-year]
      :as    options}]
  (ds->witan-send-population
   (->ds (select-keys options [::resource-file-name ::file-path ::dataset-name]))
   (dissoc options [::resource-file-name ::file-path ::dataset-name])))

(comment

  (ds->witan-send-population (->MYE-ds) {:la-name "Dorset" :min-academic-year -3 :max-academic-year 20})
  ;; MYE 2023 by LA by NCY and SEN2 calendar year [312 8]:

  ;; | :ladcode23 | :laname23 | :country | :mye-year | :calendar-year | :age | :academic-year | :population |
  ;; |------------|-----------|----------|----------:|---------------:|-----:|---------------:|------------:|
  ;; |  E06000059 |    Dorset |        E |      2011 |           2012 |  1.0 |             -3 |        3368 |
  ;; |  E06000059 |    Dorset |        E |      2011 |           2012 |  2.0 |             -2 |        3459 |
  ;; |  E06000059 |    Dorset |        E |      2011 |           2012 |  3.0 |             -1 |        3564 |
  ;; |  E06000059 |    Dorset |        E |      2011 |           2012 |  4.0 |              0 |        3614 |
  ;; |  E06000059 |    Dorset |        E |      2011 |           2012 |  5.0 |              1 |        3499 |
  ;; |  E06000059 |    Dorset |        E |      2011 |           2012 |  6.0 |              2 |        3485 |
  ;; |  E06000059 |    Dorset |        E |      2011 |           2012 |  7.0 |              3 |        3473 |
  ;; |  E06000059 |    Dorset |        E |      2011 |           2012 |  8.0 |              4 |        3623 |
  ;; |  E06000059 |    Dorset |        E |      2011 |           2012 |  9.0 |              5 |        3521 |
  ;; |  E06000059 |    Dorset |        E |      2011 |           2012 | 10.0 |              6 |        3600 |
  ;; |        ... |       ... |      ... |       ... |            ... |  ... |            ... |         ... |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 14.0 |             10 |        4413 |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 15.0 |             11 |        4561 |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 16.0 |             12 |        4283 |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 17.0 |             13 |        4136 |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 18.0 |             14 |        4099 |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 19.0 |             15 |        2935 |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 20.0 |             16 |        2489 |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 21.0 |             17 |        2462 |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 22.0 |             18 |        2855 |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 23.0 |             19 |        3122 |
  ;; |  E06000059 |    Dorset |        E |      2023 |           2024 | 24.0 |             20 |        3201 |

  )
