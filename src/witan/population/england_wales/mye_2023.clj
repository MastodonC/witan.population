(ns witan.population.england-wales.mye-2023
  "Functions to read and process ONS Subnational Mid-Year Population Estimates (MYE)
     for LAs by single year of age and sex from: 
     https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales"
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [tech.v3.libs.fastexcel :as fst]
            [tech.v3.dataset.io.spreadsheet :as ss]
            [tech.v3.dataset.reductions :as dsr]
            [tablecloth.api :as tc]
            [witan.population.lookups.lad-to-ctyua :as lad->ctyua]))

(def default-resource-file-name
  "Name of resource file containing subnational mid-year population estimates
   (for LAs by single year of age and sex) to use by default."
  "myebtablesenglandwales/myebtablesenglandwales20112023.xlsx")

(defn ->dataset-raw
  "Read MYEs from MYEB1 sheet of Excel workbook into a dataset.
   Specify Excel file by either `file-path` or `resource-file-name`,
   defaulting to `default-resource-file-name` if neither specified."
  [& {::keys [file-path resource-file-name dataset-name]
      :or    {resource-file-name default-resource-file-name}}]
  (with-open [in (-> (or file-path (io/resource resource-file-name))
                     io/file
                     io/input-stream)]
    (-> in
        fst/input->workbook
        ((partial some #(when (= "MYEB1" (.name %)) %)))
        (ss/sheet->dataset {:n-initial-skip-rows 1
                            :header-row?         true
                            :key-fn              keyword
                            :parser-fn           {:age :int8}
                            :dataset-name        (or dataset-name
                                                     (when file-path (str file-path " " "MYEB1"))
                                                     (when resource-file-name (str resource-file-name " " "MYEB1")))}))))

(comment ;; Structure of raw dataset from default file
  (-> {:resource-file-name default-resource-file-name}
      ->dataset-raw
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
  ;;=> myebtablesenglandwales/myebtablesenglandwales20112023.xlsx MYEB1: descriptive-stats [18 6]:
  ;;   
  ;;   |        :col-name | :datatype | :n-valid | :n-missing | :min |    :max |
  ;;   |------------------|-----------|---------:|-----------:|-----:|--------:|
  ;;   |       :ladcode23 |   :string |    57876 |          0 |      |         |
  ;;   |        :laname23 |   :string |    57876 |          0 |      |         |
  ;;   |         :country |   :string |    57876 |          0 |      |         |
  ;;   |             :sex |   :string |    57876 |          0 |      |         |
  ;;   |             :age |     :int8 |    57876 |          0 |  0.0 |    90.0 |
  ;;   | :population_2011 |  :float64 |    57876 |          0 |  0.0 | 10572.0 |
  ;;   | :population_2012 |  :float64 |    57876 |          0 |  0.0 | 10521.0 |
  ;;   | :population_2013 |  :float64 |    57876 |          0 |  0.0 | 10137.0 |
  ;;   | :population_2014 |  :float64 |    57876 |          0 |  2.0 | 10243.0 |
  ;;   | :population_2015 |  :float64 |    57876 |          0 |  0.0 | 10384.0 |
  ;;   | :population_2016 |  :float64 |    57876 |          0 |  0.0 | 10934.0 |
  ;;   | :population_2017 |  :float64 |    57876 |          0 |  0.0 | 10872.0 |
  ;;   | :population_2018 |  :float64 |    57876 |          0 |  0.0 | 10969.0 |
  ;;   | :population_2019 |  :float64 |    57876 |          0 |  0.0 | 11159.0 |
  ;;   | :population_2020 |  :float64 |    57876 |          0 |  0.0 | 10601.0 |
  ;;   | :population_2021 |  :float64 |    57876 |          0 |  0.0 | 10771.0 |
  ;;   | :population_2022 |  :float64 |    57876 |          0 |  0.0 | 11447.0 |
  ;;   | :population_2023 |  :float64 |    57876 |          0 |  0.0 | 11565.0 |
  ;;   

  :rcf)

(defn ds-raw->ds-by-lad
  "Canonicalise raw LAD code/name level MYE 2023 dataset `ds`
   by adding County/Unitary Authority codes & names, pivoting long by year and
   rolling up over `:sex`.
   Dataset can optionally be filtered as follows:
   - For LA District, 
     by specifying functions for keys `:ladcd-f` and/or `:ladnm-f`
     that return truthy values for LA District codes/names to select.
   - For County/Unitary Authority, 
     by specifying functions for keys `ctyuacd-f` and/or `ctyuanm-f`
     that return truthy values for County/Unitary Authority codes/names to select.
   - For (integer) ages, 
     by specifying values for keys `:min-age` and/or `:max-age`.
   - For years, 
     by specifying values for keys `:min-year` and/or `:max-year`."
  [ds & {:keys [ladcd-f   ladnm-f
                ctyuacd-f ctyuanm-f
                min-age   max-age
                min-year  max-year]}]
  (as-> ds $
    ;; Canonicalise column names
    (tc/rename-columns $ {:ladcode23 :lad23cd
                          :laname23  :lad23nm})
    ;; Filter for LAD codes/names if requested
    (cond-> $
      ladcd-f (tc/select-rows (comp ladcd-f :lad23cd))
      ladnm-f (tc/select-rows (comp ladnm-f :lad23nm)))
    ;; Merge in CTYUA codes and names
    (tc/left-join $
                  (-> (lad->ctyua/->dataset {:year 2023, :dataset-name "lad->ctyua"})
                      (tc/select-columns [:lad23cd :ctyua23cd :ctyua23nm]))
                  [:lad23cd])
    (tc/drop-columns $ #"^:lad->ctyua\..+$")
    ;; Filter for CTYUA codes/names if requested
    (cond-> $
      ctyuacd-f (tc/select-rows (comp ctyuacd-f :ctyua23cd))
      ctyuanm-f (tc/select-rows (comp ctyuanm-f :ctyua23nm)))
    ;; Filter for (integer) ages if requested
    (cond-> $
      min-age (tc/select-rows #(some-> % :age (>= min-age)))
      max-age (tc/select-rows #(some-> % :age (<= max-age))))
    ;; Pivot long for `:year`
    (tc/pivot->longer $ #"^:population_\d{4}" {:target-columns    :year
                                               :value-column-name :population
                                               :splitter          #"^:population_(\d{4})"
                                               :datatypes         {:year       :int16
                                                                   :population :int16}})
    ;; Filter for (integer) years if requested
    (cond-> $
      min-year (tc/select-rows #(some-> % :year (>= min-year)))
      max-year (tc/select-rows #(some-> % :year (<= max-year))))
    ;; Roll up across `:sex` M & F
    (dsr/group-by-column-agg (tc/column-names $ (complement #{:sex :population}))
                             {:population (dsr/sum :population)}
                             $)
    (tc/add-column $ :sex "persons")
    ;; Arrange dataset
    (tc/reorder-columns $ [:lad23cd :lad23nm :ctyua23cd :ctyua23nm
                           :country
                           :year
                           :age
                           :sex
                           :population])
    (tc/order-by $ (tc/column-names $))
    (tc/set-dataset-name $ "MYE 2023 by LAD")))

(defn ->dataset-by-lad
  [& {:as options}]
  (-> (->dataset-raw options)
      (ds-raw->ds-by-lad options)))

(comment ;; Structure of (unfiltered) dataset by LAD
  (-> {:resource-file-name default-resource-file-name}
      ->dataset-by-lad
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
  ;;=> MYE 2023 by LAD: descriptive-stats [9 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing |   :min |    :max |
  ;;   |-------------|-----------|---------:|-----------:|-------:|--------:|
  ;;   |    :lad23cd |   :string |   376194 |          0 |        |         |
  ;;   |    :lad23nm |   :string |   376194 |          0 |        |         |
  ;;   |  :ctyua23cd |   :string |   376194 |          0 |        |         |
  ;;   |  :ctyua23nm |   :string |   376194 |          0 |        |         |
  ;;   |    :country |   :string |   376194 |          0 |        |         |
  ;;   |       :year |    :int16 |   376194 |          0 | 2011.0 |  2023.0 |
  ;;   |        :age |     :int8 |   376194 |          0 |    0.0 |    90.0 |
  ;;   |        :sex |   :string |   376194 |          0 |        |         |
  ;;   | :population |  :float64 |   376194 |          0 |    0.0 | 22477.0 |
  ;;   

  :rcf)

(defn ds-by-lad->ds-by-ctyua
  "Roll up dataset `ds` of population by LA District to the County/Unitary Authority level."
  [ds & _]
  (as-> ds $
    (dsr/group-by-column-agg (tc/column-names $ (complement #{:lad23cd :lad23nm :population}))
                             {:population (dsr/sum :population)}
                             $)
    (tc/set-dataset-name $ "MYE 2023 by CTYUA")))

(defn ->dataset-by-ctyua
  [& {:as options}]
  (-> (->dataset-by-lad options)
      (ds-by-lad->ds-by-ctyua options)))

(comment ;; Structure of (unfiltered) dataset by CTYUA
  (-> {:resource-file-name default-resource-file-name}
      ->dataset-by-ctyua
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
  ;;=> SNPP 2022 by CTYUA: descriptive-stats [8 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing |   :min |        :max |
  ;;   |-------------|-----------|---------:|-----------:|-------:|------------:|
  ;;   |  :ctyua22cd |   :string |   363584 |          0 |        |             |
  ;;   |  :ctyua22nm |   :string |   363584 |          0 |        |             |
  ;;   |       :year |    :int16 |   363584 |          0 | 2022.0 |    2047.000 |
  ;;   |  :age-group |   :string |   363584 |          0 |        |             |
  ;;   |        :age |     :int8 |   355680 |       7904 |    0.0 |      89.000 |
  ;;   |  :component |   :string |   363584 |          0 |        |             |
  ;;   |        :sex |   :string |   363584 |          0 |        |             |
  ;;   | :population |  :float64 |   363584 |          0 |    0.0 | 1871238.751 |
  ;;   

  :rcf)

(defn ->dataset
  [& {:as options}]
  (->dataset-by-ctyua options))
