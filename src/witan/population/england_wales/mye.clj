(ns witan.population.england-wales.mye
  "Functions to read and process ONS Subnational Mid-Year Population Estimates (MYE)
  for LAs by single year of age and sex downloaded from: 
  https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [tech.v3.libs.fastexcel :as fst]
            [tech.v3.dataset.io.spreadsheet :as ss]
            [tech.v3.dataset.reductions :as dsr]
            [tablecloth.api :as tc]
            [witan.population.lookups.lad-to-ctyua :as lad->ctyua]))

(def resource-options
  "Option maps for resource files containing subnational mid-year population 
   estimates (for LAs by single year of age and sex)."
  {"myebtablesenglandwales20112022v3-2021-geography" {::resource-file-name "myebtablesenglandwales/myebtablesenglandwales20112022v3.xlsx"
                                                      ::sheet-name         "MYEB1 (2021 Geography)"}
   "myebtablesenglandwales20112022v3-2023-geography" {::resource-file-name "myebtablesenglandwales/myebtablesenglandwales20112022v3.xlsx"
                                                      ::sheet-name         "MYEB1 (2023 Geography)"}
   "myebtablesenglandwales20112023"                  {::resource-file-name "myebtablesenglandwales/myebtablesenglandwales20112023.xlsx"
                                                      ::sheet-name         "MYEB1"}
   "myebtablesenglandwales20112024"                  {::resource-file-name "myebtablesenglandwales/myebtablesenglandwales20112024.xlsx"
                                                      ::sheet-name         "MYEB1"}})

(def default-resource-options
  (get resource-options "myebtablesenglandwales20112024"))


(defn ->dataset-raw
  "Read MYEs from `sheet-name` of Excel workbook into a dataset.
   Specify Excel file via either `:file-path` or `:resource-file-name`,
   and specify the `:sheet-name` to be read."
  [& {::keys [file-path
              resource-file-name
              sheet-name
              dataset-name]}]
  (let [[resource-file-name sheet-name] (if (some? file-path)
                                          [resource-file-name sheet-name]
                                          [(::resource-file-name default-resource-options)
                                           (or sheet-name (::sheet-name default-resource-options))])]
    (with-open [in (-> (or file-path (io/resource resource-file-name))
                       io/file
                       io/input-stream)]
      (-> in
          fst/input->workbook
          ((partial some #(when (= sheet-name (.name %)) %)))
          (ss/sheet->dataset {:n-initial-skip-rows 1
                              :header-row?         true
                              :key-fn              keyword
                              :parser-fn           {:age :int8}
                              :dataset-name        (or dataset-name
                                                       (when file-path (str "[" file-path "]" sheet-name))
                                                       (when resource-file-name (str "[" resource-file-name "]" sheet-name)))})))))

(comment ;; Structure of raw dataset for default options
  (-> nil
      ->dataset-raw
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
  ;;=> [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: descriptive-stats [19 6]:
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
  ;;   | :population_2022 |  :float64 |    57876 |          0 |  0.0 | 11445.0 |
  ;;   | :population_2023 |  :float64 |    57876 |          0 |  0.0 | 11741.0 |
  ;;   | :population_2024 |  :float64 |    57876 |          0 |  0.0 | 11874.0 |
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
  (let [geography-year-yy (->> ds
                               tc/column-names
                               (some (comp second (partial re-matches #"^ladcode(\d\d)") name)))
        ladcd-col   (keyword (str "lad"   geography-year-yy "cd"))
        ladnm-col   (keyword (str "lad"   geography-year-yy "nm"))
        ctyuacd-col (keyword (str "ctyua" geography-year-yy "cd"))
        ctyuanm-col (keyword (str "ctyua" geography-year-yy "nm"))]
    (as-> ds $
      ;; Canonicalise column names: `:ladcode##`→`:lad##cd` & `:laname##`→`:lad##cd`
      (tc/rename-columns $
                         (fn [k]
                           (some-> k
                                   name
                                   (str/replace #"^(ladcode|laname)(\d\d)$"
                                                #(str "lad"
                                                      (nth % 2)
                                                      (get {"ladcode" "cd"
                                                            "laname"  "nm"} (nth % 1))))
                                   keyword)))
      ;; Filter for LAD codes/names if requested
      (cond-> $
        ladcd-f (tc/select-rows (comp ladcd-f ladcd-col))
        ladnm-f (tc/select-rows (comp ladnm-f ladnm-col)))
      ;; Merge in CTYUA codes and names
      (tc/left-join $
                    (-> (lad->ctyua/->dataset {::lad->ctyua/geography-year-yy geography-year-yy})
                        (tc/select-columns [ladcd-col ctyuacd-col ctyuanm-col])
                        (tc/set-dataset-name "lad->ctyua"))
                    [ladcd-col])
      (tc/drop-columns $ #"^:lad->ctyua\..+$")
      ;; Filter for CTYUA codes/names if requested
      (cond-> $
        ctyuacd-f (tc/select-rows (comp ctyuacd-f ctyuacd-col))
        ctyuanm-f (tc/select-rows (comp ctyuanm-f ctyuanm-col)))
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
      (tc/reorder-columns $ [ladcd-col ladnm-col ctyuacd-col ctyuanm-col
                             :country
                             :year
                             :age
                             :sex
                             :population])
      (tc/order-by $ (tc/column-names $))
      (tc/set-dataset-name $ (str (tc/dataset-name ds) ": long by LAD (persons)")))))

(defn ->dataset-by-lad
  [& {:as options}]
  (-> (->dataset-raw options)
      (ds-raw->ds-by-lad options)))


(comment ;; Structure of (unfiltered) dataset by LAD for default options
  (-> nil
      ->dataset-by-lad
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
  ;;=> [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: long by LAD (persons): descriptive-stats [9 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing |   :min |    :max |
  ;;   |-------------|-----------|---------:|-----------:|-------:|--------:|
  ;;   |    :lad23cd |   :string |   405132 |          0 |        |         |
  ;;   |    :lad23nm |   :string |   405132 |          0 |        |         |
  ;;   |  :ctyua23cd |   :string |   405132 |          0 |        |         |
  ;;   |  :ctyua23nm |   :string |   405132 |          0 |        |         |
  ;;   |    :country |   :string |   405132 |          0 |        |         |
  ;;   |       :year |    :int16 |   405132 |          0 | 2011.0 |  2024.0 |
  ;;   |        :age |     :int8 |   405132 |          0 |    0.0 |    90.0 |
  ;;   |        :sex |   :string |   405132 |          0 |        |         |
  ;;   | :population |  :float64 |   405132 |          0 |    0.0 | 23620.0 |
  ;;   
  
  :rcf)

(defn ds-by-lad->ds-by-ctyua
  "Roll up dataset `ds` of population by LA District to the County/Unitary Authority level."
  [ds & _]
  (as-> ds $
    (dsr/group-by-column-agg
     (tc/column-names $
                      (complement
                       (into #{}
                             (tc/column-names $
                                              #"^:(lad\d\dcd|lad\d\dnm|population)$"))))
     {:population (dsr/sum :population)}
     $)
    (tc/order-by $ (tc/column-names $))
    (tc/set-dataset-name $ (str/replace (tc/dataset-name ds) "LAD" "CTYUA"))))

(defn ->dataset-by-ctyua
  [& {:as options}]
  (-> (->dataset-by-lad options)
      (ds-by-lad->ds-by-ctyua options)))

(comment ;; Structure of (unfiltered) dataset by CTYUA for default options
  (-> nil
      ->dataset-by-ctyua
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
  ;;=> [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: long by CTYUA (persons): descriptive-stats [7 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing |   :min |    :max |
  ;;   |-------------|-----------|---------:|-----------:|-------:|--------:|
  ;;   |  :ctyua23cd |   :string |   222950 |          0 |        |         |
  ;;   |  :ctyua23nm |   :string |   222950 |          0 |        |         |
  ;;   |    :country |   :string |   222950 |          0 |        |         |
  ;;   |       :year |    :int64 |   222950 |          0 | 2011.0 |  2024.0 |
  ;;   |        :age |     :int8 |   222950 |          0 |    0.0 |    90.0 |
  ;;   |        :sex |   :string |   222950 |          0 |        |         |
  ;;   | :population |  :float64 |   222950 |          0 |    0.0 | 23620.0 |
  ;;   
  
  :rcf)

(defn ->dataset
  [& {:as options}]
  (->dataset-by-ctyua options))

(comment ;; Structure of dataset for default options
  (-> nil
      ->dataset
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
  ;;=> [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: long by CTYUA (persons): descriptive-stats [7 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing |   :min |    :max |
  ;;   |-------------|-----------|---------:|-----------:|-------:|--------:|
  ;;   |  :ctyua23cd |   :string |   222950 |          0 |        |         |
  ;;   |  :ctyua23nm |   :string |   222950 |          0 |        |         |
  ;;   |    :country |   :string |   222950 |          0 |        |         |
  ;;   |       :year |    :int64 |   222950 |          0 | 2011.0 |  2024.0 |
  ;;   |        :age |     :int8 |   222950 |          0 |    0.0 |    90.0 |
  ;;   |        :sex |   :string |   222950 |          0 |        |         |
  ;;   | :population |  :float64 |   222950 |          0 |    0.0 | 23620.0 |
  ;;   
  
  :rcf)