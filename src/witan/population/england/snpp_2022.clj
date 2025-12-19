(ns witan.population.england.snpp-2022
  "Functions to read and process 2022 based ONS Subnational Population Projections 
   for LAs by single year of age and sex downloaded from: 
   https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/datasets/localauthoritiesinenglandz1
   Note that per the [Subnational population projections QMI](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/methodologies/subnationalpopulationprojectionsqmi#quality-characteristics-of-the-subnational-population-projections-data)
   geographic boundaries existing as at mid-2022, except for the migration 
   category variant which is also available on the geographic boundaries 
   existing as at mid-2023."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.reductions :as dsr]
            [tablecloth.api :as tc]
            [witan.population.lookups.lad-to-ctyua :as lad->ctyua]))

(def resource-options
  "Option maps for resource files containing 2022 based ONS Subnational 
   Population Projections for LAs by single year of age and sex."
  {;; 2022-based: Five-year migration variant projection edition: 
   ;; aligns with the principal projection in the 2022-based NPPs.
   "2022snpppopulationsyoa5yr-persons"
   {::resource-file-name "2022snpppopulationsyoa/2022snpppopulationsyoa5yr/2022 SNPP Population persons.csv"
    ::geography-year-yy  "22"}
   ;; 2022-based: Migration category variant edition (2025-06-24 release):
   ;; aligns with the SNPP projections ONS now advise using as it
   ;; "is a better reflection of short-term population change".
   ;; See: https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/bulletins/subnationalpopulationprojectionsforengland/2022based
   "2022snpppopulationsyoamigcat-persons"
   {::resource-file-name "2022snpppopulationsyoa/2022snpppopulationsyoamigcat_2025-06-24/2022 SNPP Population persons.csv"
    ::geography-year-yy "22"}
   ;; 2022-based: Migration category variant edition (2023 geographies):
   ;; aligns with the SNPP projections ONS now advise using as it
   ;; "is a better reflection of short-term population change"
   ;; and available for 2023 geographies.
   ;; See: https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/bulletins/subnationalpopulationprojectionsforengland/2022based
   "2022snpppopulationsyoamigcat23-persons"
   {::resource-file-name "2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv"
    ::geography-year-yy "23"}})

(def default-resource-options
  "Defaulting to the 2022-based migration category variant edition (2023 geographies)
   as it is the projection ONS advise using as \"a better reflection of short-term 
   population change because of more up-to-date migration data\" and because it
   uses 2023 geographies.
   See: https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/bulletins/subnationalpopulationprojectionsforengland/2022based
   NOTE: We previously used the five-year migration variant projection edition
   as it \"aligns with the principal projection in the 2022-based national 
   population projections\"."
  (get resource-options "2022snpppopulationsyoamigcat23-persons"))

(defn ->dataset-raw
  "Read SNPPs from CSV file into a dataset.
   Specify CSV file by either via `:file-path` or `:resource-file-name`, 
   and identify the `:geography-year-yy`."
  [& {::keys [file-path
              resource-file-name
              geography-year-yy
              dataset-name]
      :or {resource-file-name (default-resource-options ::resource-file-name)
           geography-year-yy  (default-resource-options ::geography-year-yy)}}]
  (let [ladcd-col           (keyword (str "lad"   geography-year-yy "cd"))
        ladnm-col           (keyword (str "lad"   geography-year-yy "nm"))]
    (with-open [in (-> (or file-path (io/resource resource-file-name))
                       io/file
                       io/input-stream)]
      (-> in
          (ds/->dataset {:file-type    :csv
                         :separator    ","
                         :header-row?  true
                         :key-fn       #(get {"AREA_CODE" ladcd-col
                                              "AREA_NAME" ladnm-col
                                              "COMPONENT" :component
                                              "SEX"       :sex
                                              "AGE_GROUP" :age-group} % %)
                         :parser-fn    {ladcd-col  :string
                                        ladnm-col  :string
                                        :component :string
                                        :sex       :string
                                        :age-group :string}
                         :dataset-name (or dataset-name
                                           file-path
                                           resource-file-name)})))))

(comment ;; Structure of raw dataset for default options
  (-> nil
      ->dataset-raw
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
  ;;=> 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: descriptive-stats [31 6]:
  ;;   
  ;;   |  :col-name | :datatype | :n-valid | :n-missing |   :min |        :max |
  ;;   |------------|-----------|---------:|-----------:|-------:|------------:|
  ;;   |   :lad23cd |   :string |    27232 |          0 |        |             |
  ;;   |   :lad23nm |   :string |    27232 |          0 |        |             |
  ;;   | :component |   :string |    27232 |          0 |        |             |
  ;;   |       :sex |   :string |    27232 |          0 |        |             |
  ;;   | :age-group |   :string |    27232 |          0 |        |             |
  ;;   |       2022 |  :float64 |    27232 |          0 |  0.000 | 1154221.000 |
  ;;   |       2023 |  :float64 |    27232 |          0 |  4.662 | 1171883.772 |
  ;;   |       2024 |  :float64 |    27232 |          0 |  9.259 | 1184485.358 |
  ;;   |       2025 |  :float64 |    27232 |          0 |  8.214 | 1186073.704 |
  ;;   |       2026 |  :float64 |    27232 |          0 |  7.497 | 1185349.390 |
  ;;   |       2027 |  :float64 |    27232 |          0 |  7.547 | 1184849.809 |
  ;;   |       2028 |  :float64 |    27232 |          0 |  9.722 | 1185144.199 |
  ;;   |       2029 |  :float64 |    27232 |          0 |  9.178 | 1186352.129 |
  ;;   |       2030 |  :float64 |    27232 |          0 |  8.347 | 1187965.000 |
  ;;   |       2031 |  :float64 |    27232 |          0 |  8.703 | 1189588.922 |
  ;;   |       2032 |  :float64 |    27232 |          0 |  8.042 | 1191039.490 |
  ;;   |       2033 |  :float64 |    27232 |          0 | 13.319 | 1192345.997 |
  ;;   |       2034 |  :float64 |    27232 |          0 | 11.898 | 1193681.988 |
  ;;   |       2035 |  :float64 |    27232 |          0 | 11.682 | 1195192.264 |
  ;;   |       2036 |  :float64 |    27232 |          0 | 12.018 | 1196686.616 |
  ;;   |       2037 |  :float64 |    27232 |          0 | 11.133 | 1198192.755 |
  ;;   |       2038 |  :float64 |    27232 |          0 | 10.493 | 1199742.489 |
  ;;   |       2039 |  :float64 |    27232 |          0 | 10.040 | 1201325.472 |
  ;;   |       2040 |  :float64 |    27232 |          0 |  9.697 | 1202892.945 |
  ;;   |       2041 |  :float64 |    27232 |          0 | 10.099 | 1204565.124 |
  ;;   |       2042 |  :float64 |    27232 |          0 |  9.821 | 1206032.551 |
  ;;   |       2043 |  :float64 |    27232 |          0 |  9.012 | 1207534.118 |
  ;;   |       2044 |  :float64 |    27232 |          0 |  8.489 | 1209148.391 |
  ;;   |       2045 |  :float64 |    27232 |          0 |  8.813 | 1210834.814 |
  ;;   |       2046 |  :float64 |    27232 |          0 | 10.265 | 1212554.401 |
  ;;   |       2047 |  :float64 |    27232 |          0 |  9.678 | 1214273.444 |
  ;;   

  :rcf)

(comment ;; EDA of raw dataset for default options
  ;; Note non digit values of `:age-group` strings
  (->> nil ->dataset-raw :age-group distinct (remove (partial re-matches #"\d+")))
  ;;=> ("90 and over" "All ages")

  ;; Number of distinct `:lad##cd`s
  (-> nil ->dataset-raw (tc/select-columns #"^:lad\d\dcd$") vals first distinct count)
  ;;=> 296

  ;; Any `:lad23cd`s in default SNPP file not in `lad->ctyua/->dataset` for 2023?
  (-> nil
      ->dataset-raw
      (tc/select-columns #"^:lad\d\d(cd|nm)$")
      tc/unique-by
      (tc/anti-join (lad->ctyua/->dataset {::lad->ctyua/geography-year-yy "23"})
                    [:lad23cd]))
  ;;=> 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv [0 2]:
  ;;   
  ;;   | :lad23cd | :lad23nm |
  ;;   |----------|----------|
  ;;   

  ;; Note `:lad23cd`s in default SNPP file not in `lad->ctyua/->dataset` for 2022?
  (-> nil
      ->dataset-raw
      (tc/select-columns #"^:lad\d\d(cd|nm)$")
      tc/unique-by
      (tc/anti-join (lad->ctyua/->dataset {::lad->ctyua/geography-year-yy "22"})
                    {:left [:lad23cd], :right [:lad22cd]}))
  ;;=> 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv [4 2]:
  ;;   
  ;;   |  :lad23cd |                :lad23nm |
  ;;   |-----------|-------------------------|
  ;;   | E06000063 |              Cumberland |
  ;;   | E06000064 | Westmorland and Furness |
  ;;   | E06000065 |         North Yorkshire |
  ;;   | E06000066 |                Somerset |
  ;;   

  ;; Note `:lda23cd`s in 2023 lookup that are not in the default SNPP for England are for Wales (as expected):
  (-> (lad->ctyua/->dataset {::lad->ctyua/geography-year-yy "23"})
      (tc/drop-rows (comp (into #{} (-> nil ->dataset-raw :lad23cd distinct)) :lad23cd))
      (tc/drop-columns #".+nmw$")
      (tc/drop-columns [:objectid])
      (vary-meta assoc :print-index-range 1000))
  ;;=> LUP_LAD_CTYUA/Local_Authority_District_to_County_and_Unitary_Authority_(April_2023)_Lookup_in_EW.csv [22 4]:
  ;;   
  ;;   |  :lad23cd |          :lad23nm | :ctyua23cd |        :ctyua23nm |
  ;;   |-----------|-------------------|------------|-------------------|
  ;;   | W06000001 |  Isle of Anglesey |  W06000001 |  Isle of Anglesey |
  ;;   | W06000002 |           Gwynedd |  W06000002 |           Gwynedd |
  ;;   | W06000003 |             Conwy |  W06000003 |             Conwy |
  ;;   | W06000004 |      Denbighshire |  W06000004 |      Denbighshire |
  ;;   | W06000005 |        Flintshire |  W06000005 |        Flintshire |
  ;;   | W06000006 |           Wrexham |  W06000006 |           Wrexham |
  ;;   | W06000008 |        Ceredigion |  W06000008 |        Ceredigion |
  ;;   | W06000009 |     Pembrokeshire |  W06000009 |     Pembrokeshire |
  ;;   | W06000010 |   Carmarthenshire |  W06000010 |   Carmarthenshire |
  ;;   | W06000011 |           Swansea |  W06000011 |           Swansea |
  ;;   | W06000012 | Neath Port Talbot |  W06000012 | Neath Port Talbot |
  ;;   | W06000013 |          Bridgend |  W06000013 |          Bridgend |
  ;;   | W06000014 | Vale of Glamorgan |  W06000014 | Vale of Glamorgan |
  ;;   | W06000015 |           Cardiff |  W06000015 |           Cardiff |
  ;;   | W06000016 | Rhondda Cynon Taf |  W06000016 | Rhondda Cynon Taf |
  ;;   | W06000018 |        Caerphilly |  W06000018 |        Caerphilly |
  ;;   | W06000019 |     Blaenau Gwent |  W06000019 |     Blaenau Gwent |
  ;;   | W06000020 |           Torfaen |  W06000020 |           Torfaen |
  ;;   | W06000021 |     Monmouthshire |  W06000021 |     Monmouthshire |
  ;;   | W06000022 |           Newport |  W06000022 |           Newport |
  ;;   | W06000023 |             Powys |  W06000023 |             Powys |
  ;;   | W06000024 |    Merthyr Tydfil |  W06000024 |    Merthyr Tydfil |
  ;;   

  :rcf)

(defn ds-raw->ds-by-lad
  "Canonicalise raw LAD code/name level SNPP 2022 dataset `ds`
   by adding County/Unitary Authority codes & names and pivoting long by year.
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
                               (some (comp second (partial re-matches #"^lad(\d\d)cd") name)))
        ladcd-col   (keyword (str "lad"   geography-year-yy "cd"))
        ladnm-col   (keyword (str "lad"   geography-year-yy "nm"))
        ctyuacd-col (keyword (str "ctyua" geography-year-yy "cd"))
        ctyuanm-col (keyword (str "ctyua" geography-year-yy "nm"))]
    (as-> ds $
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
      ;; Calculate ages for single-year age groups
      (tc/map-columns $ :age #_:int8 [:age-group] parse-long)
      (tc/convert-types $ {:age :int8})
      ;; Filter for (integer) ages if requested
      (cond-> $
        min-age (tc/select-rows #(some-> % :age (>= min-age)))
        max-age (tc/select-rows #(some-> % :age (<= max-age))))
      ;; Pivot long for `:year`
      (tc/pivot->longer $ #"^\d+$" {:target-columns    :year
                                    :value-column-name :population
                                    :datatypes         {:year :int16}})
      ;; Filter for (integer) years if requested
      (cond-> $
        min-year (tc/select-rows #(some-> % :year (>= min-year)))
        max-year (tc/select-rows #(some-> % :year (<= max-year))))
      ;; Arrange dataset
      (tc/reorder-columns $ [ladcd-col ladnm-col ctyuacd-col ctyuanm-col
                             :year
                             :age-group :age
                             :component :sex
                             :population])
      (tc/set-dataset-name $ (str (tc/dataset-name ds) ": long by LAD")))))

(defn ->dataset-by-lad
  [& {:as options}]
  (-> (->dataset-raw options)
      (ds-raw->ds-by-lad options)))

(comment ;; Structure of (unfiltered) dataset by LAD for default options
  (-> nil
      ->dataset-by-lad
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
  ;;=> 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: long by LAD: descriptive-stats [10 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing |   :min |        :max |
  ;;   |-------------|-----------|---------:|-----------:|-------:|------------:|
  ;;   |    :lad23cd |   :string |   708032 |          0 |        |             |
  ;;   |    :lad23nm |   :string |   708032 |          0 |        |             |
  ;;   |  :ctyua23cd |   :string |   708032 |          0 |        |             |
  ;;   |  :ctyua23nm |   :string |   708032 |          0 |        |             |
  ;;   |       :year |    :int16 |   708032 |          0 | 2022.0 |    2047.000 |
  ;;   |  :age-group |   :string |   708032 |          0 |        |             |
  ;;   |        :age |     :int8 |   692640 |      15392 |    0.0 |      89.000 |
  ;;   |  :component |   :string |   708032 |          0 |        |             |
  ;;   |        :sex |   :string |   708032 |          0 |        |             |
  ;;   | :population |  :float64 |   708032 |          0 |    0.0 | 1214273.444 |
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
  ;;=> 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: long by CTYUA: descriptive-stats [8 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing |   :min |        :max |
  ;;   |-------------|-----------|---------:|-----------:|-------:|------------:|
  ;;   |  :ctyua23cd |   :string |   365976 |          0 |        |             |
  ;;   |  :ctyua23nm |   :string |   365976 |          0 |        |             |
  ;;   |       :year |    :int16 |   365976 |          0 | 2022.0 |    2047.000 |
  ;;   |  :age-group |   :string |   365976 |          0 |        |             |
  ;;   |        :age |     :int8 |   358020 |       7956 |    0.0 |      89.000 |
  ;;   |  :component |   :string |   365976 |          0 |        |             |
  ;;   |        :sex |   :string |   365976 |          0 |        |             |
  ;;   | :population |  :float64 |   365976 |          0 |    0.0 | 1848534.511 |
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
  ;;=> 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: long by CTYUA: descriptive-stats [8 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing |   :min |        :max |
  ;;   |-------------|-----------|---------:|-----------:|-------:|------------:|
  ;;   |  :ctyua23cd |   :string |   365976 |          0 |        |             |
  ;;   |  :ctyua23nm |   :string |   365976 |          0 |        |             |
  ;;   |       :year |    :int16 |   365976 |          0 | 2022.0 |    2047.000 |
  ;;   |  :age-group |   :string |   365976 |          0 |        |             |
  ;;   |        :age |     :int8 |   358020 |       7956 |    0.0 |      89.000 |
  ;;   |  :component |   :string |   365976 |          0 |        |             |
  ;;   |        :sex |   :string |   365976 |          0 |        |             |
  ;;   | :population |  :float64 |   365976 |          0 |    0.0 | 1848534.511 |
  ;;   

  :rcf)
