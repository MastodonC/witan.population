(ns witan.population.england.snpp-2022
  "Functions to read and process ONS Subnational Population Projections 
   for LAs by single year of age and sex from 
   https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/datasets/localauthoritiesinenglandz1
   into the form required for `witan.send` modelling."
  (:require [clojure.java.io :as io]
            [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.reductions :as dsr]))

;;; # Parameters
;;; ## Defaults
(def default-resource-file-name 
  "Name of resource file containing subnational population projections
   (for LAs by single year of age and sex) to use by default.

   Note: Defaulting to the 2022-based: Five-year migration variant projection edition
   as this \"aligns with the principal projection in the 2022-based national 
   population projections\" and \"is a better reflection of short-term population change_
   _because of more up-to-date migration data\"
   (see https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/bulletins/subnationalpopulationprojectionsforengland/2022based
   for details)."
  "2022snpppopulationsyoa5yr/2022 SNPP Population persons.csv")

(def LTLA22->UTLA22-lookup-default-resource-file-name
  "Name of resource file mapping Lower Tier Local Authority codes/names to Upper Tier Local Authorities.
   Using 2022 file as these match the Lower Tier Local Authority codes used in the 2022 based SNPP.
   From: https://open-geography-portalx-ons.hub.arcgis.com/datasets/ons::local-authority-district-to-county-and-unitary-authority-december-2022-lookup-in-ew/about"
  "Lower_Tier_Local_Authority_to_Upper_Tier_Local_Authority_(December_2022)_Lookup_in_England_and_Wales.csv")

(def output-columns
  "Output columns for `witan.send` population.csv file."
  [:calendar-year :academic-year :population])

(def default-min-academic-year -4)
(def default-max-academic-year 20)



;;; # Raw data files
;;; ## Lower Tier LA to Upper Tier LA Lookup
(defn LTLA22->UTLA22
  "Read Lower Tier LA (area) to Upper Tier LA lookup from CSV file
   specified by either `LTLA22->UTLA22-lookup-file-path` or `LTLA22->UTLA22-lookup-resource-file-name`,
   defaulting to `LTLA22->UTLA22-lookup-default-resource-file-name` if neither specified."
  [& {:keys [LTLA22->UTLA22-lookup-resource-file-name
             LTLA22->UTLA22-lookup-file-path
             LTLA22->UTLA22-lookup-dataset-name]
      :or {LTLA22->UTLA22-lookup-resource-file-name LTLA22->UTLA22-lookup-default-resource-file-name}}]
  (with-open [in (-> (or LTLA22->UTLA22-lookup-file-path (io/resource LTLA22->UTLA22-lookup-resource-file-name))
                     io/file
                     io/input-stream)]
    (-> (ds/->dataset in {:file-type    :csv
                          :separator    ","
                          :header-row?  true
                          :key-fn       keyword
                          :parser-fn    :string
                          :dataset-name (or LTLA22->UTLA22-lookup-dataset-name
                                            LTLA22->UTLA22-lookup-file-path
                                            LTLA22->UTLA22-lookup-resource-file-name)}))))

(comment ;; EDA
  ;; Dataset structure
  (-> (LTLA22->UTLA22)
      (-> tc/info (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])))
  ;;=> Lower_Tier_Local_Authority_to_Upper_Tier_Local_Authority_(December_2022)_Lookup_in_England_and_Wales.csv: descriptive-stats [5 4]:
  ;;   
  ;;   | :col-name | :datatype | :n-valid | :n-missing |
  ;;   |-----------|-----------|---------:|-----------:|
  ;;   | :LTLA22CD |   :string |      331 |          0 |
  ;;   | :LTLA22NM |   :string |      331 |          0 |
  ;;   | :UTLA22CD |   :string |      331 |          0 |
  ;;   | :UTLA22NM |   :string |      331 |          0 |
  ;;   | :ObjectId |   :string |      331 |          0 |
  ;;   
  
  ;; Number of distinct `LTLA22CD`s
  (-> (LTLA22->UTLA22)
      :LTLA22CD distinct count)
  ;;=> 331
  
  :rcf)


;;; ## SNPPs
(defn ->ds
  "Read SNPPs from CSV file specified by either `resource-file-name` or `file-path`,
  defaulting to `default-resource-file-name` if neither specified."
  [& {::keys [resource-file-name file-path dataset-name]
      :or    {resource-file-name default-resource-file-name}}]
  (with-open [in (-> (or file-path (io/resource resource-file-name))
                     io/file
                     io/input-stream)]
    (-> (ds/->dataset in {:file-type    :csv
                          :separator    ","
                          :header-row?  true
                          :key-fn       #(get {"AREA_CODE" :area-code
                                               "AREA_NAME" :area-name
                                               "COMPONENT" :component
                                               "SEX"       :sex
                                               "AGE_GROUP" :age-group} % %)
                          :parser-fn    {:area-code :string
                                         :area-name :string
                                         :component :string
                                         :sex       :string
                                         :age-group :string}
                          :dataset-name (or dataset-name file-path resource-file-name)}))))

(comment ;; EDA
  ;; Dataset structure
  (-> (->ds)
      (-> tc/info (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])))
  ;;=> 2022snpppopulationsyoa5yr/2022 SNPP Population persons.csv: descriptive-stats [31 6]:
  ;;   
  ;;   |  :col-name | :datatype | :n-valid | :n-missing |   :min |        :max |
  ;;   |------------|-----------|---------:|-----------:|-------:|------------:|
  ;;   | :area-code |   :string |    28428 |          0 |        |             |
  ;;   | :area-name |   :string |    28428 |          0 |        |             |
  ;;   | :component |   :string |    28428 |          0 |        |             |
  ;;   |       :sex |   :string |    28428 |          0 |        |             |
  ;;   | :age-group |   :string |    28428 |          0 |        |             |
  ;;   |       2022 |  :float64 |    28428 |          0 |  0.000 | 1154221.000 |
  ;;   |       2023 |  :float64 |    28428 |          0 |  4.662 | 1171903.333 |
  ;;   |       2024 |  :float64 |    28428 |          0 |  9.259 | 1184518.703 |
  ;;   |       2025 |  :float64 |    28428 |          0 |  8.214 | 1194143.603 |
  ;;   |       2026 |  :float64 |    28428 |          0 |  7.497 | 1201086.852 |
  ;;   |       2027 |  :float64 |    28428 |          0 |  7.549 | 1205580.091 |
  ;;   |       2028 |  :float64 |    28428 |          0 |  9.723 | 1207424.217 |
  ;;   |       2029 |  :float64 |    28428 |          0 |  9.180 | 1209339.773 |
  ;;   |       2030 |  :float64 |    28428 |          0 |  8.349 | 1211371.151 |
  ;;   |       2031 |  :float64 |    28428 |          0 |  8.705 | 1213427.119 |
  ;;   |       2032 |  :float64 |    28428 |          0 |  8.044 | 1215316.851 |
  ;;   |       2033 |  :float64 |    28428 |          0 | 13.322 | 1217062.159 |
  ;;   |       2034 |  :float64 |    28428 |          0 | 11.897 | 1218830.549 |
  ;;   |       2035 |  :float64 |    28428 |          0 | 11.694 | 1220760.267 |
  ;;   |       2036 |  :float64 |    28428 |          0 | 12.021 | 1222655.762 |
  ;;   |       2037 |  :float64 |    28428 |          0 | 11.136 | 1224544.146 |
  ;;   |       2038 |  :float64 |    28428 |          0 | 10.494 | 1226455.229 |
  ;;   |       2039 |  :float64 |    28428 |          0 | 10.044 | 1228377.995 |
  ;;   |       2040 |  :float64 |    28428 |          0 |  9.701 | 1230264.113 |
  ;;   |       2041 |  :float64 |    28428 |          0 | 10.100 | 1232235.957 |
  ;;   |       2042 |  :float64 |    28428 |          0 |  9.824 | 1233986.214 |
  ;;   |       2043 |  :float64 |    28428 |          0 |  9.016 | 1235749.735 |
  ;;   |       2044 |  :float64 |    28428 |          0 |  8.493 | 1237617.456 |
  ;;   |       2045 |  :float64 |    28428 |          0 |  8.814 | 1239571.921 |
  ;;   |       2046 |  :float64 |    28428 |          0 | 10.267 | 1241576.795 |
  ;;   |       2047 |  :float64 |    28428 |          0 |  9.676 | 1243585.457 |
  ;;   

  ;; Note non digit values of `:age-group` strings
  (->> (->ds) :age-group distinct (remove (partial re-matches #"\d+")))
  ;;=> ("90 and over" "All ages")
    
  ;; Number of distinct `:area-code`s
  (-> (->ds) :area-code distinct count)
  ;;=> 309

  ;; Any `:area-code`s in default SNPP file not in default `LTLA22->UTLA22`?
  (->> (->ds) :area-code distinct (remove (into #{} (-> (LTLA22->UTLA22) :LTLA22CD distinct))) count)
  ;;=> 0

  ;; Note `:LTA22CD`s in default `LTLA22->UTLA22` that are not in the default SNPP are for Wales:
  (->> (->ds) :area-code distinct (remove (into #{} (-> (LTLA22->UTLA22) :LTLA22CD distinct))) count)
  (-> (LTLA22->UTLA22)
      (tc/drop-rows (comp (into #{} (-> (->ds) :area-code distinct))))
      (vary-meta assoc :print-index-range 1000))
  ;;=> Lower_Tier_Local_Authority_to_Upper_Tier_Local_Authority_(December_2022)_Lookup_in_England_and_Wales.csv [22 5]:
  ;;   
  ;;   | :LTLA22CD |         :LTLA22NM | :UTLA22CD |         :UTLA22NM | :ObjectId |
  ;;   |-----------|-------------------|-----------|-------------------|-----------|
  ;;   | W06000001 |  Isle of Anglesey | W06000001 |  Isle of Anglesey |       310 |
  ;;   | W06000002 |           Gwynedd | W06000002 |           Gwynedd |       311 |
  ;;   | W06000003 |             Conwy | W06000003 |             Conwy |       312 |
  ;;   | W06000004 |      Denbighshire | W06000004 |      Denbighshire |       313 |
  ;;   | W06000005 |        Flintshire | W06000005 |        Flintshire |       314 |
  ;;   | W06000006 |           Wrexham | W06000006 |           Wrexham |       315 |
  ;;   | W06000008 |        Ceredigion | W06000008 |        Ceredigion |       316 |
  ;;   | W06000009 |     Pembrokeshire | W06000009 |     Pembrokeshire |       317 |
  ;;   | W06000010 |   Carmarthenshire | W06000010 |   Carmarthenshire |       318 |
  ;;   | W06000011 |           Swansea | W06000011 |           Swansea |       319 |
  ;;   | W06000012 | Neath Port Talbot | W06000012 | Neath Port Talbot |       320 |
  ;;   | W06000013 |          Bridgend | W06000013 |          Bridgend |       321 |
  ;;   | W06000014 | Vale of Glamorgan | W06000014 | Vale of Glamorgan |       322 |
  ;;   | W06000015 |           Cardiff | W06000015 |           Cardiff |       323 |
  ;;   | W06000016 | Rhondda Cynon Taf | W06000016 | Rhondda Cynon Taf |       324 |
  ;;   | W06000018 |        Caerphilly | W06000018 |        Caerphilly |       325 |
  ;;   | W06000019 |     Blaenau Gwent | W06000019 |     Blaenau Gwent |       326 |
  ;;   | W06000020 |           Torfaen | W06000020 |           Torfaen |       327 |
  ;;   | W06000021 |     Monmouthshire | W06000021 |     Monmouthshire |       328 |
  ;;   | W06000022 |           Newport | W06000022 |           Newport |       329 |
  ;;   | W06000023 |             Powys | W06000023 |             Powys |       330 |
  ;;   | W06000024 |    Merthyr Tydfil | W06000024 |    Merthyr Tydfil |       331 |
  ;;   
    
  :rcf)



;;; # Format for `witan.send` use
;; For `witan.send` use, want:
;; - Lower Tier LAs rolled up to Upper Tier LAs
;; - long format dataset with SNPPs for different years in separate rows
;; - `witan.send` columns added:
;;   - `:calendar-year` - Year of corresponding SEN2 census date.
;;   - `:academic-year` - NCY corresponding to age group.
;; - …and possibly filtered for UTLAs, NCYs & `:calendar-year`s

(defn ds->witan-send-population
  "Given SNPP dataset `ds` and Lower Tier LA to Upper Tier LA lookup `LTLA22->UTLA22`,
   returns a long dataset with SNPP `:population` estimates by `:snpp-year` 
   rolled up to the Upper Tier LA level, with `witan.send` variables 
   `:calendar-year` and `:academic-year` added.
   Dataset can be filtered by specifying (optional) values for:
   - Upper Tier LA: via code (string) `UTLA22CD` or name (string) `UTLA22NM`,
     or (for backwards compatibility) via name (string) `la-name`.
   - NCYs: via (integer) `min-academic-year` and/or `max-academic-year`. 
   - `:calendar-year`s: via (integer) `min-calendar-year` and/or `max-calendar-year`,
     or (for backwards compatibility) via (integer) `max-year`."
  ;; Note that compared to `witan.population.england.snpp-2018/snpp-2018->witan-send-population`
  ;; `:calendar-year` differs by +1 and `:academic-year` by +1,
  ;; due to consideration of SNPPs as mid-year estimates.
  [ds LTLA22->UTLA22 & {:keys [UTLA22CD UTLA22NM la-name
                               min-academic-year max-academic-year
                               min-calendar-year max-calendar-year max-year]
                        :or   {min-academic-year default-min-academic-year
                               max-academic-year default-max-academic-year}}]
  (-> ds
      ;; Derive `:academic-year` from `:age-group`:
      ;; - The SNPPs are mid-year estiamtes.
      ;; - Therefore the age is (almost) the age on 31st August, i.e. at the start of the school year.
      ;; - Per https://www.gov.uk/national-curriculum, 
      ;;   children aged 5 at the start of the school year should be in NCY 1.
      ;; - Thus the offset between age at the start of the school year and NCY is -4.
      (tc/drop-rows (comp #{"90 and over" "All ages"} :age-group))
      (tc/map-columns :age :int8 [:age-group] parse-long)
      (tc/map-columns :academic-year :int8 [:age] #(- % 4))
      ;; Select required `:academic-year`s (if specified)
      (cond->
       min-academic-year (tc/select-rows #(-> % :academic-year (>= min-academic-year)))
       max-academic-year (tc/select-rows #(-> % :academic-year (<= max-academic-year))))
      ;; Merge in Upper Tier LA codes and names
      (tc/left-join (tc/select-columns LTLA22->UTLA22 [:LTLA22CD :LTLA22NM
                                                       :UTLA22CD :UTLA22NM])
                    {:left  [:area-code]
                     :right [:LTLA22CD]})
      (tc/reorder-columns [:area-code :area-name :LTLA22CD :LTLA22NM :UTLA22CD :UTLA22NM])
      ;; Select Upper Tier LA (if specified)
      (cond->
       UTLA22CD (tc/select-rows #(-> % :UTLA22CD (= UTLA22CD)))
       UTLA22NM (tc/select-rows #(-> % :UTLA22NM (= UTLA22NM)))
       la-name  (tc/select-rows #(-> % :UTLA22NM (= la-name))))
      ;; Pivot long with SNPP year in `:snpp-year` and projections in `:population`
      (tc/pivot->longer #"^\d+$" {:target-columns :snpp-year, :value-column-name :population})
      ;; Roll-up to Upper Tier LA level
      (as-> $ (dsr/group-by-column-agg 
               (tc/column-names $ (complement #{:area-code :area-name :LTLA22CD :LTLA22NM :population}))
               {:population (dsr/sum :population)}
               $))
      ;; Derive `:calendar-year` from `:snpp-year`:
      ;; - The SNPPs are mid-year estiamtes.
      ;; - So are the population going into the next school year.
      ;; - Which will be reported in the following year's SEN2 census.
      ;; - So `:calendar-year` (the year for the corresponding SEN2 census date) is one more than `:snpp-year`.
      (tc/convert-types {:snpp-year :int16})
      (tc/map-columns :calendar-year :int16 [:snpp-year] inc)
      ;; Select `:calendar-year`s (if specified)
      (cond->
       min-calendar-year (tc/select-rows #(-> % :calendar-year (>= min-calendar-year)))
       max-calendar-year (tc/select-rows #(-> % :calendar-year (<= max-calendar-year)))
       max-year          (tc/select-rows #(-> % :calendar-year (<= max-year))))
      ;; Arrange dataset
      (tc/reorder-columns [:UTLA22CD :UTLA22NM
                           :snpp-year :calendar-year
                           :age-group :age :academic-year
                           :component :sex
                           :population])
      (tc/order-by [:snpp-year :age])
      (tc/set-dataset-name "SNPP 2022 by UTLA by NCY and SEN2 calendar year")))

(defn ->witan-send-population
  "Reads SNPPs from CSV file specified by either `resource-file-name` or `file-path`
   (defaulting to `default-resource-file-name` if neither specified),
   rolls up to Upper Tier LA level using LTLA22->UTLA22 lookup read from CSV file
   specified by either `LTLA22->UTLA22-lookup-file-path` or `LTLA22->UTLA22-lookup-resource-file-name`,
   (defaulting to `LTLA22->UTLA22-lookup-default-resource-file-name` if neither specified)
   and returns a long dataset with SNPP `:population` estimates by `:snpp-year` 
   rolled up to the Upper Tier LA level, with `witan.send` variables 
   `:calendar-year` and `:academic-year` added.
   Dataset can be filtered by specifying (optional) values for:
   - Upper Tier LA: via code (string) `UTLA22CD` or name (string) `UTLA22NM`,
     or (for backwards compatibility) via name (string) `la-name`.
   - NCYs: via (integer) `min-academic-year` and/or `max-academic-year`. 
   - `:calendar-year`s: via (integer) `min-calendar-year` and/or `max-calendar-year`,
     or (for backwards compatibility) via (integer) `max-year`."
  [& {::keys [resource-file-name file-path dataset-name]
      :keys  [LTLA22->UTLA22-lookup-resource-file-name
              LTLA22->UTLA22-lookup-file-path
              LTLA22->UTLA22-lookup-dataset-name
              UTLA22CD UTLA22NM la-name
              min-academic-year max-academic-year
              min-calendar-year max-calendar-year max-year]
      :as    options}]
  (ds->witan-send-population
   (->ds (select-keys options [::resource-file-name ::file-path ::dataset-name]))
   (LTLA22->UTLA22 (select-keys options [:LTLA22->UTLA22-lookup-resource-file-name
                                         :LTLA22->UTLA22-lookup-file-path
                                         :LTLA22->UTLA22-lookup-dataset-name]))
   (dissoc options [::resource-file-name ::file-path ::dataset-name
                    :LTLA22->UTLA22-lookup-resource-file-name
                    :LTLA22->UTLA22-lookup-file-path
                    :LTLA22->UTLA22-lookup-dataset-name])))

(comment ;; EDA
  (-> (->witan-send-population)
      (-> tc/info (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])))
  ;;=> SNPP 2022 by UTLA by NCY and SEN2 calendar year: descriptive-stats [10 6]:
  ;;   
  ;;   |      :col-name | :datatype | :n-valid | :n-missing |   :min |      :max |
  ;;   |----------------|-----------|---------:|-----------:|-------:|----------:|
  ;;   |      :UTLA22CD |   :string |    98800 |          0 |        |           |
  ;;   |      :UTLA22NM |   :string |    98800 |          0 |        |           |
  ;;   |     :snpp-year |    :int16 |    98800 |          0 | 2022.0 |  2047.000 |
  ;;   | :calendar-year |    :int16 |    98800 |          0 | 2023.0 |  2048.000 |
  ;;   |     :age-group |   :string |    98800 |          0 |        |           |
  ;;   |           :age |     :int8 |    98800 |          0 |    0.0 |    24.000 |
  ;;   | :academic-year |     :int8 |    98800 |          0 |   -4.0 |    20.000 |
  ;;   |     :component |   :string |    98800 |          0 |        |           |
  ;;   |           :sex |   :string |    98800 |          0 |        |           |
  ;;   |    :population |  :float64 |    98800 |          0 |    0.0 | 26633.201 |
  ;;   
  
  :rcf)



;;; # Write to CSV file
(defn write-witan-send-population!
  "Writes required output columns from `witan-send-population` dataset to CSV file `file-name`."
  [witan-send-population file-name]
  (-> witan-send-population
      (tc/select-columns output-columns)
      (tc/write! file-name)))

(defn create-send-population-file!
  "Creates `witan.send` population CSV file from SNPPs and writes to `file-name`.
   Reads SNPPs from CSV file specified by either `resource-file-name` or `file-path`
   (defaulting to `default-resource-file-name` if neither specified),
   reads LTLA22->UTLA22 lookup from CSV file specified by either 
   `LTLA22->UTLA22-lookup-file-path` or `LTLA22->UTLA22-lookup-resource-file-name`,
   (defaulting to `LTLA22->UTLA22-lookup-default-resource-file-name` if neither specified).
   Filters dataset (optionally) for:
   - Upper Tier LA: via code (string) `UTLA22CD` or name (string) `UTLA22NM`,
     or (for backwards compatibility) via name (string) `la-name`.
   - NCYs: via (integer) `min-academic-year` and/or `max-academic-year`. 
   - `:calendar-year`s: via (integer) `min-calendar-year` and/or `max-calendar-year`,
     or (for backwards compatibility) via (integer) `max-year`."
  [& {::keys [resource-file-name file-path dataset-name]
      :keys [LTLA22->UTLA22-lookup-resource-file-name
             LTLA22->UTLA22-lookup-file-path
             LTLA22->UTLA22-lookup-dataset-name
             UTLA22CD UTLA22NM la-name
             min-academic-year max-academic-year
             min-calendar-year max-calendar-year max-year
             file-name]
      :as   options}]
  (-> (->witan-send-population (dissoc options [:file-name]))
      (write-witan-send-population! file-name)))
