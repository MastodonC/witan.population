(ns witan.population.lookups.lad-to-ctyua
  "Lookups mapping UK Local Authority Districts to County and Unitary Authority.
   Data from: https://geoportal.statistics.gov.uk/search?q=LUP_LTLA_UTLA.
   Note dataset columns (<=2023) are (re)named using the aliases: LTLA->LAD & UTLA->CTYUA."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [tech.v3.dataset :as ds]))

(def LAD22-CTYUA22-EW-LU-resource-file-name
  "Name of resource file mapping 2022 Local Authority Districts in England & Wales to County and Unitary Authority.
   From: https://open-geography-portalx-ons.hub.arcgis.com/datasets/ons::local-authority-district-to-county-and-unitary-authority-december-2022-lookup-in-ew/about"
  "LUP_LAD_CTYUA/Lower_Tier_Local_Authority_to_Upper_Tier_Local_Authority_(December_2022)_Lookup_in_England_and_Wales.csv")

(def LAD23-CTYUA23-EQ-LU-resource-file-name
  "Name of resource file mapping 2023 Local Authority Districts in England & Wales to County and Unitary Authority.
   From: https://geoportal.statistics.gov.uk/datasets/ons::local-authority-district-to-county-and-unitary-authority-april-2023-lookup-in-ew/about"
  "LUP_LAD_CTYUA/Local_Authority_District_to_County_and_Unitary_Authority_(April_2023)_Lookup_in_EW.csv")

(def LAD24-CTYUA24-EQ-LU-resource-file-name
  "Name of resource file mapping 2024 Local Authority Districts in England & Wales to County and Unitary Authority.
   From: https://geoportal.statistics.gov.uk/datasets/ons::local-authority-district-to-county-and-unitary-authority-december-2024-lookup-in-ew/about"
  "LUP_LAD_CTYUA/Local_Authority_to_County_and_Unitary_Authority_(December_2024)_Lookup_in_EW.csv")

(def LAD25-CTYUA25-EQ-LU-resource-file-name
  "Name of resource file mapping 2025 Local Authority Districts in England & Wales to County and Unitary Authority.
   From: https://geoportal.statistics.gov.uk/datasets/ons::local-authority-district-to-county-and-unitary-authority-april-2025-lookup-in-ew-v2/about"
  "LUP_LAD_CTYUA/Local_Authority_District_to_County_and_Unitary_Authority_(April_2025)_Lookup_in_EW_v2.csv")

(def resource-file-name-for-year
  "Map mapping years to the corresponding resource file name."
  {2022 LAD22-CTYUA22-EW-LU-resource-file-name
   2023 LAD23-CTYUA23-EQ-LU-resource-file-name
   2024 LAD24-CTYUA24-EQ-LU-resource-file-name
   2025 LAD25-CTYUA25-EQ-LU-resource-file-name})

(defn ->dataset
  "Read mapping of Local Authority Districts in England & Wales to County and Unitary Authority from CSV file
   specified by either `file-path` or `resource-file-name` or `year` (for which resource file is looked up)
   into a dataset."
  [& {:keys [file-path
             resource-file-name
             year
             dataset-name]}]
  (let [resource-file-name (or resource-file-name
                               (get resource-file-name-for-year year))]
    (with-open [in (-> (or file-path (io/resource resource-file-name))
                       io/file
                       io/input-stream)]
      (-> (ds/->dataset in {:file-type    :csv
                            :separator    ","
                            :header-row?  true
                            :key-fn       #(-> %
                                               (str/replace #"^LTLA|UTLA" {"LTLA" "LAD", "UTLA" "CTYUA"})
                                               str/lower-case
                                               keyword)
                            :dataset-name (or dataset-name file-path resource-file-name)})))))

(comment ;; Check LADCDs in each resource-file are unique
  ;; Number of LADCDs with more than one record:
  (update-vals resource-file-name-for-year (fn [s] (as-> s $
                                                     (->dataset :resource-file-name $)
                                                     (tc/group-by $ (tc/column-names $ #"^:lad\d\dcd"))
                                                     (tc/aggregate $ {:row-count tc/row-count})
                                                     (tc/select-rows $ #(-> % :row-count (> 1)))
                                                     (tc/row-count $))))
  ;;=> {2022 0, 2023 0, 2024 0, 2025 0}
  
 :rcf)

(comment ;; EDA: Dataset structures for each resource-file
  (update-vals resource-file-name-for-year (fn [s] (as-> s $
                                                     (->dataset :resource-file-name $)
                                                     (tc/info $)
                                                     (tc/select-columns $ [:col-name :datatype :n-valid :n-missing :min :max]))))
  ;;=> {2022
  ;;    LUP_LAD_CTYUA/Lower_Tier_Local_Authority_to_Upper_Tier_Local_Authority_(December_2022)_Lookup_in_England_and_Wales.csv: descriptive-stats [5 6]:
  ;;   
  ;;   |  :col-name | :datatype | :n-valid | :n-missing | :min |  :max |
  ;;   |------------|-----------|---------:|-----------:|-----:|------:|
  ;;   |   :lad22cd |   :string |      331 |          0 |      |       |
  ;;   |   :lad22nm |   :string |      331 |          0 |      |       |
  ;;   | :ctyua22cd |   :string |      331 |          0 |      |       |
  ;;   | :ctyua22nm |   :string |      331 |          0 |      |       |
  ;;   |  :objectid |    :int16 |      331 |          0 |  1.0 | 331.0 |
  ;;   ,
  ;;    2023
  ;;    LUP_LAD_CTYUA/Local_Authority_District_to_County_and_Unitary_Authority_(April_2023)_Lookup_in_EW.csv: descriptive-stats [7 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing | :min |  :max |
  ;;   |-------------|-----------|---------:|-----------:|-----:|------:|
  ;;   |    :lad23cd |   :string |      318 |          0 |      |       |
  ;;   |    :lad23nm |   :string |      318 |          0 |      |       |
  ;;   |   :lad23nmw |   :string |       22 |        296 |      |       |
  ;;   |  :ctyua23cd |   :string |      318 |          0 |      |       |
  ;;   |  :ctyua23nm |   :string |      318 |          0 |      |       |
  ;;   | :ctyua23nmw |   :string |       22 |        296 |      |       |
  ;;   |   :objectid |    :int16 |      318 |          0 |  1.0 | 318.0 |
  ;;   ,
  ;;    2024
  ;;    LUP_LAD_CTYUA/Local_Authority_to_County_and_Unitary_Authority_(December_2024)_Lookup_in_EW.csv: descriptive-stats [7 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing | :min |  :max |
  ;;   |-------------|-----------|---------:|-----------:|-----:|------:|
  ;;   |    :lad24cd |   :string |      318 |          0 |      |       |
  ;;   |    :lad24nm |   :string |      318 |          0 |      |       |
  ;;   |   :lad24nmw |   :string |       22 |        296 |      |       |
  ;;   |  :ctyua24cd |   :string |      318 |          0 |      |       |
  ;;   |  :ctyua24nm |   :string |      318 |          0 |      |       |
  ;;   | :ctyua24nmw |   :string |       22 |        296 |      |       |
  ;;   |   :objectid |    :int16 |      318 |          0 |  1.0 | 318.0 |
  ;;   ,
  ;;    2025
  ;;    LUP_LAD_CTYUA/Local_Authority_District_to_County_and_Unitary_Authority_(April_2025)_Lookup_in_EW_v2.csv: descriptive-stats [7 6]:
  ;;   
  ;;   |   :col-name | :datatype | :n-valid | :n-missing | :min |  :max |
  ;;   |-------------|-----------|---------:|-----------:|-----:|------:|
  ;;   |    :lad25cd |   :string |      318 |          0 |      |       |
  ;;   |    :lad25nm |   :string |      318 |          0 |      |       |
  ;;   |   :lad25nmw |   :string |       22 |        296 |      |       |
  ;;   |  :ctyua25cd |   :string |      318 |          0 |      |       |
  ;;   |  :ctyua25nm |   :string |      318 |          0 |      |       |
  ;;   | :ctyua25nmw |   :string |       22 |        296 |      |       |
  ;;   |   :objectid |    :int16 |      318 |          0 |  1.0 | 318.0 |
  ;;   }

  :rcf)