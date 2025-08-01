(ns witan.population.ew.lad-to-ctyua
  "Lookups mapping Local Authority Districts in England & Wales to County and Unitary Authority.
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
   specified by either `file-path` or `resource-file-name` or `year` (for which resource file is looked up)."
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
                                               keyword)
                            :dataset-name (or dataset-name file-path resource-file-name)})))))

(comment ;; Check LADCDs in each resource-file are unique
  ;; Number of LADCDs with more than one record:
  (update-vals resource-file-name-for-year (fn [s] (as-> s $
                                                     (->dataset :resource-file-name $)
                                                     (tc/group-by $ (tc/column-names $ #"^:LAD\d\dCD"))
                                                     (tc/aggregate $ {:row-count tc/row-count})
                                                     (tc/select-rows $ #(-> % :row-count (> 1)))
                                                     (tc/row-count $))))
  ;;=> {2022 0, 2023 0, 2024 0, 2025 0}
  
 :rcf)