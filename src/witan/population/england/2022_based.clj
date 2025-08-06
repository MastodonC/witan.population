(ns witan.population.england.2022-based
  "Functions to combine ONS mid-year population estimates with 2022 based 
   subnational population projections to give a population dataset spanning 
   the recent past and near future."
  (:require [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]
            [witan.population.england-wales.mye :as mye]
            [witan.population.england.snpp-2022 :as snpp-2022]))

(defn splice-mye-snpp
  "Splices MYE estiamtes from `mye-ds` with SNPPs from `snpp-ds`.
   Switches from MYEs to SNPPs at minimum `:year` of `snpp-ds` 
   unless value for `:switch-year` option is specified."
  [mye-ds snpp-ds & {:keys [switch-year]}]
  (let [switch-year (or switch-year
                        (-> snpp-ds :year tcc/reduce-min))]
    (-> (tc/concat-copying
         (when (< 0 (tc/row-count mye-ds))
           (-> mye-ds
               (tc/drop-columns [:country])
               (tc/select-rows #(-> % :year (< switch-year)))))
         (when (< 0 (tc/row-count snpp-ds))
           (-> snpp-ds
               (tc/drop-missing [:age])
               (tc/drop-columns [:age-group])
               (tc/select-rows #(-> % :year (>= switch-year))))))
        (as-> $ (tc/order-by $ (tc/column-names $)))
        (tc/set-dataset-name (format "%s and %s spliced at switch-year of %d"
                                     (tc/dataset-name mye-ds)
                                     (tc/dataset-name snpp-ds)
                                     switch-year)))))

(comment ;; Example: LAD level concatenation of MYE & SNPP
  (let [options {:ladcd-f     #{"E06000005"}
                 :min-age     10
                 :max-age     10
                 :min-year    2021
                 :switch-year 2023
                 :max-year    2025}
        mye-ds  (-> options
                    (merge {})
                    mye/->dataset-by-lad)
        snpp-ds (-> options
                    (merge {})
                    snpp-2022/->dataset-by-lad)]
    {:mye-ds    mye-ds
     :snpp-ds   snpp-ds
     :concat-ds (splice-mye-snpp mye-ds
                                 snpp-ds
                                 options)})
  ;;=> {:mye-ds [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: long by LAD (persons) [4 9]:
  ;;   
  ;;   |  :lad23cd |   :lad23nm | :ctyua23cd | :ctyua23nm | :country | :year | :age |    :sex | :population |
  ;;   |-----------|------------|------------|------------|----------|------:|-----:|---------|------------:|
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |        E |  2021 |   10 | persons |      1320.0 |
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |        E |  2022 |   10 | persons |      1353.0 |
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |        E |  2023 |   10 | persons |      1347.0 |
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |        E |  2024 |   10 | persons |      1336.0 |
  ;;   , :snpp-ds 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: long by LAD [4 10]:
  ;;   
  ;;   |  :lad23cd |   :lad23nm | :ctyua23cd | :ctyua23nm | :year | :age-group | :age | :component |    :sex | :population |
  ;;   |-----------|------------|------------|------------|------:|------------|-----:|------------|---------|------------:|
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |  2022 |         10 |   10 | Population | persons |    1357.000 |
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |  2023 |         10 |   10 | Population | persons |    1314.757 |
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |  2024 |         10 |   10 | Population | persons |    1276.514 |
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |  2025 |         10 |   10 | Population | persons |    1253.260 |
  ;;   ,
  ;;    :concat-ds
  ;;    [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: long by LAD (persons) and 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: long by LAD spliced at switch-year of 2023 [5 9]:
  ;;   
  ;;   |  :lad23cd |   :lad23nm | :ctyua23cd | :ctyua23nm | :year | :age |    :sex | :population | :component |
  ;;   |-----------|------------|------------|------------|------:|-----:|---------|------------:|------------|
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |  2021 |   10 | persons |    1320.000 |            |
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |  2022 |   10 | persons |    1353.000 |            |
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |  2023 |   10 | persons |    1314.757 | Population |
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |  2024 |   10 | persons |    1276.514 | Population |
  ;;   | E06000005 | Darlington |  E06000005 | Darlington |  2025 |   10 | persons |    1253.260 | Population |
  ;;   }
    
  :rcf)

(defn ->dataset
  "Gets MYE and SNPP estiamtes according to options and splices together 
   at minimum `:year` of `snpp-ds` unless value for `:switch-year` option is specified.
   MYEs & SNPPs are obtained via functions specified by `::mye-f` & `::snpp-f` 
   option values (if min/switch/max year require them), and are passed the options map."
  [& {::keys [snpp-f mye-f]
      :keys [min-year switch-year max-year]
      :or {snpp-f snpp-2022/->dataset-by-ctyua
           mye-f  mye/->dataset-by-ctyua}
      :as options}]
  (let [snpp-ds     (when (or (nil? switch-year)
                              (nil? max-year)
                              (<= switch-year max-year))
                      (-> options
                          (merge (when switch-year {:min-year switch-year}))
                          snpp-f))
        switch-year (or switch-year
                        (some-> snpp-ds :year tcc/reduce-min))
        mye-ds      (when (or (nil? min-year)
                              (nil? switch-year)
                              (< min-year switch-year))
                      (-> options
                          (merge {:max-year (dec switch-year)})
                          mye-f))]
    (splice-mye-snpp mye-ds
                     snpp-ds
                     (assoc options :switch-year switch-year))))

(comment ;; Example: CTYUA level for Surrey 10 year olds from 2020 to 2025 with default switch-year
  (-> {:ctyuanm-f #{"Surrey"}
       :min-age   10
       :max-age   10
       :min-year  2020
       :max-year  2025}
      ->dataset)
  ;;=> [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: long by CTYUA (persons) and 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: long by CTYUA spliced at switch-year of 2022 [6 7]:
  ;;   
  ;;   | :ctyua23cd | :ctyua23nm | :year | :age |    :sex | :population | :component |
  ;;   |------------|------------|------:|-----:|---------|------------:|------------|
  ;;   |  E10000030 |     Surrey |  2020 |   10 | persons |   15240.000 |            |
  ;;   |  E10000030 |     Surrey |  2021 |   10 | persons |   15592.000 |            |
  ;;   |  E10000030 |     Surrey |  2022 |   10 | persons |   15842.000 | Population |
  ;;   |  E10000030 |     Surrey |  2023 |   10 | persons |   15376.017 | Population |
  ;;   |  E10000030 |     Surrey |  2024 |   10 | persons |   15173.479 | Population |
  ;;   |  E10000030 |     Surrey |  2025 |   10 | persons |   15211.734 | Population |
  ;;   

  :rcf)

(defn ->dataset-by-ctyua
  [& {:as options}]
  (->dataset (merge {::snpp-f snpp-2022/->dataset-by-ctyua
                     ::mye-f  mye/->dataset-by-ctyua}
                    options)))

(defn ->dataset-by-lad
  [& {:as options}]
  (->dataset (merge {::snpp-f snpp-2022/->dataset-by-lad
                     ::mye-f  mye/->dataset-by-lad}
                    options)))

(comment ;; Example: LAD level for Surrey 10 year olds from 2020 to 2025 with default switch-year
  (-> {:ctyuanm-f #{"Surrey"}
       :min-age   10
       :max-age   10
       :min-year  2020
       :max-year  2025}
      ->dataset-by-lad)
  ;;=> [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: long by LAD (persons) and 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: long by LAD spliced at switch-year of 2022 [66 9]:
  ;;   
  ;;   |  :lad23cd |        :lad23nm | :ctyua23cd | :ctyua23nm | :year | :age |    :sex | :population | :component |
  ;;   |-----------|-----------------|------------|------------|------:|-----:|---------|------------:|------------|
  ;;   | E07000207 |       Elmbridge |  E10000030 |     Surrey |  2020 |   10 | persons |    2090.000 |            |
  ;;   | E07000207 |       Elmbridge |  E10000030 |     Surrey |  2021 |   10 | persons |    2131.000 |            |
  ;;   | E07000207 |       Elmbridge |  E10000030 |     Surrey |  2022 |   10 | persons |    2127.000 | Population |
  ;;   | E07000207 |       Elmbridge |  E10000030 |     Surrey |  2023 |   10 | persons |    1999.438 | Population |
  ;;   | E07000207 |       Elmbridge |  E10000030 |     Surrey |  2024 |   10 | persons |    2038.976 | Population |
  ;;   | E07000207 |       Elmbridge |  E10000030 |     Surrey |  2025 |   10 | persons |    1905.430 | Population |
  ;;   | E07000208 | Epsom and Ewell |  E10000030 |     Surrey |  2020 |   10 | persons |    1050.000 |            |
  ;;   | E07000208 | Epsom and Ewell |  E10000030 |     Surrey |  2021 |   10 | persons |    1082.000 |            |
  ;;   | E07000208 | Epsom and Ewell |  E10000030 |     Surrey |  2022 |   10 | persons |    1134.000 | Population |
  ;;   | E07000208 | Epsom and Ewell |  E10000030 |     Surrey |  2023 |   10 | persons |    1158.708 | Population |
  ;;   |       ... |             ... |        ... |        ... |   ... |  ... |     ... |         ... |        ... |
  ;;   | E07000216 |        Waverley |  E10000030 |     Surrey |  2021 |   10 | persons |    1646.000 |            |
  ;;   | E07000216 |        Waverley |  E10000030 |     Surrey |  2022 |   10 | persons |    1767.000 | Population |
  ;;   | E07000216 |        Waverley |  E10000030 |     Surrey |  2023 |   10 | persons |    1739.367 | Population |
  ;;   | E07000216 |        Waverley |  E10000030 |     Surrey |  2024 |   10 | persons |    1655.795 | Population |
  ;;   | E07000216 |        Waverley |  E10000030 |     Surrey |  2025 |   10 | persons |    1653.130 | Population |
  ;;   | E07000217 |          Woking |  E10000030 |     Surrey |  2020 |   10 | persons |    1409.000 |            |
  ;;   | E07000217 |          Woking |  E10000030 |     Surrey |  2021 |   10 | persons |    1364.000 |            |
  ;;   | E07000217 |          Woking |  E10000030 |     Surrey |  2022 |   10 | persons |    1384.000 | Population |
  ;;   | E07000217 |          Woking |  E10000030 |     Surrey |  2023 |   10 | persons |    1315.137 | Population |
  ;;   | E07000217 |          Woking |  E10000030 |     Surrey |  2024 |   10 | persons |    1287.245 | Population |
  ;;   | E07000217 |          Woking |  E10000030 |     Surrey |  2025 |   10 | persons |    1299.864 | Population |
  ;;   

  :rcf)
