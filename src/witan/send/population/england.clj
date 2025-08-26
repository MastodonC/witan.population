(ns witan.send.population.england
  "Functions to add `witan.send` `:calendar-year` and `:academic-year` to ONS
   population estimates for English County & Unitary Authorities 
   and write `witan.send` population files."
  (:require [tablecloth.api :as tc]
            [witan.population.england :as ons-pop]))

(def default-options
  {::ons-pop-f        ons-pop/->dataset
   :min-academic-year -4
   :max-academic-year 20})

(defn age->ncy
  "Given age, return nominal NCY.
   The ONS population estimates/projections are mid-year estiamtes, 
   so the age is (almost) the age on 31st August, i.e. at the start of the 
   school year. Per https://www.gov.uk/national-curriculum, children aged 5 at 
   the start of the school year should be in NCY 1. Thus the offset between age 
   at the start of the school year and NCY is -4.
   Note that the implementation here is not restricted to valid NCYs."
  ;; Note that compared to `witan.population.england.snpp-2018/snpp-2018->witan-send-population`
  ;; `:academic-year` differs by +1, due to consideration here of SNPPs as mid-year estimates.
  [^long x]
  (- x 4))

(defn ncy->age
  "Given NCY, return nominal age at the start of the school year."
  [^long x]
  (+ x 4))

(defn year->census-year
  "Given the year of an ONS estimate/projection, returns the year of the SEN2 census
   that populaton would be reported in: The ONS population estimates/projections 
   are mid-year estiamtes, so that population would be reported in the SEN2 
   census the following January, so the census-year is the year after the 
   estimate year, i.e. an offset of +1."
  ;; Note that compared to `witan.population.england.snpp-2018/snpp-2018->witan-send-population`
  ;; `:census-year` here differs from the `:calendar-year`s there by +1, 
  ;; due to consideration here of SNPPs as mid-year estimates.
  [^long x]
  (inc x))

(defn census-year->year
  "Given a SEN2 census year, returns the ONS estimate/projection year of the corresponding populaton."
  [^long x]
  (dec x))

(defn ons-pop-ds->witan-send-pop-ds
  "Given ONS population dataset `ds` by `:year` & `:age`, add witan.send `:calendar-year` & `:academic-year`.
   Dataset can optionally be filtered as follows:
   - For (integer) academic-years, 
     by specifying values for keys `:min-academic-year` and/or `:max-academic-year`.
   - For calendar-years, 
     by specifying values for keys `:min-calendar-year` and/or `:max-calendar-year`."
  [ds & {:keys [min-academic-year max-academic-year
                min-calendar-year max-calendar-year]}]
  (as-> ds $
    ;; Derive `:calendar-year`
    (tc/map-columns $ :calendar-year :int16 [:year] year->census-year)
    (tc/reorder-columns $ (concat (take-while (complement #{:year}) (tc/column-names $))
                                  [:year :calendar-year]))
    ;; Select `:calendar-year`s (if specified)
    (cond-> $
      min-calendar-year (tc/select-rows #(-> % :calendar-year (>= min-calendar-year)))
      max-calendar-year (tc/select-rows #(-> % :calendar-year (<= max-calendar-year))))
    ;; Derive `:academic-year`
    (tc/map-columns $ :academic-year :int8 [:age] age->ncy)
    (tc/reorder-columns $ (concat (take-while (complement #{:age}) (tc/column-names $))
                                  [:age :academic-year]))
    ;; Select required `:academic-year`s (if specified)
    (cond-> $
      min-academic-year (tc/select-rows #(-> % :academic-year (>= min-academic-year)))
      max-academic-year (tc/select-rows #(-> % :academic-year (<= max-academic-year))))
    ;; Arrange dataset
    (tc/set-dataset-name $ (format "witan.population from %s" (tc/dataset-name ds)))))

(comment ;; Example: Surrey NCY 7 from 2021 to 2026 from default `ons-pop` ns population dataset
  (let [pop-ds       (-> {:ctyuanm-f #{"Surrey"}
                          :min-age   (ncy->age 6)
                          :max-age   (ncy->age 8)
                          :min-year  (census-year->year 2020)
                          :max-year  (census-year->year 2027)}
                         ons-pop/->dataset)
        witan-pop-ds (->> {:min-calendar-year 2021
                           :max-calendar-year 2026
                           :min-academic-year 7
                           :max-academic-year 7}
                          (ons-pop-ds->witan-send-pop-ds pop-ds))]
    {:pop-ds       pop-ds
     :witan-pop-ds witan-pop-ds})
  ;;=> {:pop-ds
  ;;    [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: long by CTYUA (persons) and 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: long by CTYUA concatenated at min-snpp-year of 2022 [24 6]:
  ;;   
  ;;   | :ctyua23cd | :ctyua23nm | :year | :age |    :sex | :population |
  ;;   |------------|------------|------:|-----:|---------|------------:|
  ;;   |  E10000030 |     Surrey |  2019 |   10 | persons |   14984.000 |
  ;;   |  E10000030 |     Surrey |  2019 |   11 | persons |   15286.000 |
  ;;   |  E10000030 |     Surrey |  2019 |   12 | persons |   14899.000 |
  ;;   |  E10000030 |     Surrey |  2020 |   10 | persons |   15240.000 |
  ;;   |  E10000030 |     Surrey |  2020 |   11 | persons |   15052.000 |
  ;;   |  E10000030 |     Surrey |  2020 |   12 | persons |   15392.000 |
  ;;   |  E10000030 |     Surrey |  2021 |   10 | persons |   15592.000 |
  ;;   |  E10000030 |     Surrey |  2021 |   11 | persons |   15323.000 |
  ;;   |  E10000030 |     Surrey |  2021 |   12 | persons |   15234.000 |
  ;;   |  E10000030 |     Surrey |  2022 |   10 | persons |   15842.000 |
  ;;   |        ... |        ... |   ... |  ... |     ... |         ... |
  ;;   |  E10000030 |     Surrey |  2023 |   11 | persons |   16033.011 |
  ;;   |  E10000030 |     Surrey |  2023 |   12 | persons |   16070.314 |
  ;;   |  E10000030 |     Surrey |  2024 |   10 | persons |   15173.479 |
  ;;   |  E10000030 |     Surrey |  2024 |   11 | persons |   15533.584 |
  ;;   |  E10000030 |     Surrey |  2024 |   12 | persons |   16190.217 |
  ;;   |  E10000030 |     Surrey |  2025 |   10 | persons |   15211.734 |
  ;;   |  E10000030 |     Surrey |  2025 |   11 | persons |   15271.068 |
  ;;   |  E10000030 |     Surrey |  2025 |   12 | persons |   15633.281 |
  ;;   |  E10000030 |     Surrey |  2026 |   10 | persons |   15301.142 |
  ;;   |  E10000030 |     Surrey |  2026 |   11 | persons |   15297.229 |
  ;;   |  E10000030 |     Surrey |  2026 |   12 | persons |   15352.579 |
  ;;   ,
  ;;    :witan-pop-ds
  ;;    witan.population from [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: long by CTYUA (persons) and 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: long by CTYUA concatenated at min-snpp-year of 2022 [6 8]:
  ;;   
  ;;   | :ctyua23cd | :ctyua23nm | :year | :calendar-year | :age | :academic-year |    :sex | :population |
  ;;   |------------|------------|------:|---------------:|-----:|---------------:|---------|------------:|
  ;;   |  E10000030 |     Surrey |  2020 |           2021 |   11 |              7 | persons |   15052.000 |
  ;;   |  E10000030 |     Surrey |  2021 |           2022 |   11 |              7 | persons |   15323.000 |
  ;;   |  E10000030 |     Surrey |  2022 |           2023 |   11 |              7 | persons |   15885.000 |
  ;;   |  E10000030 |     Surrey |  2023 |           2024 |   11 |              7 | persons |   16033.011 |
  ;;   |  E10000030 |     Surrey |  2024 |           2025 |   11 |              7 | persons |   15533.584 |
  ;;   |  E10000030 |     Surrey |  2025 |           2026 |   11 |              7 | persons |   15271.068 |
  ;;   }
  
  :rcf)

(defn ->dataset
  "Get ONS population dataset according to `options` with witan.send `:calendar-year` & `:academic-year`.
   Note: Options for `academic-year` & `calendar-year` will take precedence over coresponding options for ONS population `year` & `age`."
  [& {:as options}]
  (let [{::keys [ons-pop-f]
         :keys [min-academic-year
                max-academic-year
                min-calendar-year
                min-snpp-calendar-year
                max-calendar-year]
         :as options-with-defaults} (merge default-options options)
        ons-pop-options (merge options-with-defaults
                               (when min-academic-year      {:min-age       (-> min-academic-year      ncy->age)})
                               (when max-academic-year      {:max-age       (-> max-academic-year      ncy->age)})
                               (when min-calendar-year      {:min-year      (-> min-calendar-year      census-year->year)})
                               (when min-snpp-calendar-year {:min-snpp-year (-> min-snpp-calendar-year census-year->year)})
                               (when max-calendar-year      {:max-year      (-> max-calendar-year      census-year->year)}))
        ons-pop-ds      (ons-pop-f ons-pop-options)]
    (ons-pop-ds->witan-send-pop-ds ons-pop-ds options-with-defaults)))

(comment ;; Example: Surrey NCY 7 for `calendar-year`s 2021 to 2026 using default ONS population function
  (->dataset {:ctyuanm-f              #{"Surrey"}
              :min-academic-year      7
              :max-academic-year      7
              :min-calendar-year      2021
              :max-calendar-year      2026})
  ;;=> witan.population from [myebtablesenglandwales/myebtablesenglandwales20112024.xlsx]MYEB1: long by CTYUA (persons) and 2022snpppopulationsyoa/2022snpppopulationsyoamigcat23/2022 SNPP Population persons.csv: long by CTYUA concatenated at min-snpp-year of 2022 [6 8]:
  ;;   
  ;;   | :ctyua23cd | :ctyua23nm | :year | :calendar-year | :age | :academic-year |    :sex | :population |
  ;;   |------------|------------|------:|---------------:|-----:|---------------:|---------|------------:|
  ;;   |  E10000030 |     Surrey |  2020 |           2021 |   11 |              7 | persons |   15052.000 |
  ;;   |  E10000030 |     Surrey |  2021 |           2022 |   11 |              7 | persons |   15323.000 |
  ;;   |  E10000030 |     Surrey |  2022 |           2023 |   11 |              7 | persons |   15885.000 |
  ;;   |  E10000030 |     Surrey |  2023 |           2024 |   11 |              7 | persons |   16033.011 |
  ;;   |  E10000030 |     Surrey |  2024 |           2025 |   11 |              7 | persons |   15533.584 |
  ;;   |  E10000030 |     Surrey |  2025 |           2026 |   11 |              7 | persons |   15271.068 |
  ;;   

  :rcf)

(def output-columns
  "Output columns for `witan.send` population.csv file."
  [:calendar-year :academic-year :population])

(defn write!
  "Writes required output columns from witan.send.population dataset `ds` to CSV file `file-name`."
  [ds file-name]
  (-> ds
      (tc/select-columns output-columns)
      (tc/write! file-name)))

(defn create-file!
  "Create witan.send population file at `file-path` according to options.
   For backwards compatibility, for a single county/unitary, the `la-name` option 
   may be specified (and will be used in precedence to any `ctyuanm-f` specified)."
  [file-path & {:keys [la-name]
                :as options}]
  (-> options
      (merge (when la-name {:ctyuanm-f #{la-name}}))
      ->dataset
      (write! file-path)))

(comment ;; Examples using `create-file!`
  ;; Surrey population for SEND ages for `calendar-year`s 2020 to 2035
  ;; using default MYEs & SNPPs
  (create-file! "surrey-population.csv"
                {:la-name "Surrey"
                 :min-calendar-year 2023
                 :max-calendar-year 2035})
  
  ;; Thurrock population for SEND ages for `calendar-year`s 2023 to 2035 
  ;; using 2022 based five-year migration variant projection edition SNPPs
  (create-file! "thurrock-population.csv"
                (merge (get witan.population.england.snpp-2022/resource-options
                            "2022snpppopulationsyoa5yr-persons")
                       {:la-name "Thurrock"
                        :min-calendar-year 2023
                        :max-calendar-year 2035}))

  ;; Dorset population for NCYs -3 to 20 for `calendar-year`s 2021 to 2035 
  ;; using 2023 MYEs (which are on 2023 LA geographies)
  ;; and   2022 based five-year migration variant projection edition SNPPs, 
  ;; considering the latter as if on 2023 LA geographies (rather than 2022),
  ;; noting that Dorset is a unitary authority in both 2022 & 2023 so there is 
  ;; no issue mixing geography years.
  (create-file! "dorset-population.csv"
                (-> (merge (get witan.population.england-wales.mye/resource-options
                                "myebtablesenglandwales20112023")
                           (get witan.population.england.snpp-2022/resource-options
                                "2022snpppopulationsyoa5yr-persons")
                           {:witan.population.england.snpp-2022/geography-year-yy "23"}
                           {:la-name "Dorset"
                            :min-academic-year -3
                            :max-academic-year 20
                            :min-calendar-year 2021
                            :max-calendar-year 2035})))

  :rcf)