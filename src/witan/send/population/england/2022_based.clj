(ns witan.send.population.england.2022-based
  (:require [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]
            [witan.population.england-wales.mye :as mye]
            [witan.population.england.snpp-2022 :as snpp-2022]
            [witan.population.england.2022-based :as ons-pop-2022-based]))

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
  [x]
  (- x 4))

(defn ncy->age
  "Given NCY, return nominal age at the start of the school year."
  [x]
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
  [x]
  (inc x))

(defn census-year->year
  "Given a SEN2 census year, returns the ONS estimate/projection year of the corresponding populaton."
  [x]
  (dec x))

(def default-options
  (merge (get snpp-2022/resource-options "2022snpppopulationsyoamigcat23-persons")
         (get mye/resource-options       "myebtablesenglandwales20112024")
         {:min-academic-year -4
          :max-academic-year 20}))

;; TODO: function to add witan.send `:academic-year` and `:calendar-year` to an 
;;       ONS population dataset and apply any requested filters.
;; DEV framework below:
(-> default-options
    (merge {:ctyuanm-f #{"Surrey"}
            :max-age   30
            :min-year  2019
            :max-year  2026})
    ons-pop-2022-based/->dataset
    ((fn ;; ons-pop-ds->witan-send-pop-ds
       [ds & {:keys [;; Add `la-name` to facilitate filtering for a single CTYUANM?
                     min-academic-year max-academic-year
                     min-calendar-year max-calendar-year]
              :as   options}]
       (let []
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
           )
         )
       )
     (-> default-options
         (merge {}))
     )
    #_(-> tc/info (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
    )

;; TODO: `->dataset` function to get an ONS pop dataset (via `ons-pop-2022-based/->dataset`),
;;       and process for witan.send use (using `ons-pop-ds->witan-send-pop-ds` above).
;; DEV framework below
(-> default-options
    (merge {:min-calendar-year 2022
            :max-calendar-year 2026})
    ((fn ;;->dataset
       [& {:keys [;; Add `la-name` to facilitate filtering for a single CTYUANM?
                  min-academic-year
                  max-academic-year
                  min-calendar-year
                  max-calendar-year
                  switch-calendar-year]
           :as options}]
       (let [options (-> options
                         ;; NOTE: need to be careful with defaults (for sheet & geography-year-yy) if MYE or SNPP files are specified.
                         ;; NOTE: Deliberately over-writing any [min-age max-age min-year max-year] here, as this is a witan.send.pop ns
                         (merge {:min-age     (some-> min-academic-year ncy->age)
                                 :max-age     (some-> max-academic-year ncy->age)
                                 :min-year    (some-> min-calendar-year census-year->year)
                                 :max-year    (some-> max-calendar-year census-year->year)
                                 :switch-year (some-> switch-calendar-year census-year->year)
                                 })
                         )]
         {:options options}
         )
       )
     )
    )

(def output-columns
  "Output columns for `witan.send` population.csv file."
  [:calendar-year :academic-year :population])

(defn write!
  "Writes required output columns from witan.send.population dataset `ds` to CSV file `file-name`."
  [ds file-name]
  (-> ds
      (tc/select-columns output-columns)
      (tc/write! file-name)))

;; TODO: `create-file!` function to create and write a witan.send population file to CSV file