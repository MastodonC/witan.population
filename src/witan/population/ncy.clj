(ns witan.population.ncy)

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn ncy->age [^long ncy]
  (+ ncy 5))

(defn age->ncy [^long age]
  (- age 5))
