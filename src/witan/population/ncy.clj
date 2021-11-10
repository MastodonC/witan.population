(ns witan.population.ncy)

(defn ncy->age [ncy]
  (+ ncy 5))

(defn age->ncy [age]
  (- age 5))
