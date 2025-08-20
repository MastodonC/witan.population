# witan.population

[ONS Subnational population projections, for local authorities in England, 
by single year of age, rolled up to upper tier LAs](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/datasets/localauthoritiesinenglandz1), 
with derived variables and formatted as required for `witan.send` modelling use.


## Usage
### 2022 based SNPPs
``` clojure
(require '[witan.send.population.england :as pop])
(pop/create-file! "population.csv" {:la-name   "Tower Hamlets"
                                    :min-calendar-year 2022
                                    :max-calendar-year 2035})
```

### 2018 based SNPPs (2020 release)
``` clojure
(require '[witan.population.england.snpp-2018 :as pop])
(pop/create-send-population-file! {:la-name   "Tower Hamlets"
                                   :max-year  2031 
                                   :file-name "population.csv"})
```

## License

Population data available under [UK Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

Code copyright © 2025 Mastodon C Ltd, distributed under Eclipse Public License 2.0 as described in LICENSE file.
