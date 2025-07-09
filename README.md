# witan.population

[ONS Subnational population projections, for local authorities in England, 
by single year of age, rolled up to upper tier LAs](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/datasets/localauthoritiesinenglandz1), 
with derived variables and formatted as required for `witan.send` modelling use.
- 2022 based (24-JUN-2025 release): [2022snpppopulationsyoa5yr.zip](https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationprojections/datasets/localauthoritiesinenglandz1/2022basedfiveyearmigrationvariantprojection/2022snpppopulationsyoa5yr.zip)
- 2018 based (24-MAR-2020 release): [2018snpppopulation.zip](https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fpopulationandmigration%2fpopulationprojections%2fdatasets%2flocalauthoritiesinenglandz1%2f2018based/2018snpppopulation.zip)


## Usage
### 2020 based SNPPs
``` clojure
(require '[witan.population.england.snpp-2022 :as pop])
(pop/create-send-population-file! {:la-name   "Tower Hamlets"
                                   :max-year  2031 
                                   :file-name (str out-dir "population.csv")})
```

### 2018 based SNPPs (2020 release)
``` clojure
(require '[witan.population.england.snpp-2018 :as pop])
(pop/create-send-population-file! {:la-name   "Tower Hamlets"
                                   :max-year  2031 
                                   :file-name (str out-dir "population.csv")})
```

## License

Population data available under [UK Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

Code copyright © 2025 Mastodon C Ltd, distributed under Eclipse Public License 2.0 as described in LICENSE file.
