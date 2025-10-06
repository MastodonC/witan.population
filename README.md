# witan.population

ONS Subnational population estimates & projections, for local authority districts and rolled up to county/unitary authority level, with derived variables and formatted as required for `witan.send` modelling use.


## 2022 based SNPPs

### Basic Usage for `witan.send` Modelling

To create population files suitable for `witan.send` modelling:

``` clojure
(require '[witan.population.england.send :as pop])
(pop/create-file! "population.csv" {:la-name   "Tower Hamlets"
                                    :min-calendar-year 2022
                                    :max-calendar-year 2035})
```

By default this uses the migration category variant edition (on 2023 geographies) of the [ONS 2022-based Subnational population projections](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/datasets/localauthoritiesinenglandz1) (SNPP), augmented with the 2024 release of the  [ONS Subnational Mid-Year Population Estimates](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales)  (MYE) for `:calendar-year`s prior to 2022.

Other population files may be specified: see documentation and examples in `witan.population.england.send`.

### Population estimates

The ONS 2022-based SNPPs and MYEs are accessible via the following supporting namespaces:
- MYEs: `witan.population.england-wales.mye`
- 2022-based SNPPs: `witan.population.england.snpp-2022`
- MYEs & SNPPs combined: `witan.population.england`

See the documentation and examples there for details.

### Geographies

Namespace `witan.population.lookups.lad-to-ctyua` provides lookups mappinglocal authority areas to county/unitary authorities for various geography years.


## 2018 based SNPPs (2020 release)

``` clojure
(require '[witan.population.england.snpp-2018 :as pop])
(pop/create-send-population-file! {:la-name   "Tower Hamlets"
                                   :max-year  2031 
                                   :file-name "population.csv"})
```

Note that the legacy code for the 2018 based SNPPs mapped age to National Curriculum Year (NCY) by subtracting 5 rather than the 4 used elsewhere.


## License

Population data available under [UK Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

Code copyright © 2025 Mastodon C Ltd, distributed under Eclipse Public License 2.0 as described in LICENSE file.
