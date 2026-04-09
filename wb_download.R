library(jsonlite)
library(dplyr)
library(nanoparquet)

dir.create("wb", showWarnings = FALSE)

wb_download <- function(url, path) {
  path <- file.path("wb", path)
  if (!file.exists(path)) {
    download.file(url, path)
  }
  fromJSON(path)[[2]] |> as_tibble()
}

# Country metadata (region, income group, etc.)
countries_raw <- wb_download(
  "https://api.worldbank.org/v2/country/all?format=json&per_page=500",
  "wb_countries.json"
)

income_levels <- c("Low income", "Lower middle income", "Upper middle income", "High income")

countries <- countries_raw |>
  transmute(
    iso3_code = id,
    country_name = name,
    region = region$value,
    income_group = factor(incomeLevel$value, levels = income_levels)
  ) |>
  filter(region != "Aggregates") |>
  arrange(iso3_code)

# Population and GDP
wb_fetch <- function(indicator, path) {
  wb_download(
    paste0(
      "https://api.worldbank.org/v2/country/all/indicator/", indicator,
      "?date=2000:2023&format=json&per_page=20000"
    ),
    path
  ) |>
    transmute(
      iso3_code = countryiso3code,
      year = as.integer(date),
      value
    ) |>
    filter(!is.na(value), iso3_code != "")
}

pop <- wb_fetch("SP.POP.TOTL", "wb_pop.json") |> rename(population = value)
gdp <- wb_fetch("NY.GDP.MKTP.CD", "wb_gdp.json") |> rename(gdp_usd = value)

wb <- pop |>
  full_join(gdp, by = c("iso3_code", "year")) |>
  semi_join(countries, by = "iso3_code") |>
  arrange(iso3_code, year)

write_parquet(countries, "country.parquet")
write_parquet(wb, "country-year.parquet")
