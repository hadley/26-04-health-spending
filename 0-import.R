library(nanoparquet)

financing_schemes <- read.csv("raw-data/financing_schemes.csv")
health_spending <- read.csv("raw-data/health_spending.csv")
spending_purpose <- read.csv("raw-data/spending_purpose.csv")

# Create country lookup table
countries <- unique(rbind(
  health_spending[, c("iso3_code", "country_name")],
  financing_schemes[, c("iso3_code", "country_name")],
  spending_purpose[, c("iso3_code", "country_name")]
))
countries <- countries[order(countries$country_name), ]
write_parquet(countries, "countries.parquet")

# Drop country_name from main tables
health_spending$country_name <- NULL
financing_schemes$country_name <- NULL
spending_purpose$country_name <- NULL

write_parquet(financing_schemes, "financing_schemes.parquet")
write_parquet(health_spending, "health_spending.parquet")
write_parquet(spending_purpose, "spending_purpose.parquet")
