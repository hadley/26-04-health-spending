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

# Extract short codes from indicator_code and drop redundant columns
extract_codes <- function(df) {
  df$unit <- ifelse(grepl("_che$", df$indicator_code), "che", "usd2023")
  df$indicator_code <- sub("_.*", "", df$indicator_code)
  df$country_name <- NULL
  df
}

health_spending <- extract_codes(health_spending)
names(health_spending)[names(health_spending) == "indicator_code"] <- "expenditure_type"

financing_schemes <- extract_codes(financing_schemes)
names(financing_schemes)[names(financing_schemes) == "indicator_code"] <- "financing_scheme"

spending_purpose <- extract_codes(spending_purpose)
names(spending_purpose)[names(spending_purpose) == "indicator_code"] <- "spending_purpose"

write_parquet(health_spending, "health_spending.parquet")
write_parquet(financing_schemes, "financing_schemes.parquet")
write_parquet(spending_purpose, "spending_purpose.parquet")
