library(nanoparquet)
library(purrr)

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
extract_codes <- function(df, label_col) {
  parts <- strsplit(df$indicator_code, "_")
  df$indicator_code <- map_chr(parts, 1)
  df$unit <- map_chr(parts, 2)
  df$country_name <- NULL
  df[[label_col]] <- NULL
  df
}

unit_levels <- c("che", "usd2023")

health_spending <- extract_codes(health_spending, "expenditure_type")
names(health_spending)[names(health_spending) == "indicator_code"] <- "expenditure_type"
health_spending$expenditure_type <- factor(health_spending$expenditure_type, c("che", "gghed", "pvtd", "ext"))
health_spending$unit <- factor(health_spending$unit, unit_levels)

financing_schemes <- extract_codes(financing_schemes, "financing_scheme")
names(financing_schemes)[names(financing_schemes) == "indicator_code"] <- "financing_scheme"
financing_schemes$financing_scheme <- factor(financing_schemes$financing_scheme, c("hf1", "hf2", "hf3", "hf4", "hfnec"))
financing_schemes$unit <- factor(financing_schemes$unit, unit_levels)

spending_purpose <- extract_codes(spending_purpose, "spending_purpose")
names(spending_purpose)[names(spending_purpose) == "indicator_code"] <- "spending_purpose"
spending_purpose$spending_purpose <- factor(spending_purpose$spending_purpose, c("hc1", "hc2", "hc3", "hc4", "hc5", "hc6", "hc7", "hc9"))
spending_purpose$unit <- factor(spending_purpose$unit, unit_levels)

write_parquet(health_spending, "health_spending.parquet")
write_parquet(financing_schemes, "financing_schemes.parquet")
write_parquet(spending_purpose, "spending_purpose.parquet")
