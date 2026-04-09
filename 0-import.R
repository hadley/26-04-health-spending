library(nanoparquet)

financing_schemes <- read.csv("raw-data/financing_schemes.csv")
health_spending <- read.csv("raw-data/health_spending.csv")
spending_purpose <- read.csv("raw-data/spending_purpose.csv")

write_parquet(financing_schemes, "financing_schemes.parquet")
write_parquet(health_spending, "health_spending.parquet")
write_parquet(spending_purpose, "spending_purpose.parquet")
