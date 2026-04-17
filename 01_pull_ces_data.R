library(tidyverse)
library(tidyusmacro)
library(lubridate)

ces <- getBLSFiles("ces", "your_email@gmail.com")

# data_type <chr>_code data_type_text
# 10             WOMEN EMPLOYEES, THOUSANDS
ces %>% filter(date == "2025-01-01") %>% select(data_type_code, data_type_text)

# Most data is delayed one month, like Diffusion Index. So we want one month back.
ces %>%
  filter(year(date) == 2026, seasonal == "S") %>%
  filter(data_type_code == "10") %>%
  group_by(date, industry_display_level) %>%
  summarize(n = n())

ces %>%
  filter(year(date) == 2026, seasonal == "S") %>%
  filter(data_type_code == "10") %>%
  filter(industry_code %in% cesDiffusionIndex$industry_code) %>%
  group_by(date) %>%
  summarize(n = n())


ces_women <- ces %>%
  filter(
    period == "M13"
  ) %>%
  filter(data_type_code %in% c("01", "10")) %>%
  select(
    year,
    industry_name,
    data_type_code,
    value,
    supersector_name,
    industry_display_level
  ) %>%
  pivot_wider(names_from = data_type_code, values_from = value) %>%
  rename(total = `01`, women = `10`) %>%
  mutate(
    pct_women = (women / total),
    diffusion_index = industry_name %in% cesDiffusionIndex$industry_title
  )

write_rds(ces_women, "data/ces_women.rds")
