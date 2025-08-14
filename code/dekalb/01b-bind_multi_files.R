
## Goal here is to put together one digest file per year based on all the different datasets

library(dplyr)
library(tidyr)
library(stringr)
library(here)
library(purrr)
library(readr)


###############################################################################
# 2011-12 that are in singular CSVs
###############################################################################

###############################################################################
# 2013-14 that are in .mbd files
###############################################################################

###############################################################################
# 2015-2021 that are in relational database structure
###############################################################################

# 1. Make sure that the table names are same cross years ######################

year_folders <- list.dirs(here("data/dekalb/DIGEST_SALES"), recursive = FALSE)

# data frame of table names per year
tables_by_year <- lapply(year_folders, function(folder) {
  year <- str_extract(folder, "\\d{4}")
  
  # list all files in the folder, remove extensions
  tables <- tools::file_path_sans_ext(list.files(folder))
  
  data.frame(
    year = year,
    table_name = tables,
    stringsAsFactors = FALSE
  )
}) %>% bind_rows()

# pivot so we can see which tables exist in which years
table_check <- tables_by_year %>%
  mutate(present = TRUE) %>%
  pivot_wider(names_from = year, values_from = present, values_fill = FALSE)

table_check

# 2. ID and merge datasets consistent across all class type of parcels #########


years <- 2015:2022
yr <- 2015
dekalb_nested <- map_dfr(years, function(yr) {
  folder <- year_folders[str_detect(year_folders, as.character(yr))]

  # read tables directly & select/rename relevant columns
  ## need to append 
  parcel    <- read_csv(file.path(folder, "PARCEL DATA.csv"), show_col_types = FALSE) %>% 
    janitor::clean_names()
  asmt      <- read_csv(file.path(folder, "ASMT.csv"), show_col_types = FALSE) %>% 
    janitor::clean_names()
  legal     <- read_csv(file.path(folder, "LEGAL DATA.csv"), show_col_types = FALSE) %>% 
    janitor::clean_names()
  land      <- read_csv(file.path(folder, "LAND.csv"), show_col_types = FALSE) %>% 
    janitor::clean_names()
  ownership <- read_csv(file.path(folder, "OWNERSHIP DATA.csv"), show_col_types = FALSE) %>% 
    janitor::clean_names() %>%
    rename()
  
  
  # merge by parid starting w PARCEL DATA %>% ASMT %>% LEGAL DATA %>% LAND %>% OWNERSHIP DATA
  merged_data <- parcel %>%
    left_join(asmt, by = c("parid", "taxyr")) %>%
    left_join(legal, by = c("parid", "taxyr")) %>%
    left_join(land, by = c("parid", "taxyr")) %>%
    left_join(ownership, by = c("parid", "taxyr"))
  
  # Save merged CSV
  write_csv(merged_data, file.path(folder, paste0("RAW_dekalb_digest_", yr, ".csv")))
  
  # Return nested tibble row
  tibble(
    year = yr,
    data = list(merged_data)
  )
})

# Inspect the nested tibble
dekalb_nested






# 2. ID and merge datasets consistent across all class type of parcels #########
