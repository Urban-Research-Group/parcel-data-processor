
## Goal here is to put together one digest file per year based on all the different datasets

library(tidyverse)
library(here)
library(readr)
library(janitor)

###############################################################################
# 2011-12 that are in singular CSVs
###############################################################################

# 1. Read in the data ######################
dk_2011 <- read_csv("data/dekalb/DIGEST_SALES/DeKalb County Parcel File 2011 - 10-03-2011.csv",
                    col_types = cols(.default = "c")) %>%
  clean_names()

dk_2012 <- read_csv("data/dekalb/DIGEST_SALES/DeKalb County Parcel File 2012 - 07-27-2012.csv",
                    col_types = cols(.default = "c")) %>%
  clean_names()

dekalb_11_12 <- tibble(
  year = 2011:2012,
  data = list(dk_2011, dk_2012)
  )

# 2. Check column consistency ######################

## get col names from each file
column_names_list <- dekalb_11_12 %>%
  mutate(cols = map(data, names)) %>%
  select(year, cols)

## get diff, similar column names
common_columns <- Reduce(intersect, column_names_list$cols)
all_columns <- Reduce(union, column_names_list$cols)
different_columns <- setdiff(all_columns, common_columns)

## create a table to view in which files certain columns differ 
column_presence <- map_dfr(different_columns, function(col) {
  tibble(
    column = col,
    year = column_names_list$year,
    present = map_lgl(column_names_list$cols, ~ col %in% .)
  )
}) %>%
  pivot_wider(names_from = year, values_from = present, values_fill = list(present = FALSE)) %>%
  arrange(column)

column_presence

## date_notice_mailed, leaid, reasoncode are new starting 2012 -- its okay to leave this here
## we can now bind

dekalb_11_12 <- dekalb_11_12 %>%
  select(data) %>%
  unnest(data)

# 3. Save merged CSV ######################
path <- here::here("output/dekalb/2-processed_clean/RAW_dekalb_digest_2011_2012.csv")
write_csv(dekalb_11_12, path)

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
dekalb_nested <- map_dfr(years, function(yr) {
  folder <- year_folders[str_detect(year_folders, as.character(yr))]

  # read tables directly & select/rename relevant columns
  ## need to append 
  parcel <- read_csv(file.path(folder, "PARCEL DATA.csv"), col_types = cols(.default = "c"), show_col_types = FALSE) %>% 
    janitor::clean_names() %>%
    rename_with(~ paste0("par_", .x), -c(parid, taxyr))
  asmt <- read_csv(file.path(folder, "ASMT.csv"), col_types = cols(.default = "c"), show_col_types = FALSE) %>% 
    janitor::clean_names() %>%
    rename_with(~ paste0("asmt_", .x), -c(parid, taxyr))
  legal <- read_csv(file.path(folder, "LEGAL DATA.csv"), col_types = cols(.default = "c"), show_col_types = FALSE) %>% 
    janitor::clean_names() %>%
    rename_with(~ paste0("legal_", .x), -c(parid, taxyr))
  land <- read_csv(file.path(folder, "LAND.csv"), col_types = cols(.default = "c"), show_col_types = FALSE) %>% 
    janitor::clean_names() %>%
    rename_with(~ paste0("land_", .x), -c(parid, taxyr))
  ownership <- read_csv(file.path(folder, "OWNERSHIP DATA.csv"), col_types = cols(.default = "c"), show_col_types = FALSE) %>% 
    janitor::clean_names() %>%
    rename_with(~ paste0("own_", .x), -c(parid, taxyr))
  
  
  # merge by parid starting w PARCEL DATA %>% ASMT %>% LEGAL DATA %>% LAND %>% OWNERSHIP DATA
  merged_data <- parcel %>%
    left_join(asmt, by = c("parid", "taxyr")) %>%
    left_join(legal, by = c("parid", "taxyr")) %>%
    left_join(land, by = c("parid", "taxyr")) %>%
    left_join(ownership, by = c("parid", "taxyr"))
  
  # Return nested tibble row
  tibble(
    year = yr,
    data = list(merged_data)
  )
})


# 3. Check column consistency ######################

## get col names from each file
column_names_list <- dekalb_nested %>%
  mutate(cols = map(data, names)) %>%
  select(year, cols)

## get diff, similar column names
common_columns <- Reduce(intersect, column_names_list$cols)
all_columns <- Reduce(union, column_names_list$cols)
different_columns <- setdiff(all_columns, common_columns)

## create a table to view in which files certain columns differ 
column_presence <- map_dfr(different_columns, function(col) {
  tibble(
    column = col,
    year = column_names_list$year,
    present = map_lgl(column_names_list$cols, ~ col %in% .)
  )
}) %>%
  pivot_wider(names_from = year, values_from = present, values_fill = list(present = FALSE)) %>%
  arrange(column)

column_presence

## asmt class is new starting 2020 -- its okay to leave this here
## we can now bind 2015-2022

dekalb_nested <- dekalb_nested %>%
  select(data) %>%  # Keep only necessary columns
  unnest(data)

# 4. Save merged CSV ######################
path <- here::here("output/dekalb/2-processed_clean/RAW_dekalb_digest_2015_2022.csv")
write_csv(dekalb_nested, path)
