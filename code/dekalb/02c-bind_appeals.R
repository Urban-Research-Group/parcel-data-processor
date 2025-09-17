## Goal here is to put together one digest file per year based on all the different datasets

library(tidyverse)
library(here)
library(readr)
library(janitor)
library(readxl)

###############################################################################
# 2011-12 that are in singular CSVs
###############################################################################

# 1. Make sure that the table names are same cross years ######################

appeals_paths <- list.files(here("data/dekalb/APPEALS"), 
                            recursive = T,
                            full.names = T)

## if .txt then it is the data, if .xlsx then its the dictionary
appeal_files_df <- data.frame(
  year = str_extract(appeals_paths, "\\d{4}"),
  data_path = appeals_paths
)

## read in and clean data dictionaries
appeal_files_df <- appeal_files_df %>%
  mutate(
    data = map(data_path, ~ {
      read_excel(.x) %>% clean_names()
    })
  )

# 2. Check column consistency ######################

## get col names from each file
column_names_list <- appeal_files_df %>%
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

appeal_files_df <- appeal_files_df %>%
  mutate(data = map2(data, year, ~ if (str_detect(.y, "2017")) {
    .x %>%
      select(-x6, -x7)
  } else {
    .x
  }))
## we can now bind

appeal_files_df <- appeal_files_df %>%
  select(data) %>%
  unnest(data)

# 3. Save merged CSV ######################
path <- here::here("output/dekalb/2-processed_clean/APPEALS_dekalb_digest_2011_2012.csv")
write_csv(appeal_files_df, path)
