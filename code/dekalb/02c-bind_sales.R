
## Goal here is to put together one sales file per year based on all the different datasets

library(tidyverse)
library(here)
library(readr)
library(janitor)


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
nested_sales <- map_dfr(years, function(yr) {
  folder <- year_folders[str_detect(year_folders, as.character(yr))]
  
  # read tables directly & select/rename relevant columns
  ## need to append 
  sales <- read_csv(file.path(folder, "SALES.csv"), col_types = cols(.default = "c"), show_col_types = FALSE) %>% 
    janitor::clean_names() 
  
  # Return nested tibble row
  tibble(
    year = yr,
    data = list(sales)
  )
})


# 3. Check column consistency ######################

## get col names from each file
column_names_list <- nested_sales %>%
  mutate(cols = map(data, names)) %>%
  select(year, cols)

## get diff, similar column names
common_columns <- Reduce(intersect, column_names_list$cols)
all_columns <- Reduce(union, column_names_list$cols)
different_columns <- setdiff(all_columns, common_columns)

## create a table to view in which files certain columns differ 
column_presence <- ifelse(is_empty(different_columns),"There are no differing columns any year", 
  
  map_dfr(different_columns, function(col) {
  tibble(
    column = col,
    year = column_names_list$year,
    present = map_lgl(column_names_list$cols, ~ col %in% .)
  )
}) %>%
  pivot_wider(names_from = year, values_from = present, values_fill = list(present = FALSE)) %>%
  arrange(column) )

column_presence


## we can now bind 2015-2022

nested_sales <- nested_sales %>%
  select(data) %>%  # Keep only necessary columns
  unnest(data)

# 4. Save merged CSV ######################
path <- here::here("output/dekalb/2-processed_clean/SALES_dekalb_digest_2015_2022.csv")
write_csv(nested_sales, path)
