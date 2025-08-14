## goal is to combine the fulton commerical and residential sets to have a master dataset.

library(readr)
library(here)
library(tidyverse)
library(dplyr)
library(readxl)
library(janitor)
library(data.table)

## NF, SF, and DIGEST files cleaned
nfsf_digest <- read_csv(here("output/DIGEST_fulton_NFSFDIGEST_2011_2022_apr092025.csv"))

nfsf_digest <- nfsf_digest %>%
  dplyr::rename(yrblt = d_yrblt,
         sf = sfla,
         grade = d_grade)

## need to bring this in because ns/sf does not contain all commercial data
## no vars have been dropped from raw data in this
## commercial data does not have unit number in the notes section
commercial_digest <- read_excel(here::here("output/CMR/DIGEST_fulton_commercial_2011_2022_feb202025.xlsx")) %>%
  clean_names()

############## Column name comparisons ############## 
## get col names from each file
column_names_list <- tibble(data = list(nfsf_digest, commercial_digest)) %>%
  mutate(cols = map(data, names),
         name = c("nfsf", "commercial")) %>%
  select(name, cols)

## get diff, similar column names
common_columns <- Reduce(intersect, column_names_list$cols)
all_columns <- Reduce(union, column_names_list$cols)
different_columns <- setdiff(all_columns, common_columns)

## create a table to view in which files certain columns differ 
column_presence <- map_dfr(different_columns, function(col) {
  tibble(
    column = col,
    name = column_names_list$name,
    present = map_lgl(column_names_list$cols, ~ col %in% .)
  )
}) %>%
  pivot_wider(names_from = name, values_from = present, values_fill = list(present = FALSE)) %>%
  arrange(column)

column_presence

keep_columns <- c(common_columns, "note1", "note2")

############## Bind similar rows only & isolate ownership info ############## 
library(plyr)
fulton_all <- rbind.fill(commercial_digest[common_columns], 
                    nfsf_digest[keep_columns]) %>%
  distinct()

## remove duplicates of parcel ids
fulton_all_owner <- data.table(fulton_all)
fulton_all_owner <- fulton_all_owner[order(-sf), .SD[1], by = .(taxyr, parid)]
fulton_all_owner <- fulton_all_owner[order(taxyr, parid)]


write_csv(fulton_all_owner, file = here::here("output/fulton_ALL_ownership_2011_2022.csv"))





