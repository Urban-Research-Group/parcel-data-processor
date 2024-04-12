######
# mel-data-val.r
# purpose: read in output, logic data and validate data in each col
# author: melissa juarez
#####

# Import libraries
library(dplyr)
library(here)
library(tidyverse)
library(stringr)
library(readr)
library(readxl)
library(validate)

## read in output df and logic df -- i created 100,000 row sample for faster testing
output_df <- read_csv(here("data/outputfulton.csv"), col_types = cols(.default = col_character()))
sample <- sample_n(output_df, 100000)

logic <- read_excel(here::here("validation/rules.xlsx")) # must have 3 columns: name, rule, description

## missingness on sample of 10,000 -- estimated 24.6% missing data
visdat::vis_miss(sample_n(output_df, 10000), warn_large_data = FALSE)

# Some missingness may be allowable in certain columns, but not in others. We should differentiate where
# NAs would be a problem.


## run validator ##################################################################################
rules <- validator(.data = logic)

out <- confront(sample, rules)
results <- summary(out)
plot(out) # 
violating(sample, out) # due to the validator not allowing NAs, almost every row fails in some way.

# Some notes: currently, the validator allows `grepl` for pattern recognition which returns only T/F.
# NAs in `grepl` are read as False, meaning that the validator marks them as failed tests rather than
# missing. I have included a data missingness viz to supplement and differentiate the levels of failed
# test versus just NAs.
# 
# I assume the handling of NAs is column specific. For example, NAs in owner_country may be allowable (?),
# but in columns describing grade or address information, it may not be allowable -- and should be read as 
# fails. Moving forward, we should specify which is which.

## trying with full dataset to see runtime -- ~1 min ##############################################
start.time <- Sys.time()

out <- confront(output_df, rules)
results <- summary(out)
plot(out)
violating(output_df, out)

end.time <- Sys.time()
time.taken <- round(end.time - start.time,2)
time.taken
