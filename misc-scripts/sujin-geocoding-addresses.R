library(tidyverse)
library(dplyr)
library(here)
library(janitor)
library(readr)
library(data.table)

## Get top address after grouping by parcel in the time frame they can be geocoded
cobb <- read_csv("output/cobb/cobb_digest_full_final.csv")
clayton <- read_csv("output/clayton/DIGEST_clayton_2012_2022.csv")


## isolate relevant address information
cobb <- cobb %>%
  select(tax_year, parid, propertyaddr, l_adrpre, l_adrno, l_adradd, l_adrdir, l_adrstr, 
         l_adrsuf, l_adrsuf2, l_cityname, l_unitdesc, l_unitno, l_zip1, l_zip2)
clayton <- clayton %>% 
  select(tax_year, pin, streetno, streetname, postdirection, city, zipcode)


## group by parid and get top address
setDT(cobb)
setDT(clayton)

cobb_parcel_addresses <- cobb[order(-tax_year), .SD[1], by = parid]
clayton_parcel_addresses <- clayton[order(-tax_year), .SD[1], by = pin]

path <- paste0("misc-scripts/output/cobb_parcel_addresses_", tolower(format(Sys.time(), "%b%d%Y")), ".csv")
write_csv(cobb_parcel_addresses, file = here::here(path))

path <- paste0("misc-scripts/output/clayton_parcel_addresses_", tolower(format(Sys.time(), "%b%d%Y")), ".csv")
write_csv(clayton_parcel_addresses, file = here::here(path))
