# Data Record Notes

## Fulton (Finished)

### Process
- Read files (selecting only needed columns)
- Append all files
- Clean columns
- Add column specifying Fulton as the data source
- Create new columns for site address and owner address
- Export to csv and parquet

### Notes
- The universe of parcels includes SF, NF, ATL, 14th, 17th

### Missing Data
- All file subsets start at 2010, except the ATL parcel data, which starts at 2011.
- 14th has a strangely formatted 2015-2016 file and appears to have very few entries for this year.

### Column Mapping & Constructed Columns

### Questions

---
## Paulding

### Process
- For 2011-2013, scrape the schema off of the corresponding PDF. Insert delimiter and read as CSVs.
- For 2014-2023, read in the dbf files.
- Merge the dbf files for each year to get the complete set of variables.
- Append all data.
- Clean columns.
- Clean columns.
- Create desired columns (site addr and owner addr).
- Export to CSV and Parquet.

### Notes
- 2011-2013 are in a text file format. 2011 has a different column format than the other 2.
- The text file format files do not have a delimiter. Therefore, a delimiter needs to be imposed by scanning the schema description. By doing so, we can determine the indexes each column should be at.
- 2014-2023 are in dbf format. There are an extremely large number of files. It is hard to identify which files have the variables we need.
- There is a good Sales Code list detailed qualified and non-qualified sales.

### Column Mapping & Constructed Columns

### Questions

---
## Cobb

### Process
- For 2000-2019, extract the schema files to impose delimiters. Then read these as CSVs.
- Merge the CSVs for each year to get the complete set of variables.
- Append the CSVs for each year.
- Clean columns.
- Create desired additional columns.
- Export to CSV and Parquet.

### Notes
- We have 2000, 2011-2016, and 2022 data.
- Has extensive extra documentation- for instance, a TaxBills file which contains the data used to produce tax bills.
- Data is stored in DAT files. These files don't have a delimiter until after 2020. Therefore, a delimiter needs to be imposed by scanning the schema description. By doing so, we can determine the indexes each column should be at.

### Missing Data
- Appear to be missing 2017-2021.

### Column Mapping & Constructed Columns

### Questions

---
## Cherokee

### Missing Data
- Appear to be missing 2011-2019.

---
## DeKalb

### Notes
- Data 