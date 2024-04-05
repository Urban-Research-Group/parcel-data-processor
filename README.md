[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![Coverage](badges/coverage.svg)

# Data Processor
Created to process Parcel, Appeals, and Sales data for the GA Tax Assessment Project, affiliated with the Urban Research Lab (URL) at Georgia Tech.

## Contents
1. Purpose
2. Usage
3. Documentation

## Purpose
This application enables the creation of flexible data processing protocols with a few pieces of information. These inputs create documentation of the process that even non-engineers can understand or contribute to. Creating an entirely different script for each data source can is avoided, encouraging a maintainable system.

This project was created to solve the URL's challenges related to historical tax data. We have data from many metro Atlanta counties, spanning many years and in many different formats. Some formats even require custom code to read because their schemas are defined in Word or PDF documents. To efficiently analyze this data for our research purposes, we needed to re-format each separate data source into the same structure.

## Usage

### What Can it Do?
- Use a powerful but readable YAML syntax to perform data operations.
- Run operations over many files using a REGEX pattern instead of manually specifying each file.
- Define a data schema ("var_map") for data reformatting which is done automatically in the processing.
- Perform operations on the result of previous operations, effectively allowing any combination of common data transformations.
- Specify custom file parsers if the data cannot be read with default Pandas I/O functions.

### Installing Requirements
Before running the program, make a virtual environment from the provided requirements.txt file.

Option 1, conda: 
```bash
conda env create -f requirements.txt
```
Option 2, venv:
```bash
pip install -r requirements.txt
```
If you can't get this to work, you can manually run the following command for each package included in requirements.txt.
```bash
pip install <requirement>
```

### How to Run
```bash
python src/processor <config_path> <execution_name>
```
<config_path>: path to YAML file with execution instructions \
<execution_name>: name given to identify the current execution

## Documentation

### Inputs

Two pieces of information are needed as inputs to the processor, everything else is handled automatically:
- YAML instruction set/config file
- Variable mapping
- OPTIONALLY: custom parser

### YAML Schema
```YAML
CONFIG_SCHEMA = Schema(
    {
        "name": str,
        "root-path": str,
        "var-map-path": str,
        "operations": {
            str: {
                "type": Or("append", "merge", "join"),
                Optional("groups"): [str],
                "files": {str: str},
                Optional("key"): [str],
                Optional("join-type"): str,
            }
        },
        "output": {"path": str, "formats": [str]},
    }
)
```

**Global**
```yaml
name: String
    User-defined name for the data process

root-path: String
    Absolute (recommended) or relative path to the root source data folder

var-map-path: String
    Absolute (recommended) or relative path to file containing variable mapping file

operations: Dict[str: dict]
    Specifications of the operations to be completed

output: Dict[str: str]
    Specifications of output type and destination
```

**Operations**
```yaml
{operation-name}:
    type: {append, merge or join}
    Optional(groups): {specifies if the file patterns should be separated into groups, for instance, by year}
        - group-pattern-1
        - group-pattern-2
        - ...
    files:
        {file1-pattern}: {parser1-name}
        {file2-pattern}: {parser2-name}
        ...
    Optional(key):
        - {merge-key-col}
    Optional(join-type): {left, right, inner, outer, cross}
...
```
Note: make sure key columns are the new column names, not the original ones

### Output
```yaml
output:
    path: {absolute-output-path}
    formats:
        - {csv, parquet}
        - ...
```

### Variable Mapping (CSV)
```yaml
COLUMNS: -----
old_name: variable name as-is from the source data
new_name: new name for the variable after processing
data_type: desired data type of the variable after processing
source_file: pattern of the file 
```

### Derived Variables

Derived variables, which are the result of concatenting other columns, can be defined by creating a list of variable names in the `new_name` column with a semicolon as delimeter.

Example: address is derived from the source data columns of address_number and address_string. The `new_name` column should be `address_number;address_string`

### Custom Parser
Custom parsers that read atypical source files can be defined in `src/processor/parser.py`. They must accept a file_path and return a Pandas dataframe.

## Example

This example uses Cherokee County tax data.

The source data has a directory structure as follows:
```
cherokee
    digest2020-Cherokee
        d_real_fdf.txt
        d_real.txt
        d_sales_fdf.txt
        d_sales.txt
        ...
    digest2021-Cherokee
        ...
    digest2022-Cherokee
        ...
```

Across each year folder, the file names are identical. **We want to merge each of the files within their year folder, then append all of the results.** The following specifications get us that result.

The YAML configuration below selects for each year folder in `groups`, and selects the desired files within each group with a REGEX pattern. It also specifies the name of a custom parser located in `parsers.py`. This custom parser uses the `_fdf.txt` files for the schema of each corresponding `txt` file.


### YAML Config for Cherokee County
```yaml
name: cherokee
root-path: C:\Users\Nick\Documents\code\ga-tax-assessment\data\cherokee
var-map-path: C:\Users\Nick\Documents\code\ga-tax-assessment\input\var_maps\cherokee_var_map.csv
operations:
  merge-years:
    type: merge
    groups:
      - "2020"
      - "2021"
      - "2022"
    files:
      ".*d_real.*": cherokee
      ".*d_owner.*": cherokee
      ".*d_calcexemptions.*": cherokee
      ".*d_accsry.*": cherokee
    key:
      - parcel_id
      - tax_year
    join-type: outer
  append-years:
    type: append
    files:
      merge-years: default
output:
  path: cherokee-output
  formats:
    - csv
```

### Variable Mapping for Cherokee County
```csv
old_name,new_name,data_type,source_file

TAXDISTRIC,tax_district,string,d_real.txt
HOUSE_NO;EXTENSION;STDIRECT,site_addr,string,d_real.txt
GRANTEE,buyer_name,string,d_sales.txt
...{truncated for readability}
```