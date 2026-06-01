import os
import pandas as pd
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="multiprocessing.resource_tracker")

DIGEST_MAP_PATH = "data/digest_map_clean.csv"
DIGEST_KEEP_PATH = "data/digest_keep.csv"
DIGEST_DELETE_PATH = "data/digest_delete.csv"

SALES_MAP_PATH = "data/sales_map_clean.csv"
SALES_KEEP_PATH = "data/sales_keep.csv"
SALES_DELETE_PATH = "data/sales_delete.csv"

TAX_DIGEST_PATH = "/Users/tpeng/Library/CloudStorage/OneDrive-GeorgiaInstituteofTechnology/Housing and Urban Policy (HUP) Lab - Documents/Data Files/Final Datasets"
SALES_DIGEST_PATH = "/Users/tpeng/Library/CloudStorage/OneDrive-GeorgiaInstituteofTechnology/Housing and Urban Policy (HUP) Lab - Documents/Data Files/Raw Data/ATLSales/FINAL"

COUNTIES = ["fulton", "gwinnett", "cobb", "dekalb", "clayton"]
METADATA_FILES = [DIGEST_MAP_PATH, DIGEST_KEEP_PATH, DIGEST_DELETE_PATH, SALES_MAP_PATH, SALES_KEEP_PATH, SALES_DELETE_PATH]

# Check for the existence of the desired files
for county in COUNTIES:
    tax_digest_path = TAX_DIGEST_PATH + f"/{county.capitalize()}/{county}_final_cleaned.csv"
    sales_digest_path = SALES_DIGEST_PATH + f"/{county.upper()}_SALES_FINAL.csv"
    if not os.path.exists(tax_digest_path):
        print(f"No tax digest for {county} county at: {tax_digest_path}")
        exit(1)

    if not os.path.exists(sales_digest_path):
        print(f"No sales digest for {county} county at: {sales_digest_path}")
        exit(1)

for metadata_path in METADATA_FILES:
    if not os.path.exists(metadata_path):
        print(f"{metadata_path} does not exist.")
        exit(1)

digest_map = pd.read_csv(DIGEST_MAP_PATH)
digest_keep = pd.read_csv(DIGEST_KEEP_PATH)
digest_delete = pd.read_csv(DIGEST_DELETE_PATH)
sales_map = pd.read_csv(SALES_MAP_PATH)
sales_keep = pd.read_csv(SALES_KEEP_PATH)
sales_delete = pd.read_csv(SALES_DELETE_PATH)
actual_dig_cols_map = {}
actual_sales_cols_map = {}

missing = False
for county in COUNTIES:
    tax_digest_path = TAX_DIGEST_PATH + f"/{county.capitalize()}/{county}_final_cleaned.csv"
    sales_digest_path = SALES_DIGEST_PATH + f"/{county.upper()}_SALES_FINAL.csv"
    exp_dig_cols = pd.concat([digest_map[county.capitalize()].dropna(), 
                                digest_keep[county.capitalize()].dropna(),
                                digest_delete[county.capitalize()].dropna()], axis=0).tolist()
    
    exp_sales_cols = pd.concat([sales_map[county.capitalize()].dropna(), 
                                sales_keep[county.capitalize()].dropna(),
                                sales_delete[county.capitalize()].dropna()], axis=0).tolist()
    
    actual_dig_cols = pd.read_csv(tax_digest_path, nrows=0).columns.tolist()
    actual_dig_cols_map[county] = actual_dig_cols

    actual_sales_cols = pd.read_csv(sales_digest_path, nrows=0).columns.tolist()
    actual_sales_cols_map[county] = actual_sales_cols

    for expected in exp_dig_cols:
        if expected not in actual_dig_cols:
            print(f"{county.upper()}: {expected} column not present in tax digest.")
            missing = True

    for act in actual_dig_cols:
        if act not in exp_dig_cols:
            print(f"{county.upper()}: Extra {act} column not expected in tax digest.")
            missing = True

    for expected in exp_sales_cols:
        if expected not in actual_sales_cols:
            print(f"{county.upper()}: {expected} column not present in sales digest.")
            missing = True

    for act in actual_sales_cols:
        if act not in exp_sales_cols:
            print(f"{county.upper()}: Extra {act} column not expected in sales digest.")
            missing = True
if missing:
    exit(1)

man_keep_tax_cols_map = {
    "fulton": [],
    "gwinnett": [],
    "cobb": [],
    "dekalb": [],
    "clayton": []
}

man_keep_sales_cols_map = {
    "fulton": [],
    "gwinnett": ['LEGAL1'],
    "cobb": ['TRANSDT'],
    "dekalb": [],
    "clayton": []
}

tax_dfs = []
sales_dfs = []
merged_dfs = []
threshold = 0.95
for county in COUNTIES:
    print(f"---- {county.capitalize()} Tax Digest ----")
    tax_digest_path = TAX_DIGEST_PATH + f"/{county.capitalize()}/{county}_final_cleaned.csv"
    tax_df = pd.read_csv(tax_digest_path, low_memory=False)

    keep_tax_cols = digest_map[county.capitalize()].dropna().to_list()
    man_keep_tax_cols = man_keep_tax_cols_map[county]

    sparse_tax_cols = tax_df.columns[(tax_df.isnull().mean() >= threshold) & 
                                     ~tax_df.columns.isin(keep_tax_cols) & ~tax_df.columns.isin(man_keep_tax_cols)].to_list()

    print(f"The following columns are sparse for {county} and will be dropped from tax digest:")
    print(sparse_tax_cols)

    keep_tax_cols = [x for x in actual_dig_cols_map[county] if 
                     ((x not in digest_delete[county.capitalize()].dropna().to_list() and (x not in sparse_tax_cols)))]
    
    digest_rename_map = digest_map[["Standardized Name", county.capitalize()]][~digest_map[county.capitalize()] \
                                    .isnull()].set_index(county.capitalize()).to_dict()["Standardized Name"]
    
    # Get the names of the parcel_id and tax_year in their respective files, so as to preserve any sort of leading zeros etc.
    # that might be clipped by data_type inference
    pid_orig_name = digest_map[digest_map["Human Readable Name"] == "Parcel ID"][county.capitalize()].item()
    taxyr_orig_name = digest_map[digest_map["Human Readable Name"] == "Tax Year"][county.capitalize()].item()
    tax_df = pd.read_csv(tax_digest_path, usecols=keep_tax_cols, low_memory=False, 
                         dtype={pid_orig_name: str, taxyr_orig_name: str}).rename(columns=digest_rename_map) # preserve the datatypes of the fields that will be merged on
    tax_df['ORIG_COUNTY'] = county.capitalize()

    tax_dfs.append(tax_df)

final_tax_df = pd.concat(tax_dfs, axis=0, ignore_index=True)
digest_shared_cols = digest_map["Standardized Name"].to_list()
digest_columns = digest_shared_cols + [col for col in final_tax_df.columns if col not in digest_shared_cols]
final_tax_df = final_tax_df[digest_columns]
final_tax_df.to_csv("data/five_counties_tax_digest.csv", index=False)

del tax_dfs

for county in COUNTIES:
    print(f"---- {county.capitalize()} Sales Digest ----")
    sales_digest_path = SALES_DIGEST_PATH + f"/{county.upper()}_SALES_FINAL.csv"
    sales_df = pd.read_csv(sales_digest_path, low_memory=False)

    keep_sales_cols = sales_map[county.capitalize()].dropna().to_list()
    man_keep_sales_cols = man_keep_sales_cols_map[county]

    sparse_sales_cols = sales_df.columns[(sales_df.isnull().mean() >= threshold) & 
                                         ~sales_df.columns.isin(keep_sales_cols) & ~sales_df.columns.isin(man_keep_sales_cols)].to_list()
    
    print(f"The following columns are sparse for {county} and will be dropped from sales digest:")
    print(sparse_sales_cols)

    keep_sales_cols = [x for x in actual_sales_cols_map[county] if 
                     ((x not in sales_delete[county.capitalize()].dropna().to_list() and (x not in sparse_sales_cols)))]
    
    sales_rename_map = sales_map[["Standardized Name", county.capitalize()]][~sales_map[county.capitalize()].isnull()] \
                                .set_index(county.capitalize()).to_dict()["Standardized Name"]
    
    # Get the names of the parcel_id and sale_year in their respective files, so as to preserve any sort of leading zeros etc.
    # that might be clipped by data_type inference
    pid_orig_name = sales_map[sales_map["Human Readable Name"] == "Parcel ID"][county.capitalize()].item()
    saleyr_orig_name = sales_map[sales_map["Human Readable Name"] == "Sale Year"][county.capitalize()].item()
    sales_df = pd.read_csv(sales_digest_path, usecols=keep_sales_cols, 
                           dtype={pid_orig_name: str, saleyr_orig_name: str}, low_memory=False).rename(columns=sales_rename_map)
    sales_df['sale_dt'] = pd.to_datetime(sales_df['sale_dt'])
    sales_df['ORIG_COUNTY'] = county.capitalize()

    sales_dfs.append(sales_df)

final_sales_df = pd.concat(sales_dfs, axis=0, ignore_index=True)
sales_shared_cols = sales_map["Standardized Name"].to_list()
sales_columns = sales_shared_cols + [col for col in final_sales_df.columns if col not in sales_shared_cols]
final_sales_df = final_sales_df[sales_columns]
final_sales_df.to_csv("data/five_counties_sales_digest.csv", index=False)

del sales_dfs

# Merging of the final files
final_sales_df = pd.read_csv("data/five_counties_sales_digest.csv", low_memory=False, dtype={"parcel_id": str, "sale_yr": str})
sales_shared_cols = sales_map["Standardized Name"].to_list()
sales_columns = sales_shared_cols + [col for col in final_sales_df.columns if col not in sales_shared_cols]
final_sales_df = final_sales_df[sales_columns]

final_tax_df_columns = pd.read_csv("data/five_counties_tax_digest.csv", low_memory=False, dtype={"parcel_id": str, "tax_year": str}, nrows=2).columns
digest_shared_cols = digest_map["Standardized Name"].to_list()
digest_columns = digest_shared_cols + [col for col in final_tax_df_columns if col not in digest_shared_cols] # column order

chunks = []
with pd.read_csv("data/five_counties_tax_digest.csv", low_memory=False, 
                 dtype={"parcel_id": str, "tax_year": str}, chunksize=10000) as reader:
    for chunk in tqdm(reader):
        chunk = chunk[digest_columns]
        merged_chunk = pd.merge(final_sales_df, chunk, how="inner", left_on=["parcel_id", "sale_yr", "ORIG_COUNTY"],
            right_on=["parcel_id", "tax_year", "ORIG_COUNTY"])
        chunks.append(merged_chunk)

final_merged_df = pd.concat(chunks, ignore_index=True)

# final_merged_df = pd.merge(final_sales_df, final_tax_df, how="left", left_on=["parcel_id", "sale_yr", "ORIG_COUNTY"], right_on=["parcel_id", "tax_year", "ORIG_COUNTY"])
# merged_shared_cols = sales_shared_cols + [col for col in digest_shared_cols if col not in sales_shared_cols]
# merged_columns = merged_shared_cols + [col for col in final_merged_df.columns if col not in merged_shared_cols]
# final_merged_df = final_merged_df[merged_columns]
final_merged_df.to_csv("data/five_counties_merged_sales_digest.csv", index=False)