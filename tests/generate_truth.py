"""Manually generate truth files for testing"""

from functools import reduce
import os
import pandas as pd
from truth_utils import read_file, create_derived_vars, clean_df

TEST_DATA_DIR = "tests/test_data"
TEST_INPUT_DIR = "tests/test_input"
TRUTH_DIR = "tests/test_truth"
VAR_MAP_PATH = f"{TEST_INPUT_DIR}/var_maps/test_map.csv"
MERGE_KEYS = ["parcel_id", "tax_year"]

# Read data
test_files = [
    os.path.join(dirpath, file)
    for dirpath, _, filenames in os.walk(TEST_DATA_DIR)
    for file in filenames
]

var_map = pd.read_csv(VAR_MAP_PATH)
derived_mask = var_map["old_name"].apply(lambda x: ";" in x)
var_map_non_derived = var_map[~derived_mask]
var_map_derived = var_map[derived_mask]

cmr_files = [f for f in test_files if "CMR" in f]
res_files = [f for f in test_files if "RES" in f]

cmr_dfs = [read_file(path=f, var_map=var_map_non_derived) for f in cmr_files]
res_dfs = [
    read_file(path=f, var_map=var_map_non_derived, format_pat="_fdf.")
    for f in res_files
]
res_dfs = [x for x in res_dfs if x is not None]

# Generate append

append_output = pd.concat(cmr_dfs)
append_output = clean_df(append_output, var_map_non_derived)
append_output = create_derived_vars(append_output, var_map_derived)
append_output.to_csv(os.path.join(TRUTH_DIR, "truth_append.csv"), index=False)

# Generate merge

merge_output = reduce(
    lambda left, right: pd.merge(left, right, on=MERGE_KEYS, how="outer"), res_dfs
)

merge_output = clean_df(merge_output, var_map_non_derived)
merge_output = create_derived_vars(merge_output, var_map_derived)
merge_output.to_csv(os.path.join(TRUTH_DIR, "truth_merge.csv"), index=False)

# Generate append_merge
append_merge_output = pd.merge(append_output, merge_output, on=MERGE_KEYS, how="outer")
append_merge_output = clean_df(append_merge_output, var_map_non_derived)
append_merge_output = create_derived_vars(append_merge_output, var_map_derived)
append_merge_output.to_csv(
    os.path.join(TRUTH_DIR, "truth_append_merge.csv"), index=False
)

# Generate merge_append
merge_append_output = pd.concat([merge_output, append_output])
merge_append_output = clean_df(merge_append_output, var_map_non_derived)
merge_append_output = create_derived_vars(merge_append_output, var_map_derived)
merge_append_output.to_csv(
    os.path.join(TRUTH_DIR, "truth_merge_append.csv"), index=False
)
