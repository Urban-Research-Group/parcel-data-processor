"""Manually generate truth files for testing"""

from functools import reduce
import os
import pandas as pd
from truth_utils import read_file

TEST_DATA_DIR = "tests/test_data"
TEST_INPUT_DIR = "tests/test_input"
TRUTH_DIR = "tests/test_truth"
VAR_MAP_PATH = f"{TEST_INPUT_DIR}/var_maps/test_map.csv"
MERGE_KEYS = ["parcel_id", "tax_year"]

# Read data
test_files = os.listdir(TEST_DATA_DIR)
var_map = pd.read_csv(VAR_MAP_PATH)

cmr_files = [f for f in test_files if "CMR" in f]
res_files = [f for f in test_files if "RES" in f]

cmr_dfs = [read_file(TEST_DATA_DIR, f, var_map) for f in cmr_files]
res_dfs = [read_file(TEST_DATA_DIR, f, var_map, "_fdf.") for f in res_files]
res_dfs = [x for x in res_dfs if x is not None]

# need to modify format file pattern so that it takes init file and appends pattern

# Generate append

append_output = pd.concat(cmr_dfs)
append_output.to_csv(os.path.join(TRUTH_DIR, "truth_append.csv"), index=False)

# Generate merge

merge_output = reduce(
    lambda left, right: pd.merge(left, right, on=MERGE_KEYS, how="outer"), res_dfs
)
merge_output.to_csv(os.path.join(TRUTH_DIR, "truth_merge.csv"), index=False)

# Generate append_merge
append_merge_output = pd.merge(append_output, merge_output, on=MERGE_KEYS, how="outer")
append_merge_output.to_csv(
    os.path.join(TRUTH_DIR, "truth_append_merge.csv"), index=False
)

# Generate merge_append
merge_append_output = pd.concat([merge_output, append_output])
merge_append_output.to_csv(
    os.path.join(TRUTH_DIR, "truth_merge_append.csv"), index=False
)
