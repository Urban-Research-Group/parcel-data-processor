"""Manually generate truth files for testing"""

import os
import pandas as pd
from truth_utils import read_file

TEST_DATA_DIR = "tests/test_data"
TEST_INPUT_DIR = "tests/test_input"
TRUTH_DIR = "tests/test_truth"

# Read data

test_files = os.listdir(TEST_DATA_DIR)
cmr_files = [f for f in test_files if "CMR" in f]
res_files = [f for f in test_files if "RES" in f]

cmr_dfs = [read_file(f) for f in cmr_files]
res_dfs = [read_file(f) for f in res_files]

# Generate append

# Generate merge

# Generate append_merge

# Generate merge_append

# Generate append_merge_merge_append
