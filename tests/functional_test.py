import os
import pandas as pd
from src import main

curr_dir = os.path.dirname(__file__)
TEST_DIR = f"{curr_dir}/test_data"
TEST_OUTPUT = f"{curr_dir}/test_output/test_county.csv"
TRUTH_DIR = f"{curr_dir}/truth"
TEST_CONFIG = TEST_DIR + "/test.yaml"

# INPUTS: test.yaml, test_files
# OUTPUT: one single file, test_output.csv

# add fwf file
# test append alone (format file and derived variables)
# test merge alone (format file and derived variables)
# test merge and append
# test append and merge
# test append and merge and append


def test_functional():
    main._main(TEST_CONFIG)
    output_file = TEST_OUTPUT
    ground_truth = f"{TRUTH_DIR}/truth_test.csv"
    assert pd.read_csv(output_file).equals(pd.read_csv(ground_truth))
