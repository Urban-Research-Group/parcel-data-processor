import os
import pandas as pd
from src.run import run
from pandas.testing import assert_frame_equal

CURR_DIR = os.path.dirname(__file__)
TEST_DATA_DIR = os.path.join(CURR_DIR, "test_data")
TEST_INPUT_DIR = os.path.join(CURR_DIR, "test_input")
TEST_OUTPUT_DIR = os.path.join(CURR_DIR, "test_output")
TRUTH_DIR = os.path.join(CURR_DIR, "test_truth")


def test_append():
    config = os.path.join(TEST_INPUT_DIR, "config", "test_append.yaml")
    run(config, exec_name="test_append")

    output_file = os.path.join(TEST_OUTPUT_DIR, "test_append.csv")
    truth_file = os.path.join(TRUTH_DIR, "truth_append.csv")

    df1 = pd.read_csv(output_file)
    df2 = pd.read_csv(truth_file)
    assert_frame_equal(df1, df2, check_like=True)


def test_merge():
    config = os.path.join(TEST_INPUT_DIR, "config", "test_merge.yaml")
    run(config, exec_name="test_merge")

    output_file = os.path.join(TEST_OUTPUT_DIR, "test_merge.csv")
    truth_file = os.path.join(TRUTH_DIR, "truth_merge.csv")
    df1 = pd.read_csv(output_file)
    df2 = pd.read_csv(truth_file)
    assert_frame_equal(df1, df2, check_like=True)


def depreicated_append_merge():
    """Depreciated because this patterns of operations results in var_name_x, var_name_y,
    we do not care about this case at the moment as it is not a desired use case."""
    config = os.path.join(TEST_INPUT_DIR, "config", "test_append_merge.yaml")
    run(config, exec_name="test_append_merge")

    output_file = os.path.join(TEST_OUTPUT_DIR, "test_append_merge.csv")
    truth_file = os.path.join(TRUTH_DIR, "truth_append_merge.csv")
    df1 = pd.read_csv(output_file)
    df2 = pd.read_csv(truth_file)
    assert_frame_equal(df1, df2, check_like=True)


def test_merge_append():
    config = os.path.join(TEST_INPUT_DIR, "config", "test_merge_append.yaml")
    run(config, exec_name="test_merge_append")

    output_file = os.path.join(TEST_OUTPUT_DIR, "test_merge_append.csv")
    truth_file = os.path.join(TRUTH_DIR, "truth_merge_append.csv")
    df1 = pd.read_csv(output_file)
    df2 = pd.read_csv(truth_file)
    assert_frame_equal(df1, df2, check_like=True)
