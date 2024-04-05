import os
import pandas as pd
from src.processor.run import run
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

    df1 = pd.read_csv(output_file).drop(columns=["source_file"]).reset_index(drop=True)
    df2 = pd.read_csv(truth_file).drop(columns=["source_file"]).reset_index(drop=True)
    assert assert_frame_equal(df1, df2, check_like=True) is None


def test_merge():
    config = os.path.join(TEST_INPUT_DIR, "config", "test_merge.yaml")
    run(config, exec_name="test_merge")

    output_file = os.path.join(TEST_OUTPUT_DIR, "test_merge.csv")
    truth_file = os.path.join(TRUTH_DIR, "truth_merge.csv")
    df1 = pd.read_csv(output_file).drop(columns=["source_file"]).reset_index(drop=True)
    df2 = pd.read_csv(truth_file).drop(columns=["source_file"]).reset_index(drop=True)
    assert assert_frame_equal(df1, df2, check_like=True) is None


def test_merge_append():
    config = os.path.join(TEST_INPUT_DIR, "config", "test_merge_append.yaml")
    run(config, exec_name="test_merge_append")

    output_file = os.path.join(TEST_OUTPUT_DIR, "test_merge_append.csv")
    truth_file = os.path.join(TRUTH_DIR, "truth_merge_append.csv")
    df1 = pd.read_csv(output_file).drop(columns=["source_file"]).reset_index(drop=True)
    df2 = pd.read_csv(truth_file).drop(columns=["source_file"]).reset_index(drop=True)
    print(df1)
    print(df2)
    assert assert_frame_equal(df1, df2, check_like=True) is None
