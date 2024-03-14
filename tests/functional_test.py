import os
import pandas as pd
from src.run import run

CURR_DIR = os.path.dirname(__file__)
TEST_DATA_DIR = os.path.join(CURR_DIR, "test_data")
TEST_INPUT_DIR = os.path.join(CURR_DIR, "test_input")
TEST_OUTPUT_DIR = os.path.join(CURR_DIR, "test_output")
TRUTH_DIR = os.path.join(CURR_DIR, "test_truth")


def test_append():
    config = os.path.join(TEST_INPUT_DIR, "config", "test_append.yaml")
    run._main(config, exec_name="test_append")

    output_file = os.path.join(TEST_OUTPUT_DIR, "test_append.csv")
    truth_file = os.path.join(TRUTH_DIR, "truth_append.csv")

    test_output = pd.read_csv(output_file)
    truth = pd.read_csv(truth_file)
    print(test_output.compare(truth))
    assert test_output.equals(truth)


def test_merge():
    config = os.path.join(TEST_INPUT_DIR, "config", "test_merge.yaml")
    run._main(config, exec_name="test_merge")

    output_file = os.path.join(TEST_OUTPUT_DIR, "test_merge.csv")
    truth_file = os.path.join(TRUTH_DIR, "truth_merge.csv")
    assert pd.read_csv(output_file).equals(pd.read_csv(truth_file))


def test_append_merge():
    config = os.path.join(TEST_INPUT_DIR, "config", "test_append_merge.yaml")
    run._main(config, exec_name="test_append_merge")

    output_file = os.path.join(TEST_OUTPUT_DIR, "test_append_merge.csv")
    truth_file = os.path.join(TRUTH_DIR, "truth_append_merge.csv")
    assert pd.read_csv(output_file).equals(pd.read_csv(truth_file))


def test_merge_append():
    config = os.path.join(TEST_INPUT_DIR, "config", "test_merge_append.yaml")
    print(config)
    run._main(config, exec_name="test_merge_append")

    output_file = os.path.join(TEST_OUTPUT_DIR, "test_merge_append.csv")
    truth_file = os.path.join(TRUTH_DIR, "truth_merge_append.csv")
    assert pd.read_csv(output_file).equals(pd.read_csv(truth_file))
