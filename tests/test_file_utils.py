import pytest
from src import file_utils

TEST_DIR = "test_files_dir"


def test_select_files():
    assert file_utils.select_files(TEST_DIR, "CMR") == ["CMR1.txt", "CMR2.txt"]
