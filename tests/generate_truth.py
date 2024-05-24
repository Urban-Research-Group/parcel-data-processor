"""Manually generate truth files for testing"""

from functools import reduce
import os
import pandas as pd
from .truth_utils import read_file, create_derived_vars, clean_df
from file_utils import select_files

TEST_DATA_DIR = "tests/test_data"
TEST_INPUT_DIR = "tests/test_input"
TRUTH_DIR = "tests/test_truth"
VAR_MAP_PATH = f"{TEST_INPUT_DIR}/var_maps/test_map.csv"
MERGE_KEYS = ["parcel_id", "tax_year"]
GROUP_MERGE_KEYS = ["parcel_id"]


def generate_truth():
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

    cmr_files = select_files(test_files, (None, ".*CMR.csv"))
    res_files = select_files(test_files, (None, "^(?!.*_fdf).*RES_OWNER.*$"))
    res_files += select_files(test_files, (None, ".*RES_A.*$"))

    cmr_dfs = [
        read_file(path=f, parser="default", var_map=var_map_non_derived)
        for f in cmr_files
    ]
    res_dfs = [
        (
            read_file(path=f, parser="cherokee", var_map=var_map_non_derived)
            if "OWNER" in f
            else read_file(path=f, parser="default", var_map=var_map_non_derived)
        )
        for f in res_files
    ]

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

    # Generate merge_append

    merge_append_output = pd.concat([merge_output, append_output])
    merge_append_output = clean_df(merge_append_output, var_map_non_derived)
    merge_append_output = create_derived_vars(merge_append_output, var_map_derived)
    merge_append_output.to_csv(
        os.path.join(TRUTH_DIR, "truth_merge_append.csv"), index=False
    )

    # Generate test groups
    gf2020 = select_files(test_files, ("2020", ".*APPL.csv")) + select_files(
        test_files, ("2020", ".*EXC.csv")
    )
    gf2021 = select_files(test_files, ("2021", ".*APPL.csv")) + select_files(
        test_files, ("2021", ".*EXC.csv")
    )

    dfs2020 = []
    dfs2021 = []
    for f in gf2020:
        dfs2020.append(read_file(path=f, parser="default", var_map=var_map_non_derived))

    for f in gf2021:
        dfs2021.append(read_file(path=f, parser="default", var_map=var_map_non_derived))

    df2020 = reduce(
        lambda left, right: pd.merge(left, right, on=GROUP_MERGE_KEYS, how="outer"),
        dfs2020,
    )

    df2021 = reduce(
        lambda left, right: pd.merge(left, right, on=GROUP_MERGE_KEYS, how="outer"),
        dfs2021,
    )

    df = pd.concat([df2020, df2021])
    df = clean_df(df, var_map_non_derived)
    df = create_derived_vars(df, var_map_derived)
    df.to_csv(os.path.join(TRUTH_DIR, "truth_groups.csv"), index=False)
