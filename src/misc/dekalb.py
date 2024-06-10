import pandas as pd
import os
import re

import utils


def read_file(path: str, data_info: pd.DataFrame) -> pd.DataFrame:
    print("Reading file: ", path)

    print("")
    rel_cols = utils.get_rel_cols(path, data_info)
    rel_cols = [col.strip() for col in rel_cols]

    with open(path, "r") as f:
        header = f.readline()
        full_len = len(f.readlines())

    headers = header.split(",")
    headers = [header.strip() for header in headers]

    # get header indices
    header_indices = [headers.index(col) for col in rel_cols]

    data = pd.read_csv(
        path, encoding="latin-1", usecols=header_indices, on_bad_lines="skip"
    )

    # data.columns = header.split(",")

    print(f"Skipped {full_len - len(data)} rows due to data corruption.")

    return data


CURR_PATH = os.getcwd()
DATA_PATH = os.path.join(CURR_PATH, "..", "..", "data", "dekalb")
FORMAT_PATH = os.path.join(
    CURR_PATH, "..", "..", "input", "var_maps", "dekalb", "dekalb.xlsx"
)
data_info = pd.read_excel(FORMAT_PATH)
rel_files = utils.get_rel_files(data_info)

_, subdirs, files = next(os.walk(DATA_PATH))
subdirs = subdirs[1:]  # skip first subdir bc it is not relevant

print(*subdirs, sep="\n")

all_subdir_data = []

for subdir in subdirs:
    curr_path = os.path.join(DATA_PATH, subdir)
    rel_path_files = [os.path.join(curr_path, f"{name}.csv") for name in rel_files]

    data = None
    for f in rel_path_files:
        try:
            data = [read_file(f, data_info)]
            data = utils.execute_instructions(data, data_info)
        except Exception as e:
            print(f"Error reading file {f}: {e}")
            continue

    # combined_subdir_data = utils.merge_data()
    # pd.concat(data, axis=1)  # NEED TO MERGE ON KEYS
    # combined_subdir_data.to_csv(os.path.join(curr_path, "combined.csv"), index=False)

    # all_subdir_data.append(combined_subdir_data)

# all_data = pd.concat(all_subdir_data, axis=0)
# all_data.to_csv(os.path.join(DATA_PATH, "merged_cleaned_data.csv"), index=False)
