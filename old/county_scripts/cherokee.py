import os
import re

import pandas as pd
import numpy as np
import polars as pl

import utils
import config

COUNTY = "cherokee"
DATA_CLASS = "parcel"
PATH_TO_COUNTY = config.PATH_TO_DATA + f"\{COUNTY}\\"


def get_widths(format_file: np.array):
    return format_file[:, 2].astype(np.int16).tolist()


def read_format_file(path):
    return np.loadtxt(path, dtype=str)


def get_column_headers(format_file: np.array):
    header_col = format_file[:, 0]
    return header_col


def drop_vars(df, var_map):
    columns_to_keep = [row["old_name"] for row in var_map]
    return df.loc[:, columns_to_keep]


def get_format_path(path):
    format_path = (path).split(".")
    format_path.insert(1, "_fdf.")
    format_path = "".join(format_path)
    return format_path


def read_file(path, year, desired_vars, var_dtypes):
    print(f"Reading file: {path}")
    format_file = read_format_file(get_format_path(path))
    widths = get_widths(format_file)
    column_headers = get_column_headers(format_file)

    file_df = pd.read_fwf(path, widths=widths, header=None, encoding="latin-1")

    file_df.columns = column_headers
    file_df = file_df[desired_vars]

    final_dtypes = {k: v for k, v in var_dtypes.items() if k in file_df.columns}
    file_df = file_df.astype(final_dtypes)
    file_df["year"] = year
    file_df["source_file"] = path
    return file_df


def main():
    # READ IN FORMAT FILES
    # mapping_processor = utils.MappingProcessor(COUNTY)
    # desired_files, var_map = mapping_processor.get_mapping(DATA_CLASS)
    # READ IN FILES WE NEED
    dfs = []
    desired_files = ["D_TAXMASTER.txt", "d_real.txt"]
    desired_variables = {
        "D_TAXMASTER.txt": ["REALKEY", "LASTNAME"],
        "d_real.txt": ["REALKEY", "STREET_NAM"],
    }
    var_dtypes = {"REALKEY": str, "LASTNAME": str, "STREET_NAME": str}

    filenames = set()
    for folder in os.listdir(PATH_TO_COUNTY):
        print(f"Reading folder {folder} ----")
        year_df = []
        year = re.search(r"(19|20)\d{2}", folder).group()
        folder_path = PATH_TO_COUNTY + folder
        for file in os.listdir(folder_path):
            if file in desired_files:
                filenames.update([file])
                desired_vars = desired_variables[file]
                year_df.append(
                    read_file(
                        os.path.join(folder_path, file), year, desired_vars, var_dtypes
                    )
                )
        dfs.append(year_df)

    # print(dfs)

    # DROP UN-NEEDED VARS - DONE
    # GET YEAR FROM FOLDER - DONE
    # CLEAN DATA TO CONVERT TO DATATYPES - DONE
    # MERGE ALL DATA TOGETHER - CHEROKEE IS YEAR BY YEAR - DONE
    final_dfs = [pd.concat(group) for group in zip(*dfs)]
    # OUTPUT IN PARQUET AND CSV
    filenames = list(filenames)
    for i, df in enumerate(final_dfs):
        df.to_csv(f"{filenames[i]}.csv")  # need file names


main()
