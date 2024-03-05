import os

import pandas as pd
import numpy as np
import polars as pl


def get_column_headers(format_file: np.array):
    header_col = format_file[:, 0]
    return header_col


def get_format_tuples(format_file: np.array):
    """DEPRECIATED"""
    index_col = (format_file[:, 2]).astype(np.int16)
    cumsum = np.cumsum(index_col)
    start_i = np.insert(cumsum, 0, 0)[:-1]
    np_2d = np.stack((start_i, cumsum), axis=1)
    return [tuple(row) for row in np_2d]


def get_widths(format_file: np.array):
    return format_file[:, 2].astype(np.int16).tolist()


def read_format_file(PATH):
    return np.loadtxt(PATH, dtype=str)


def read_data_colspec(PATH, columns):
    """DEPRECIATED"""
    return pd.read_fwf(PATH, colspecs=columns, header=None)


def read_data(PATH, widths):
    return pd.read_fwf(PATH, widths=widths, header=None)


DATA_PATH = r"C:\Users\nicho\Documents\research\ga-tax-assessment\data\cherokee\digest2020-Cherokee\d_real.txt"
FORMAT_PATH = r"C:\Users\nicho\Documents\research\ga-tax-assessment\data\cherokee\digest2020-Cherokee\d_real_fdf.txt"

DATA_PATH_ALL = r"C:\Users\nicho\Documents\research\ga-tax-assessment\data\cherokee\digest2020-Cherokee\\"


format_file = read_format_file(FORMAT_PATH)
column_headers = get_column_headers(format_file)


    pl.read_csv("./cherokee_cols_2020-2022.csv")[["old_name", "new_name"]].iter_rows()
)
DTYPE_MAP = dict(
    pl.read_csv("./cherokee_cols_2020-2022.csv")[["new_name", "data_type"]].iter_rows()
)
print(COL_MAP)
print(DTYPE_MAP)
for file in os.listdir(DATA_PATH_ALL):
    if "txt" in file and "fdf" not in file:
        print(file)

# make list of needed files and variables
#
# loop through OS

# old_name,new_name
