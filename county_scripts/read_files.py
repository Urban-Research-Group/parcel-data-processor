import os
import re
import pandas as pd
import numpy as np


def _get_all_files_in_dir(directory="."):
    """
    Returns a list of all files in the directory and its subdirectories
    """
    files = []
    for dirpath, dirnames, filenames in os.walk(directory):
        for file in filenames:
            files.append(os.path.join(dirpath, file))
    return files


def _get_format_path(file):
    format_path = (file).split(".")
    format_path.insert(1, "_fdf.")
    format_path = "".join(format_path)
    return format_path


def _read_format_file(path):
    return np.loadtxt(path, dtype=str)


def _get_widths_from_format(format_file: np.array):
    return format_file[:, 2].astype(np.int16).tolist()


def _get_column_headers_from_format(format_file: np.array):
    header_col = format_file[:, 0]
    return header_col


def _get_fwf_paramters(file):
    format_file = _read_format_file(_get_format_path(file))
    widths = _get_widths_from_format(format_file)
    column_headers = _get_column_headers_from_format(format_file)
    return widths, column_headers


def _create_df_from_file(file):
    file_type = file.split(".")[-1].lower()

    match file_type:
        case "csv":
            df = pd.read_csv(file, encoding="latin-1", low_memory=False)
        case "xlsx":
            df = pd.read_excel(file, encoding="latin-1", low_memory=False)
        case "txt":
            widths, column_headers = _get_fwf_paramters(file)
            df = pd.read_fwf(file, widths=widths, header=None, encoding="latin-1")
            df.columns = column_headers
        case "dat":
            NotImplemented
        case _:
            raise ValueError(f"File type {file_type} not supported")

    return df


def get_files_for_county(county_name, file_pattern):
    """
    Returns a list of all files in the county's directory
    """
    file_dir = os.path.dirname(os.path.abspath(__file__))
    return [
        file
        for file in _get_all_files_in_dir(directory=f"{file_dir}/../data/{county_name}")
        if re.search(file_pattern, file)
    ]


def create_dfs_from_files(files, foramtting_file):
    """
    Returns a list of dataframes created from the files
    """
    dfs = {}

    for file in files:
        df = _create_df_from_file(file)
        dfs[file] = df
        # TODO: add try catch logic
        # TODO: add more file types
        # TODO: add checkpoints
        # TODO: add pickle output

    return dfs


# we get files for county, selecting for root dir and pattern
# we create dfs from selected files in a dict: {file: df} DONT NEED TO DO THIS YET
#

# get files for county
# get merge and append lists

# read in files from list
# remove uneeded vars
