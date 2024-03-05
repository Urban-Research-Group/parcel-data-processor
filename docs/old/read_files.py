import os
import re
import pandas as pd
import numpy as np

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

# TODO
def get_all_files_in_dir(directory: str) -> list[str]:
    """Returns a list of all files in a directory and its subdirectories

    Args:
        directory (str): path of top-level directory to walk

    Returns:
        list[str]: list of all files in directory and subdirectories
    """
    return [os.path.join(dirpath, file) 
            for dirpath, _, filenames in os.walk(directory) 
            for file in filenames]

def get_files_for_county(county_name: str, file_pattern: str) -> list[str]:
    """Returns a list of files matching the pattern in the county directory

    Args:
        county_name (str): name of directory (usually county name)
        file_pattern (str): regex expression

    Returns:
        list[str]: list of county files matching the pattern
    """
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    return [
        path
        for path in get_all_files_in_dir(directory=f"{curr_dir}/../data/{county_name}")
        if re.search(file_pattern, path)
    ]

def create_df_from_file(file_path: str, format_file = None) -> pd.DataFrame:
    """Reads a file based on its extension and returns a dataframe

    Args:
        file_path (str): path to file
        format_file_pattern (str, optional): regex pattern for format file, if needed.
            Defaults to None.

    Raises:
        ValueError: if file type is not supported

    Returns:
        pd.DataFrame: dataframe of file
    """
    print(f"Reading {file_path}")
    file_type = file_path.split(".")[-1].lower()

    match file_type:
        case "csv":
            df = pd.read_csv(file_path, encoding="latin-1", low_memory=False)
        case "xlsx" | "xls":
            df = pd.read_excel(file_path, encoding="latin-1", low_memory=False)
        case "txt":
            widths, column_headers = _get_fwf_paramters(file_path)
            df = pd.read_fwf(file_path, widths=widths, header=None, encoding="latin-1")
            df.columns = column_headers
        case "dat":
            NotImplemented
        case _:
            raise ValueError(f"File type {file_type} not supported")

    return df

def create_dfs_from_files(file_paths: list[str], format_file):
    """
    Returns a list of dataframes created from the files
    """
    # add source column in here
    dfs = []
    for file_path in file_paths:
        print("FILE PATH", file_path)
        df = create_df_from_file(file_path)
        df['source'] = file_path
        dfs.append(df)
        # TODO: add try catch logic
        # TODO: add more file types
        # TODO: add checkpoints
        # TODO: add pickle output

    return dfs