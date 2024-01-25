import os
import pandas as pd
import numpy as np
from src import operations
from src.logger import configure_logger, timing

logger = configure_logger()


def _read_format_file(path):
    return np.loadtxt(path, dtype=str)


def _get_widths_from_format(format_file: np.array):
    return format_file[:, 2].astype(np.int16).tolist()


def _get_column_headers_from_format(format_file: np.array):
    header_col = format_file[:, 0]
    return header_col


def _get_fwf_paramters(format_file_path):
    format_file = _read_format_file(format_file_path)
    widths = _get_widths_from_format(format_file)
    column_headers = _get_column_headers_from_format(format_file)
    return widths, column_headers


def read_var_map(var_map_path: str) -> dict[str, str]:
    """_summary_

    Args:
        var_map_path (str): _description_

    Returns:
        dict[str, str]: _description_
    """
    # TODO: derived variables?
    var_map = pd.read_csv(var_map_path)
    # we want a dict with (old, source): {new var name: val, type: val, source: val}
    var_dict = {
        (row["old_name"], row["source_file"]): {
            "new_name": row["new_name"],
            "data_type": row["data_type"],
        }
        for _, row in var_map.iterrows()
        if row["derived"] == "false"
    }
    return var_dict


def get_all_files_in_dir(directory: str) -> list[str]:
    """Returns a list of all files in a directory and its subdirectories

    Args:
        directory (str): path of top-level directory to walk

    Returns:
        list[str]: list of all files in directory and subdirectories
    """
    return [
        os.path.join(dirpath, file)
        for dirpath, _, filenames in os.walk(directory)
        for file in filenames
    ]


def select_files(file_paths: list[str], key: str) -> list[str]:
    """Returns a list of files matching the pattern (currently substring),
    formerly depreciated method used Regex

    Args:
        file_paths (list[str]): list of file paths to search
        key (str): pattern to match (currently substring)

    Returns:
        list[str]: list of file paths matching the pattern
    """
    return [file_path for file_path in file_paths if key in file_path]


@timing
def create_df_from_file(file_path: str, format_file=None) -> pd.DataFrame:
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
    # TODO add and validate file types
    file_type = file_path.split(".")[-1].lower()

    match file_type:
        case "csv":
            df = pd.read_csv(file_path, encoding="latin-1", low_memory=False)
        case "xlsx" | "xls":
            df = pd.read_excel(file_path)
        case "txt":
            widths, column_headers = _get_fwf_paramters(format_file)
            df = pd.read_fwf(file_path, widths=widths, header=None, encoding="latin-1")
            df.columns = column_headers
        case "dat":
            NotImplemented
        case _:
            raise ValueError(f"File type {file_type} not supported")

    return df


def get_desired_cols(file_name: str, var_map: dict) -> list[str]:
    """Returns a list of all original column names that are desired

    Args:
        file_name (str): _description_
        var_map (dict): _description_

    Returns:
        list[str]: _description_
    """
    return [
        key[0]
        for key in var_map.keys()
        if key[1].lower() == "all" or file_name in key[1]
    ]


def get_col_params(column: str, source: str, var_map: dict) -> dict:
    """_summary_

    Args:
        column (str): _description_
        source (str): _description_
        var_map (dict): _description_

    Returns:
        dict: _description_
    """
    # TODO: might want to convert data type strings to actual types here
    params = None

    for key in var_map.keys():
        if key[0] == column and (source in key[1] or key[1].lower() == "all"):
            params = var_map[key]

    return params


def create_dfs_from_files(
    file_paths: list[str], format_file: str = None, var_map: dict = None
) -> list[pd.DataFrame]:
    """Reads in a file as a DataFrame and performs data cleaning operations for
    each file in file_paths

    Args:
        file_paths (list[str]): list of files to read
        format_file (str): pattern to find format files for each data file, if needed
        var_map (dict): dict of old and new variable names and data types

    Raises:
        ValueError: if file type is not supported for read

    Returns:
        list[pd.DataFrame]: list of dataframes created from reading each file in file_paths
    """
    # TODO: add checkpoints (pickle output)
    dfs = []

    for file_path in file_paths:
        file_name = file_path.split("\\")[-1]

        df = create_df_from_file(file_path, format_file)
        logger.info(f"Shape of {file_name} when read: {df.shape}")

        df = operations.clean_df(df, file_name, var_map)
        df = operations.create_derived_cols(df, file_name)

        logger.info(f"Shape of {file_name} after processing: {df.shape}")
        dfs.append(df)

    return dfs
