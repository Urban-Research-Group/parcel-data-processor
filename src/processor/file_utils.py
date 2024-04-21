import os
import re
import pandas as pd
import file_io
from logger import configure_logger

logger = configure_logger()


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


def select_files(file_paths: list[str], keys: tuple[str]) -> list[str]:
    """Returns a list of files matching the pattern (currently substring),
    formerly depreciated method used Regex

    Args:
        file_paths (list[str]): list of file paths to search
        key tuple[str]: pattern to match (currently substring) [group, pattern]

    Returns:
        list[str]: list of file paths matching the pattern
    """
    if keys[0]:
        return [
            file_path
            for file_path in file_paths
            if all(re.search(key, file_path) for key in keys)
        ]
    else:
        return [file_path for file_path in file_paths if re.search(keys[1], file_path)]


def create_dfs_from_files(
    file_paths: list[str],
    var_map: pd.DataFrame = None,
    parser: str = None,
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
    dfs = []

    for file_path in file_paths:
        df = file_io.File(file_path, parser).read()
        logger.info("Shape of %s when read: %s", file_path, df.shape)
        rename_dict = dict(
            zip(var_map["old_name"].tolist(), var_map["new_name"].tolist())
        )
        df = df.rename(columns=rename_dict)
        dfs.append(df)

    return dfs
