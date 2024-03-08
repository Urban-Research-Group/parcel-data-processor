import os
import pandas as pd
from src import file_io
from src import operations
from src.logger import configure_logger

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


def create_dfs_from_files(
    file_paths: list[str], format_file: str = None, var_map: pd.DataFrame = None
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

        df = file_io.File(file_path, format_file).read()
        logger.info("Shape of %s when read: %s", file_name, df.shape)
        df = operations.clean_df(df, file_name, var_map)

        logger.info("Shape of %s after processing: %s", file_name, df.shape)
        dfs.append(df)

    return dfs
