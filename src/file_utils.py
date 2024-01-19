import os
import pandas as pd
import numpy as np
from logger import configure_logger, timing


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
    var_map = pd.read_csv(var_map_path)
    # we want a dict with (old, source): {new var name: val, type: val, source: val}
    var_dict = {
        (row["old_name"], row["source_file"]): {
            "new_name": row["new_name"],
            "data_type": row["data_type"],
        }
        for _, row in var_map.iterrows()
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


def select_files(file_paths, key):
    """
    Returns a list of files matching the pattern
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


def get_desired_cols(file_name: str, var_map: dict):
    return [
        key[0]
        for key in var_map.keys()
        if key[1].lower() == "all" or file_name in key[1]
    ]


def get_col_params(column: str, source: str, var_map: dict):
    params = None

    for key in var_map.keys():
        if key[0] == column and (source in key[1] or key[1].lower() == "all"):
            params = var_map[key]

    return params
    # TODO: might need to convert data type strings
    # match params["data_type"]:
    #    case "str":
    #        pd.


def create_dfs_from_files(file_paths: list[str], format_file: str, var_map: dict):
    """
    Returns a list of dataframes created from the files
    """
    dfs = []
    for file_path in file_paths:
        logger = configure_logger()
        file_name = file_path.split("\\")[-1]
        df = create_df_from_file(file_path, format_file)
        logger.info(f"Shape of {file_name} when read: {df.shape}")

        df["source"] = file_name
        # need to exclude derived or none columns- only matching source
        print(file_name)
        df = df[get_desired_cols(file_name, var_map)]
        print(df)
        # fill nulls with empty before cast due to problems
        for column in df.columns:
            col_params = get_col_params(column, file_name, var_map)
            # LOG HOW MANY FILLED

            if col_params["data_type"] in ["str", "string"]:
                df[column] = df[column].fillna("")
            elif col_params["data_type"] == "int":
                df[column] = df[column].fillna(0)
                df[column] = df[column].replace("", 0)
            elif col_params["data_type"] == "float":
                df[column] = df[column].fillna(0.0)
                df[column] = df[column].replace("", 0.0)
            else:
                raise ValueError(f"Data type {col_params['data_type']} not supported")

            df[column] = df[column].astype(col_params["data_type"])
        df.rename(columns={key[0]: var_map[key]["new_name"] for key in var_map.keys()})
        logger.info(f"Shape of {file_name} after processing: {df.shape}")
        dfs.append(df)
        # TODO: add try catch logic
        # TODO: add more file types
        # TODO: add checkpoints (pickle output)

    return dfs
