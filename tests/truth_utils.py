import os
import pandas as pd
import numpy as np


def _read_format_file(path):
    return np.loadtxt(path, dtype=str)


def _get_widths_from_format(format_file: np.array):
    return format_file[:, 2].astype(np.int16).tolist()


def _get_column_headers_from_format(format_file: np.array):
    header_col = format_file[:, 0]
    return header_col


def _get_fwf_paramters(format_file: str):
    format_file = _read_format_file(format_file)
    widths = _get_widths_from_format(format_file)
    column_headers = _get_column_headers_from_format(format_file)
    return widths, column_headers


def clean_and_cast_col(column: pd.Series, data_type: str) -> pd.Series:
    """Fills nulls and re-casts a given DataFrame column

    Args:
        column (pd.Series): column to clean
        data_type (str): data type to cast column to

    Raises:
        ValueError: when specified data type is not supported

    Returns:
        pd.Series: resulting cleaned column
    """
    match data_type:
        case "str" | "string":
            column = column.fillna("")
        case "int" | "integer":
            data_type = "int"
            column = column.fillna(0)
            column = column.replace("", 0)
        case "float":
            column = column.fillna(0.0)
            column = column.replace("", 0.0)
        case _:
            raise ValueError(f"Data type {data_type} not supported")

    column = column.astype(data_type)
    return column


def read_file(directory, file, var_map, format_file=None):
    """Reads file, returns DF"""
    if format_file and format_file in file:
        return

    if file.endswith(".csv"):
        df = pd.read_csv(os.path.join(directory, file))
    elif file.endswith(".txt"):
        construct_format = file.split(".")
        construct_format.insert(1, format_file)
        construct_format = "".join(construct_format)
        widths, column_headers = _get_fwf_paramters(
            os.path.join(directory, construct_format)
        )
        df = pd.read_fwf(
            os.path.join(directory, file),
            widths=widths,
            header=None,
            encoding="latin-1",
        )
        df.columns = column_headers
    else:
        raise ValueError(f"File {file} is not a CSV or TXT file.")

    # Clean and rename df
    for col in df.columns:
        df[col] = clean_and_cast_col(
            df[col], var_map[var_map["old_name"] == col]["data_type"].item()
        )
    names = zip(var_map["old_name"].tolist(), var_map["new_name"].tolist())
    df = df.rename(columns=dict(names))

    return df
