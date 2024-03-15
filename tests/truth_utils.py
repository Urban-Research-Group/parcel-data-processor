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


def read_file(path: str, var_map: pd.DataFrame, format_pat: str = None) -> pd.DataFrame:
    """Reads file, returns DF"""
    if format_pat and format_pat in path:
        return

    if path.endswith(".csv"):
        df = pd.read_csv(path)
    elif path.endswith(".txt"):
        construct_format = path.split(".")
        construct_format.insert(1, format_pat)
        construct_format = "".join(construct_format)
        widths, column_headers = _get_fwf_paramters(construct_format)
        df = pd.read_fwf(
            path,
            widths=widths,
            header=None,
            encoding="latin-1",
        )
        df.columns = column_headers
    else:
        raise ValueError(f"File {path} is not a CSV or TXT file.")

    rename_dict = dict(zip(var_map["old_name"].tolist(), var_map["new_name"].tolist()))
    df = df.rename(columns=rename_dict)
    return df


def clean_df(df: pd.DataFrame, var_map: pd.DataFrame = None) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        var_map (pd.DataFrame, optional): _description_. Defaults to None.

    Returns:
        pd.DataFrame: _description_
    """
    new_names = set(var_map["new_name"].tolist())
    new_names = list(new_names.intersection(df.columns))

    df = df[new_names]
    for column in df.columns:
        col_params = var_map[var_map["new_name"].str.lower() == column.lower()]
        df[column] = clean_and_cast_col(df[column], col_params["data_type"].item())

    df = df.drop_duplicates()
    return df


def create_derived_vars(
    df: pd.DataFrame, var_map_derived: pd.DataFrame, sep: str = " "
) -> pd.DataFrame:
    """Creates derived columns from a given DataFrame"""
    for _, row in var_map_derived.iterrows():
        columns_to_concat = row["old_name"].split(";")

        if set(columns_to_concat).difference(df.columns):
            continue

        df[columns_to_concat] = df[columns_to_concat].astype(str).fillna("")
        df[row["new_name"]] = df[columns_to_concat].agg(sep.join, axis=1)

    return df
