from functools import reduce
import pandas as pd
import file_utils
from logger import configure_logger, timing

logger = configure_logger()


def append(data):
    return pd.concat(data).reset_index(drop=True)


def merge(data, key, merge_type):
    return reduce(
        lambda left, right: pd.merge(left, right, on=key, how=merge_type), data
    )


def clean_and_cast_column(df, column, data_type):
    init_len = len(df)
    df = df.drop_duplicates()

    num_nulls = df[column].isnull().sum()
    num_dup = init_len - len(df)
    logger.info(
        f"Cleaning col: {column}; dropped {num_dup} duplicates, filling {num_nulls} nulls"
    )

    match data_type:
        case "str" | "string":
            df[column] = df[column].fillna("")
        case "int":
            df[column] = df[column].fillna(0)
            df[column] = df[column].replace("", 0)
        case "float":
            df[column] = df[column].fillna(0.0)
            df[column] = df[column].replace("", 0.0)
        case _:
            raise ValueError(f"Data type {data_type} not supported")

    df[column] = df[column].astype(data_type)
    return df


def clean_df(df, file_name, var_map):
    df = df[file_utils.get_desired_cols(file_name, var_map)]

    for column in df.columns:
        col_params = file_utils.get_col_params(column, file_name, var_map)
        df = clean_and_cast_column(df, column, col_params["data_type"])

    df = df.rename(columns={key[0]: var_map[key]["new_name"] for key in var_map.keys()})

    return df


def create_derived_cols(df, file_name):
    df["source"] = file_name
    return df
    # ADDRESS, ETC.
