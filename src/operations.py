from functools import reduce
import pandas as pd
import file_utils
from logger import configure_logger, timing

logger = configure_logger()


def concat(data: list[pd.DataFrame]) -> pd.DataFrame:
    """Concats a list of dataframes together

    Args:
        data (list[pd.DataFrame]): dfs to concat

    Returns:
        pd.DataFrame: resulting concatenated df
    """
    return pd.concat(data).reset_index(drop=True)


def join(data: list[pd.DataFrame], key: str, join_type: str) -> pd.DataFrame:
    """Joins a list of dataframes together on a join key

    Args:
        data (list[pd.DataFrame]): dfs to join
        key (str): key, or colum name, to join on
        join_type (str): specify a inner, outer, or left join

    Raises:
        ValueError: when join type is invalid

    Returns:
        pd.DataFrame: resulting joined df
    """
    if join_type not in ["inner", "outer", "left"]:
        raise ValueError("Not a valid join type")

    return reduce(
        lambda left, right: pd.merge(left, right, on=key, how=join_type), data
    )


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
    init_len = len(column)

    num_nulls = column.isnull().sum()
    num_dup = init_len - len(column)
    logger.info(
        f"Cleaning col: {column}; dropped {num_dup} duplicates, filling {num_nulls} nulls"
    )

    match data_type:
        case "str" | "string":
            column = column.fillna("")
        case "int":
            column = column.fillna(0)
            column = column.replace("", 0)
        case "float":
            column = column.fillna(0.0)
            column = column.replace("", 0.0)
        case _:
            raise ValueError(f"Data type {data_type} not supported")

    column = column.astype(data_type)
    return column


def clean_df(df: pd.DataFrame, file_name: str, var_map: None):
    df = df[file_utils.get_desired_cols(file_name, var_map)]
    df = df.drop_duplicates()

    for column in df.columns:
        col_params = file_utils.get_col_params(column, file_name, var_map)
        # can pass in column directly
        df = clean_and_cast_col(df[column], col_params["data_type"])

    df = df.rename(columns={key[0]: var_map[key]["new_name"] for key in var_map.keys()})

    return df


def create_derived_cols(df, file_name):
    df["source"] = file_name
    return df
    # ADDRESS, ETC., NEED VARIABLE MAPPING STUFF HERE
