from functools import reduce
import pandas as pd
from src.logger import configure_logger

logger = configure_logger()


def concat(data: list[pd.DataFrame]) -> pd.DataFrame:
    """Concats a list of dataframes together

    Args:
        data (list[pd.DataFrame]): dfs to concat

    Returns:
        pd.DataFrame: resulting concatenated df
    """
    return pd.concat(data).reset_index(drop=True)


def guard_join(join_type: str) -> None:
    ACCEPTED_TYPES = ["inner", "outer", "left"]

    if join_type.lower() not in ACCEPTED_TYPES:
        error_msg = "Not a valid join type"
        logger.log(error_msg)
        raise ValueError("Not a valid join type")


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
        "Cleaning col: %s; dropped %d duplicates, filling %d nulls",
        column,
        num_dup,
        num_nulls,
    )
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


def clean_df(
    df: pd.DataFrame, file_name: str, var_map: pd.DataFrame = None
) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        file_name (str): _description_
        var_map (pd.DataFrame, optional): _description_. Defaults to None.

    Returns:
        pd.DataFrame: _description_
    """
    file_name = file_name.split(".")[0]
    # Rename all columns which are specified
    var_map = var_map[var_map["old_name"].isin(df.columns)].copy()
    names = zip(var_map["old_name"].tolist(), var_map["new_name"].tolist())
    df = df.rename(columns=dict(names))
    # Drop any columns not in new_name column; we don't need these
    df = df[var_map["new_name"].tolist()]
    for column in df.columns:
        col_params = var_map[var_map["new_name"].str.lower() == column.lower()]
        df[column] = clean_and_cast_col(df[column], col_params["data_type"].item())

    df = df.drop_duplicates()
    return df


def create_derived_cols(
    df: pd.DataFrame, var_map_derived: pd.DataFrame
) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        var_map (pd.DataFrame): _description_

    Raises:
        ValueError: _description_

    Returns:
        pd.DataFrame: _description_
    """
    for _, row in var_map_derived.iterrows():
        columns_to_concat = row["derived"].split(";")

        missing_cols = [col for col in columns_to_concat if col not in df.columns]

        if not missing_cols:
            df[row["new_name"]] = df[columns_to_concat].astype(str).agg("".join, axis=1)
        else:
            error_msg = f"Missing columns in DataFrame for '{row['new_name']}': {', '.join(missing_cols)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    return df


def concat_columns(
    df: pd.DataFrame, columns: list[str], sep: str = " "
) -> pd.DataFrame:
    """Concats a list of dataframes together

    Args:
        data (list[pd.DataFrame]): dfs to concat

    Returns:
        pd.DataFrame: resulting concatenated df
    """

    columns_in_df = df.columns.tolist()
    diff = set(columns).difference(columns_in_df)
    if diff:
        error_msg = f"Columns {diff} not in DataFrame"
        logger.error(error_msg)
        raise ValueError(error_msg)

    return df[columns].agg(sep.join, axis=1)
