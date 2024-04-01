from functools import reduce
import pandas as pd
from logger import configure_logger

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
        lambda left, right: pd.merge(
            left, right, on=key, how=join_type, suffixes=("1", "2")
        ),
        data,
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

    match data_type.lower():
        case "str" | "string":
            column = column.fillna("")
        case "int" | "integer":
            data_type = "int"
            column = column.fillna(0)
            column = column.apply(pd.to_numeric, errors="coerce")
            column = column.fillna(-1)
        case "float":
            column = column.fillna(0)
            column = column.apply(pd.to_numeric, errors="coerce")
            column = column.fillna(-1)
        case _:
            raise ValueError(f"Data type {data_type} not supported")

    logger.info(
        "Cleaning col: %s; dropped %d duplicates, filling %d nulls",
        column,
        num_dup,
        num_nulls,
    )
    column = column.astype(data_type)
    return column


def clean_df(df: pd.DataFrame, var_map: pd.DataFrame = None) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        var_map (pd.DataFrame, optional): _description_. Defaults to None.

    Returns:
        pd.DataFrame: _description_
    """
    new_names = set(
        var_map["new_name"].tolist()
        + [
            "source_file",
            "source_file_1",
            "source_file_2",
        ]
    )
    new_names = list(new_names.intersection(df.columns))

    df = df[new_names]
    for column in df.columns:
        # skip added source_file column
        if "source_file" in column:
            continue
        col_params = var_map[var_map["new_name"].str.lower() == column.lower()]
        df.loc[:, column] = clean_and_cast_col(
            df[column], col_params["data_type"].item()
        )
    df = df.drop_duplicates()
    return df


def create_derived_cols(
    df: pd.DataFrame, var_map_derived: pd.DataFrame, sep: str = " "
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
        columns_to_concat = row["old_name"].split(";")

        if set(columns_to_concat).difference(df.columns):
            logger.warning(f"Columns {columns_to_concat} not in DataFrame")
            continue

        df[columns_to_concat] = df[columns_to_concat].astype(str).fillna("")
        df[row["new_name"]] = df[columns_to_concat].agg(sep.join, axis=1)

    return df
