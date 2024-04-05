import pandas as pd
from file_io import File


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

    column = column.astype(data_type)
    return column


def read_file(
    path: str,
    parser: str,
    var_map: pd.DataFrame,
) -> pd.DataFrame:
    """Reads file, returns DF"""
    df = File(path, parser).read()
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
