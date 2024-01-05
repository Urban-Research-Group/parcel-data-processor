import polars as pl
import pandas as pd
import os
import logging
from simpledbf import Dbf5

NUMERIC_TYPES = ["Int16", "Int32", "Int64", "Float16", "Float32", "Float64"]


def read_data(data_format, col_format: pl.DataFrame):
    """Reads data from multiple file formats and selects only required columns"""
    curr_col_names = col_format["old_name"].to_list()
    file_readers = {
        "csv": lambda f: _read_csv(f, curr_col_names, data_format.separator),
        "xlsx": lambda f: _read_excel(f, curr_col_names),
        "xls": lambda f: _read_excel(f, curr_col_names),
        "parquet": lambda f: _read_parquet(f, curr_col_names),
        "dbf": lambda f: _read_dbf(f, curr_col_names),
    }

    dfs = []

    for file in os.listdir(data_format.files_path):
        file_ext = file.split(".")[-1]

        reader = file_readers[file_ext]
        if reader:
            dfs.append(reader(data_format.files_path + r"/" + file))
            logging.info("Successfully read: %s", file)
        else:
            logging.error("Could not read: %s", file)

    return dfs


def _read_csv(file, cols, sep):
    print(cols)
    return pl.read_csv(file, separator=sep, columns=cols, infer_schema_length=0)


def _read_excel(file, cols):
    return pl.from_pandas(pd.read_excel(file, usecols=cols))


def _read_parquet(file, cols):
    return pl.read_parquet(file, columns=cols)


def _read_dbf(file, cols):
    return pl.from_pandas(Dbf5(file).to_dataframe()).select(cols)


def rename_col(dfs, mapping):
    """Renames columns given a mapping for all dataframes passed in"""
    new_dfs = []
    for df in dfs:
        new_dfs += df.rename(mapping)
    return new_dfs


def merge_cols(dfs, merge_key):
    """Merges columns on a given key for all dataframes passed in"""
    raise NotImplementedError


def append_cols(dfs):
    """Appends all dataframes passed in"""
    return pl.concat(dfs)


def replace_null_zero(df, new_col_dtypes):
    """Replaces"""
    for col in new_col_dtypes:
        if col in NUMERIC_TYPES:
            df[col] = df[col].fill_na("0")
    return df


def replace_null_str(df, new_col_dtypes):
    """Replaces"""
    for col in new_col_dtypes:
        if col in ["String"]:
            df[col] = df[col].fill_na("")
    return df


def create_site_addr(df, cols):
    """Creates a new column"""
    df["site_addr"] = pl.concat_str(
        [pl.col(col) + pl.lit(" ") for col in cols]
    ).str.replace("  ", " ")
    return df


def cast_columns(df, new_col_dtypes):
    """Casts"""
    for col in new_col_dtypes.keys():
        df[col] = df.select(pl.col(col).cast(new_col_dtypes[col]))
    return df


def export_to_csv(df, out_file):
    """Exports to CSV"""
    df.write_csv(out_file + ".csv")


def export_to_parquet(df, out_file):
    """Exports to Parquet"""
    df.write_parquet(out_file + ".parquet")
