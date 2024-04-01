import pandas as pd
import parser_utils as pu


def cobb(file_path: str) -> pd.DataFrame:
    # word doc for DAT
    NotImplemented


def cherokee(file_path: str) -> pd.DataFrame:
    format_file_pattern = "_fdf."
    format_path = file_path.split(".")
    format_path.insert(1, format_file_pattern)
    format_path = "".join(format_path)

    widths, column_headers = pu._get_fwf_paramters(format_path)
    df = pd.read_fwf(file_path, widths=widths, header=None, encoding="latin-1")
    df.columns = column_headers
    return df


def paulding_2010(file_path: str) -> pd.DataFrame:
    NotImplemented


def paulding_2011_2012(file_path: str) -> pd.DataFrame:
    NotImplemented
