import os
import pandas as pd
import numpy as np


@staticmethod
def _read_format_file(path):
    return np.loadtxt(path, dtype=str)


@staticmethod
def _get_widths_from_format(format_file: np.array):
    return format_file[:, 2].astype(np.int16).tolist()


@staticmethod
def _get_column_headers_from_format(format_file: np.array):
    header_col = format_file[:, 0]
    return header_col


def _get_fwf_paramters(format_file: str):
    format_file = _read_format_file(format_file)
    widths = _get_widths_from_format(format_file)
    column_headers = _get_column_headers_from_format(format_file)
    return widths, column_headers


def read_file(directory, file, format_file=None):
    """Reads file, returns DF"""
    if file.endswith(".csv"):
        df = pd.read_csv(os.path.join(directory, file))
    elif file.endswith(".txt"):
        widths, column_headers = _get_fwf_paramters()
        df = pd.read_fwf(self.file_path, widths=widths, header=None, encoding="latin-1")
        df = pd.read_csv(os.path.join(TEST_DATA_DIR, file), sep="|")
    else:
        raise ValueError(f"File {file} is not a CSV or TXT file.")

    return df
