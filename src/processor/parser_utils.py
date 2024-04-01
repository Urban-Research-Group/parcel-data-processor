import numpy as np


def _read_format_file(path):
    return np.loadtxt(path, dtype=str)


def _get_widths_from_format(format_file: np.array):
    return format_file[:, 2].astype(np.int16).tolist()


def _get_column_headers_from_format(format_file: np.array):
    header_col = format_file[:, 0]
    return header_col


def _get_fwf_paramters(format_path: str):
    format_file = _read_format_file(format_path)
    widths = _get_widths_from_format(format_file)
    column_headers = _get_column_headers_from_format(format_file)
    return widths, column_headers
