import yaml
import numpy as np
import pandas as pd
from src.logger import configure_logger, timing

logger = configure_logger()


class File:
    def __init__(self, file_path: str, format_file=None):
        self.file_path = file_path
        self.format_pattern = format_file

        construct_format_path = None
        if format_file:
            construct_format_path = file_path.split(".")
            construct_format_path.insert(1, format_file)
            construct_format_path = "".join(construct_format_path)
        self.format_file = construct_format_path

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

    def _get_fwf_paramters(self):
        format_file = File._read_format_file(self.format_file)
        widths = File._get_widths_from_format(format_file)
        column_headers = File._get_column_headers_from_format(format_file)
        return widths, column_headers

    @timing
    def read(self):
        """Reads a file based on its extension and returns a dataframe"""
        file_type = self.file_path.split(".")[-1].lower()
        if self.format_pattern and self.format_pattern in self.file_path:
            return

        match file_type:
            case "csv":
                df = pd.read_csv(self.file_path, encoding="latin-1", low_memory=False)
            case "xlsx" | "xls":
                df = pd.read_excel(self.file_path)
            case "txt":
                widths, column_headers = self._get_fwf_paramters()
                df = pd.read_fwf(
                    self.file_path, widths=widths, header=None, encoding="latin-1"
                )
                df.columns = column_headers
            case "dat":
                raise NotImplementedError("File type .dat not supported")
            case _:
                error_msg = f"File type {file_type} not supported"
                logger.error(error_msg)
                raise ValueError(error_msg)

        return df


def read_var_map(var_map_path: str) -> pd.DataFrame:
    try:
        var_map = pd.read_csv(var_map_path)
    except Exception as e:
        msg = f"Could not read config file with error {e}"
        logger.error(msg)
        raise type(e)(msg) from e

    return var_map


def read_config(config_path: str) -> dict:
    try:
        with open(config_path, "r", encoding="utf-8") as config:
            config = yaml.safe_load(config)
    except Exception as e:
        msg = f"Could not read config file with error {e}"
        logger.error(msg)
        raise type(e)(msg) from e

    return config


class WriteOutput:
    def __init__(self, data, county_name, output_path, output_formats):
        self.data = data
        self.county_name = county_name
        self.output_path = output_path
        self.output_formats = output_formats

    @timing
    def write_output(self):
        for output_format in self.output_formats:
            if output_format == "csv":
                self.write_csv()
            elif output_format == "parquet":
                self.write_parquet()
            else:
                logger.warning("%s is not supported!", output_format)

    def write_csv(self):
        try:
            self.data.to_csv(self.output_path + f"{self.county_name}.csv", index=False)
        except Exception as e:
            self.log("CSV", e)

    def write_parquet(self):
        try:
            self.data.to_parquet(
                self.output_path + f"{self.county_name}.parquet", index=False
            )
        except Exception as e:
            self.log("Parquet", e)

    def log(self, file_type, e):
        if e:
            logger.error("Error writing %s output: %s", file_type, e)
        else:
            logger.info("Saved to %s", self.output_path)
