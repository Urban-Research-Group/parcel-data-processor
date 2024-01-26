import yaml
import numpy as np
import pandas as pd
from src.logger import configure_logger, timing

logger = configure_logger()


class File:
    def __init__(self, file_path: str, format_file=None):
        self.file_path = file_path
        self.format_file = format_file

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
                NotImplemented
            case _:
                error_msg = f"File type {file_type} not supported"
                logger.error(error_msg)
                raise ValueError(error_msg)

        return df


def read_var_map(var_map_path: str) -> pd.DataFrame:
    try:
        var_map = pd.read_csv(var_map_path)
    except Exception as e:
        logger.error("Could not read var map file with error %s", e)

    return var_map


def read_config(config_path: str) -> dict:
    try:
        with open(config_path, "r") as config:
            config = yaml.safe_load(config)
    except Exception as e:
        logger.error("Could not read config file with error %s", e)

    return config


class WriteOutput:
    def __init__(self, data, output_path, output_formats):
        self.data = data
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
            self.data.to_csv(self.output_path + ".csv")
        except Exception as e:
            self.log("CSV", e)

    def write_parquet(self):
        try:
            self.data.to_parquet(self.output_path + ".parquet")
        except Exception as e:
            self.log("Parquet", e)

    def log(self, file_type, e):
        if e:
            logger.error("Error writing %s output: %s", file_type, e)
        else:
            logger.info("Saved to %s", self.output_path)
