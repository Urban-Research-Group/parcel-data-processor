"""File I/O utilities"""

import yaml
import pandas as pd
from logger import configure_logger, timing
import parser

logger = configure_logger()


class File:
    def __init__(self, file_path: str, parser_name=None):
        self.file_path = file_path
        self.parser_name = parser_name

    @timing
    def read(self):
        """Reads a file based on its extension and returns a dataframe"""
        file_type = self.file_path.split(".")[-1].lower()

        # Use UDF read function, if not default
        if self.parser_name.lower() != "default":
            try:
                parser_func = getattr(parser, self.parser_name)
                df = parser_func(self.file_path)
            except AttributeError:
                logger.error(
                    "Parser '%s' not found in src.udf.parser", self.parser_name
                )
                raise ValueError(f"Parser '{self.parser_name}' not found")
            except Exception as e:
                logger.error("Error parsing file: %s", e)
                raise e

            df["source_file"] = self.file_path
            return df

        # Otherwise use default
        match file_type:
            case "csv":
                df = pd.read_csv(self.file_path, encoding="latin-1", low_memory=False)
            case "xlsx" | "xls":
                df = pd.read_excel(self.file_path)
            case "txt":  # TODO: change
                widths, column_headers = self._get_fwf_paramters()
                df = pd.read_fwf(
                    self.file_path, widths=widths, header=None, encoding="latin-1"
                )
                df.columns = column_headers
            case "dat":
                df = pd.read_csv(self.file_path, delimiter="\t", encoding="latin1")
            case _:
                error_msg = f"File type {file_type} not supported in default mode"
                logger.error(error_msg)
                raise ValueError(error_msg)

        df["source_file"] = self.file_path
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
