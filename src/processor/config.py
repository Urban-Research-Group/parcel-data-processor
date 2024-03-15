"""Defines and reads configuaration instruction set for the application"""

from dataclasses import dataclass
from functools import reduce
import pandas as pd
import file_io
import file_utils


@dataclass
class DataInfo:
    """Retains data parameters with processing instructions"""

    name: str
    root_path: str
    operations: dict
    retain: list
    output: dict
    files: list = None
    var_map_non_derived: pd.DataFrame = None
    var_map_derived: pd.DataFrame = None


def separate_var_map(var_map: pd.DataFrame) -> tuple:
    derived_mask = var_map["old_name"].apply(lambda x: ";" in str(x))
    var_map_non_derived = var_map[~derived_mask]
    var_map_derived = var_map[derived_mask]

    return var_map_non_derived, var_map_derived


def get_retain_ids(operations: dict) -> list:
    op_names = list(operations.keys())
    file_patterns = [list(op["files"].keys()) for op in operations.values()]
    file_patterns = reduce(lambda x, y: x + y, file_patterns, [])
    retain = [op_name for op_name in file_patterns if op_name in op_names] + [
        op_names[-1]
    ]
    return retain


def create_data_info(config_path: str) -> DataInfo:
    """Reads YAML config instruction set and initalizes a
    data_info dataclass with values from the config.

    Args:
        file_path (str): path to config YAML

    Returns:
        data_info: dataclass containing config parameters
    """
    config = file_io.read_config(config_path)

    root_path = config["root-path"]
    var_map_path = config["var-map-path"]

    files = file_utils.get_all_files_in_dir(root_path)
    var_map = file_io.read_var_map(var_map_path)

    var_map_non_derived, var_map_derived = separate_var_map(var_map)

    operations = config["operations"]
    retain = get_retain_ids(operations)

    return DataInfo(
        name=config["name"],
        root_path=root_path,
        operations=operations,
        retain=retain,
        output=config["output"],
        files=files,
        var_map_non_derived=var_map_non_derived,
        var_map_derived=var_map_derived,
    )
