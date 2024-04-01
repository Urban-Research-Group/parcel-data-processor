"""Defines and reads configuaration instruction set for the application"""

from dataclasses import dataclass
from functools import reduce
from schema import Schema, Optional, Or, SchemaError
import pandas as pd
import file_io
import file_utils
from logger import configure_logger

logger = configure_logger()

OPERATIONS_REQUIRING_KEY = ["merge", "join"]
REQUIRED_VAR_MAP_COLUMNS = set(["old_name", "new_name", "data_type", "source_file"])
CONFIG_SCHEMA = Schema(
    {
        "name": str,
        "root-path": str,
        "var-map-path": str,
        "operations": {
            str: {
                "type": Or("append", "merge", "join"),
                Optional("groups"): [str],
                "files": {str: str},
                Optional("key"): [str],
                Optional("join-type"): str,
            }
        },
        "output": {"path": str, "formats": [str]},
    }
)


@dataclass
class DataInfo:
    """Retains data parameters with processing instructions"""

    name: str
    root_path: str
    operations: dict
    retain: list
    output: dict
    files: list
    parser: str = None
    var_map_non_derived: pd.DataFrame = None
    var_map_derived: pd.DataFrame = None


def _separate_var_map(var_map: pd.DataFrame) -> tuple:
    derived_mask = var_map["old_name"].apply(lambda x: ";" in str(x))
    var_map_non_derived = var_map[~derived_mask]
    var_map_derived = var_map[derived_mask]

    return var_map_non_derived, var_map_derived


def _get_retain_ids(operations: dict) -> list:
    """Returns list of operation results also used in later operations- e.g. data we want to retain"""
    op_names = list(operations.keys())
    # Get file patterns for each operation
    file_patterns = [list(op["files"].keys()) for op in operations.values()]
    # Flatten list
    file_patterns = reduce(lambda x, y: x + y, file_patterns, [])
    # Retain file pattern if it matches name of later operation
    retain = [op_name for op_name in file_patterns if op_name in op_names] + [
        op_names[-1]
    ]
    return retain


def _validate_config(config: dict) -> None:
    try:
        CONFIG_SCHEMA.validate(config)
    except SchemaError as e:
        logger.error("Config schema error: ", e)
        raise e

    for operation in config["operations"].values():
        if (operation["type"] in OPERATIONS_REQUIRING_KEY) & (not operation.get("key")):
            raise SchemaError("Merge or join operation requires key")


def _validate_var_map(var_map: pd.DataFrame) -> None:
    column_diff = REQUIRED_VAR_MAP_COLUMNS.difference(var_map.columns)
    if column_diff:
        raise SchemaError("Var map must contain column(s): ", column_diff)


def create_data_info(config_path: str) -> DataInfo:
    """Reads YAML config instruction set and initalizes a
    data_info dataclass with values from the config.

    Args:
        file_path (str): path to config YAML

    Returns:
        data_info: dataclass containing config parameters
    """
    config = file_io.read_config(config_path)
    _validate_config(config)

    root_path = config["root-path"]
    files = file_utils.get_all_files_in_dir(root_path)

    var_map_path = config["var-map-path"]
    var_map = file_io.read_var_map(var_map_path)
    _validate_var_map(var_map)

    var_map_non_derived, var_map_derived = _separate_var_map(var_map)

    operations = config["operations"]
    retain = _get_retain_ids(operations)
    parser = config.get("parser")

    return DataInfo(
        name=config["name"],
        root_path=root_path,
        operations=operations,
        retain=retain,
        output=config["output"],
        files=files,
        parser=parser,
        var_map_non_derived=var_map_non_derived,
        var_map_derived=var_map_derived,
    )
