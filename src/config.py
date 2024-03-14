"""Defines and reads configuaration instruction set for the application"""

from dataclasses import dataclass
import pandas as pd
import file_io
import file_utils


@dataclass
class DataInfo:
    """Retains data parameters with processing instructions"""

    name: str
    root_path: str
    operations: dict
    output: dict
    county_files: list = None
    var_map_non_derived: pd.DataFrame = None
    var_map_derived: pd.DataFrame = None


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

    derived_mask = var_map["source_file"].apply(lambda x: str(x).contains(";"))
    var_map_non_derived = var_map[~derived_mask]
    var_map_derived = var_map[derived_mask]

    operations = config["operations"]
    op_names = set(operations.keys())
    file_patterns = set(operation["files"].keys() for operation in operations)
    retain = op_names.intersection(file_patterns)

    print(retain)
    return DataInfo(
        name=config["name"],
        root_path=root_path,
        operations=config["operations"],
        retain=retain,
        output=config["output"],
        files=files,
        var_map_non_derived=var_map_non_derived,
        var_map_derived=var_map_derived,
    )
