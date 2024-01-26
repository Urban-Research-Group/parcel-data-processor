from dataclasses import dataclass
import pandas as pd
from src import file_io
from src import file_utils


# TODO: check YAML formatting
@dataclass
class DataInfo:
    """Retains data parameters for processing instructions"""

    county_name: str
    county_path: str
    var_map_path: str
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
    county_path = config["county-path"]
    var_map_path = config["var-map-path"]

    county_files = file_utils.get_all_files_in_dir(county_path)
    var_map = file_io.read_var_map(var_map_path)

    derived_mask = var_map["derived"].str.lower().eq("false")
    var_map_non_derived = var_map[~derived_mask]
    var_map_derived = var_map[derived_mask]

    return DataInfo(
        county_name=config["county-name"],
        county_path=county_path,
        var_map_path=var_map_path,
        operations=config["operations"],
        output=config["output"],
        county_files=county_files,
        var_map_non_derived=var_map_non_derived,
        var_map_derived=var_map_derived,
    )
