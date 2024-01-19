import pandas as pd
import argparse
from dataclasses import dataclass
import yaml
import operations
import file_utils
from logger import configure_logger, timing


@dataclass
class data_info:
    """Retains data parameters for processing instructions"""

    county_name: str
    county_path: str
    var_map_path: str
    operations: dict
    output: dict
    county_files: list = None
    var_map: str = None


def read_config(file_path: str) -> data_info:
    """Reads YAML config instruction set and initalizes a
    data_info dataclass with values from the config.

    Args:
        file_path (str): path to config YAML

    Returns:
        data_info: dataclass containing config parameters
    """
    with open(file_path, "r") as config:
        config = yaml.safe_load(config)

    return data_info(
        county_name=config["county-name"],
        county_path=config["county-path"],
        var_map_path=config["var-map-path"],
        operations=config["operations"],
        output=config["output"],
    )


@timing
def load_data(
    data_info: data_info, file_pattern: str, format_file: str
) -> list[pd.DataFrame]:
    """Selects required files and calls on file_utils to complete I/O read

    Args:
        data_info (data_info): dataclass providing data instruction parameters
        file_pattern (str): selects files matching this pattern in given dir path
        format_file (str): pattern to identify format files for each data file, if needed

    Returns:
        list[pd.DataFrame]: list of dataframes created by reading in each file specified
    """
    county_files = data_info.county_files
    var_map = data_info.var_map
    selected_files = file_utils.select_files(county_files, file_pattern)

    return file_utils.create_dfs_from_files(selected_files, format_file, var_map)


@timing
def process_operation(operation, data) -> pd.DataFrame:
    """Executes a single data operation as specified by the data info params

    Args:
        operation (dict): operation params from data info params
        data (list[pd.DataFrame]): data to execute the operation on

    Raises:
        ValueError: if a merge is specified without a key or merge-type

    Returns:
        pd.DataFrame: result of executing the operation on the given data
    """
    match operation["type"]:
        case "append":
            return operations.append(data)
        case "merge":
            if "key" not in operation or "merge-type" not in operation:
                raise ValueError("Key or merge type not specified for merge operation")

            return operations.merge(data, operation["key"], operation["merge-type"])
        case _:
            print("Operation not supported")


@timing
def process_operations(data_info: data_info) -> pd.DataFrame:
    """Executes, in order, every operation specified in the config

    Args:
        data_info (data_info): dataclass providing operation instructions

    Returns:
        pd.DataFrame: end result of executing the operations
    """
    operations = data_info.operations
    data_per_op = {name: [] for name in operations}
    current_op = None

    for name, operation in operations.items():
        files = operation["files"]

        data = [
            load_data(data_info, file_pat, format_file)
            if file_pat not in operations
            else data_per_op[file_pat]
            for file_pat, format_file in files
        ]

        data_per_op[name] = process_operation(operation, data)
        current_op = name

    result = data_per_op[current_op]
    logger.info(f"Shape of Result: {result.shape}")

    return result


def write_output(data, output) -> None:
    """Writes to I/O as defined by paramters in the instruction set

    Args:
        data (pd.DataFrame): data to write to file
        output (dict): output params as specified by the data info params
    """
    output_path = output["path"]
    output_formats = output["formats"]

    for output_format in output_formats:
        if output_format == "csv":
            write_csv(data, output_path)
        elif output_format == "parquet":
            write_parquet(data, output_path)
        else:
            logger.warn("Output format not supported.")


# could be put in its own file
def write_csv(data, output_path) -> None:
    try:
        data.to_csv(output_path + ".csv")
    except:
        logger.error("Error writing CSV output!")


def write_parquet(data, output_path) -> None:
    try:
        data.to_parquet(output_path + ".parquet")
    except:
        logger.error("Error writing Parquet output!")


@timing
def main(config_file_path):
    data_info = read_config(config_file_path)
    data_info.county_files = file_utils.get_all_files_in_dir(data_info.county_path)
    data_info.var_map = file_utils.read_var_map(data_info.var_map_path)
    processed_data = process_operations(data_info)
    write_output(processed_data, data_info.output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process county tax data")
    parser.add_argument("config_path", type=str, help="Path to the configuration file")
    parser.add_argument("exec_name", type=str, help="Name given to current execution")
    args = parser.parse_args()
    logger = configure_logger(execution_name=args.exec_name)
    main(args.config_path)

# TODO
# mapping processer
# code imp
# test Fulton completely
# complete other YAML files, ppt
# clean up repo + docs
# confirm data quality

# DOCS
# To run: python main.py <config_path> <execution_name>
