import argparse
import yaml
import operations
import file_utils
from logger import configure_logger, timing


def read_config(file_path):
    with open(file_path, "r") as config:
        return yaml.safe_load(config)


@timing
def load_data(operation, var_map, county_files, file_pattern, format_file, data_per_op):
    if file_pattern in operation.keys():
        return data_per_op[file_pattern]
    else:
        selected_files = file_utils.select_files(county_files, file_pattern)
        return file_utils.create_dfs_from_files(selected_files, format_file, var_map)


@timing
def process_operation(operation, var_map, county_files, data_per_op):
    data = []

    for file_pattern, format_file in operation["files"].items():
        dfs = load_data(
            operation, var_map, county_files, file_pattern, format_file, data_per_op
        )
        data += dfs

    if operation["type"] == "append":
        return operations.append(data)
    elif operation["type"] == "merge":
        return operations.merge(data, operation["key"], operation["merge-type"])
    else:
        print("Operation not supported")


@timing
def process_operations(config, var_map, county_files):
    data_per_op = {key: [] for key in config["operations"].keys()}
    current_op = None

    for name, operation in config["operations"].items():
        data_per_op[name] = process_operation(
            operation, var_map, county_files, data_per_op
        )
        current_op = name

    logger.info(f"Shape of Result: {data_per_op[current_op].shape}")
    return data_per_op[current_op]


def output(data, output_path, output_formats):
    for output_format in output_formats:
        try:
            if output_format == "csv":
                data.to_csv(output_path + ".csv")
            elif output_format == "parquet":
                data.to_parquet(output_path + ".parquet")
            else:
                logger.warn("Output format not supported.")
        except:
            logger.error("Error writing output!")
            continue


@timing
def main(config_file_path):
    config = read_config(config_file_path)
    county_files = file_utils.get_all_files_in_dir(config["county-path"])
    var_map = file_utils.read_var_map(config["var-map-path"])
    processed_data = process_operations(config, var_map, county_files)
    output(processed_data, config["output"]["path"], config["output"]["formats"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some data.")
    parser.add_argument("config_path", type=str, help="Path to the configuration file")
    parser.add_argument("exec_name", type=str, help="Path to the configuration file")
    args = parser.parse_args()
    logger = configure_logger(execution_name=args.exec_name)
    main(args.config_path)

# and docs
# run Fulton as test
# complete other YAML files, ppt

# Next steps:
# run more counties
# confirm data quality, make modifications
# re-assess process; took too long
