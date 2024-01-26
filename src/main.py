import argparse
from src import config
from src.processor import DataProcessor
from src import file_io
from src.logger import configure_logger, timing


@timing
def _main(config_path):
    data_info = config.create_data_info(config_path)
    processed_data = DataProcessor(data_info).run()

    file_io.WriteOutput(
        processed_data, data_info["output_path"], data_info["output_formats"]
    ).write_output()


def _parse_args():
    parser = argparse.ArgumentParser(description="Process county tax data")
    parser.add_argument("config_path", type=str, help="Path to the configuration file")
    parser.add_argument("exec_name", type=str, help="Name given to current execution")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logger = configure_logger(execution_name=args.exec_name)
    _main(args.config_path)
