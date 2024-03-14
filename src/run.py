import config
import file_io
from processor import DataProcessor
from logger import configure_logger, timing


STATUS_CODE_SUCCESS = 0


@timing
def run(config_path: str, exec_name: str) -> int:
    """Runs the application

    Args:
        config_path (str): _description_
        exec_name (str): _description_

    Returns:
        int: _description_
    """
    configure_logger(execution_name=exec_name)
    data_info = config.create_data_info(config_path)
    processed_data = DataProcessor(data_info).run()

    file_io.WriteOutput(
        processed_data,
        data_info.county_name,
        data_info.output["path"],
        data_info.output["formats"],
    ).write_output()

    return STATUS_CODE_SUCCESS
