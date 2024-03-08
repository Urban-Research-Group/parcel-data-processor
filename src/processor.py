import pandas as pd
from src import operations
from src import file_utils
from src.config import DataInfo
from src.logger import configure_logger, timing

logger = configure_logger()


class DataProcessor:
    def __init__(self, data_info: DataInfo):
        # TODO: should just keep DataInfo object
        self.data_info = data_info
        self.county_files = data_info.county_files
        self.var_map_non_derived = data_info.var_map_non_derived
        self.var_map_derived = data_info.var_map_derived
        self.derive_vars = data_info.derive_vars
        self.operations = data_info.operations
        self.data_per_op = {name: [] for name in self.operations}
        self.current_op = None

    @timing
    def load_data(self, file_pattern: str, format_file: str) -> list[pd.DataFrame]:
        """Selects required files and calls on file_utils to complete I/O read

        Args:
            data_info (data_info): dataclass providing data instruction parameters
            file_pattern (str): selects files matching this pattern in given dir path
            format_file (str): pattern to identify format files for each data file, if needed

        Returns:
            list[pd.DataFrame]: list of dataframes created by reading in each file specified
        """
        data = None

        if file_pattern not in self.operations:
            selected_files = file_utils.select_files(self.county_files, file_pattern)

            data = file_utils.create_dfs_from_files(
                selected_files, format_file, self.var_map_derived
            )
        else:
            data = [self.data_per_op[file_pattern]]

        return pd.concat(data, axis=0)

    @staticmethod
    def guard_join(operation) -> None:
        if "key" not in operation or "join-type" not in operation:
            error_msg = "Key or merge type not specified for join operation"
            logger.error(error_msg)
            raise ValueError(error_msg)

    @timing
    @staticmethod
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
        operation_type = operation["type"]

        match operation_type:
            case "append" | "concat":
                return operations.concat([*data])
            case "merge" | "join":
                DataProcessor.guard_join(operation)
                return operations.join(data, operation["key"], operation["join-type"])
            case _:
                print("Operation not supported")

    @timing
    def run(self) -> pd.DataFrame:
        """Executes, in order, every operation specified in the config

        Args:
            data_info (data_info): dataclass providing operation instructions

        Returns:
            pd.DataFrame: end result of executing the operations
        """
        for name, operation in self.operations.items():
            files = operation["files"]

            data = [
                self.load_data(file_pat, format_file)
                for file_pat, format_file in files.items()
            ]

            self.data_per_op[name] = DataProcessor.process_operation(operation, data)
            self.current_op = name

        # Create derived vars here
        if self.derive_vars:
            self.data_per_op["derived"] = operations.create_derived_cols(
                self.data_per_op[self.current_op], self.var_map_derived
            )
            self.current_op = "derived"

        result = self.data_per_op[self.current_op]
        logger.info("Shape of Result: %s", result.shape)

        return result
