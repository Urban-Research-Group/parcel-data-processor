import pandas as pd
import operations as ops
import file_utils
from config import DataInfo
from logger import configure_logger, timing

logger = configure_logger()


class DataProcessor:
    def __init__(self, data_info: DataInfo):
        self.data_info = data_info
        self.data_per_op = {name: [] for name in self.data_info.operations.keys()}
        self.current_op = None

    @timing
    def load_data(self, file_key: tuple, parser_name: str) -> list[pd.DataFrame]:
        """Selects required files and calls on file_utils to complete I/O read

        Args:
            data_info (data_info): dataclass providing data instruction parameters
            file_key (str): tuple of group and file pattern to select files
            format_file (str): pattern to identify format files for each data file, if needed

        Returns:
            list[pd.DataFrame]: list of dataframes created by reading in each file specified
        """
        data = None
        if file_key[1] not in self.data_info.operations:
            selected_files = file_utils.select_files(self.data_info.files, file_key)
            data = file_utils.create_dfs_from_files(
                selected_files,
                self.data_info.var_map_non_derived,
                parser_name,
            )
        else:
            data = [self.data_per_op[file_key[1]]]

        return data

    @staticmethod
    def _guard_join(operation) -> None:
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
                return ops.concat([*data])
            case "merge" | "join":
                DataProcessor._guard_join(operation)
                return ops.join(data, operation["key"], operation["join-type"])
            case _:
                print("Operation not supported")

    def run(self) -> pd.DataFrame:
        """Executes, in order, every operation specified in the config

        Args:
            data_info (data_info): dataclass providing operation instructions

        Returns:
            pd.DataFrame: end result of executing the operations
        """
        for name, operation in self.data_info.operations.items():
            # combine groups and file patterns to get all desired files
            groups = [group for group in operation.get("groups", [None])]
            files = operation["files"].keys()
            file_keys = [(group, file) for group in groups for file in files]

            parser_names = operation["files"].values()

            data = [
                df
                for file_keys, parser_name in zip(file_keys, parser_names)
                for df in self.load_data(file_keys, parser_name)
            ]

            self.data_per_op[name] = DataProcessor.process_operation(operation, data)
            self.data_per_op[name] = ops.clean_df(
                df=self.data_per_op[name], var_map=self.data_info.var_map_non_derived
            )

            # Reduce memory usage
            if name not in self.data_info.retain:
                del self.data_per_op[name]
            else:
                self.data_info.retain.remove(name)
            self.current_op = name

        result = self.data_per_op[self.current_op]
        if not self.data_info.var_map_derived.empty:
            ops.create_derived_cols(result, self.data_info.var_map_derived, sep=" ")

        logger.info("Shape of Result: %s", result.shape)
        return result
