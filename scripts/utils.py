import config
import polars as pl


class MappingProcessor:
    def __init__(self, county: str):
        self.county = county

    def get_mapping(self, data_class: str):
        mapping_file = config.VARIABLE_MAPPING_PATHS[self.county][data_class]
        mapping_df = pl.read_csv(config.PATH_TO_MAPPINGS + mapping_file)
        files = mapping_df["source_file"].unique().to_list()
        var_map = [
            {file: mapping_df.filter(pl.col("source_file") == file).to_dicts()}
            for file in files
        ]
        return files, var_map
