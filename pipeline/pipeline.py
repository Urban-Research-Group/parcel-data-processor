"""Module"""
import polars as pl
import filters
import logging
import os

from dataclasses import dataclass


@dataclass(kw_only=True)
class DataFormat:
    """Class to configure the data format for each county"""

    data_name: str
    files_path: str
    separator: str = None
    format_path: str
    merge_files: list
    merge_key: list
    pipes: list
    out_file: str


class Pipeline:
    """Class to construct the data pipeline"""

    def __init__(self, data_format: DataFormat, use_checkpoints: bool):
        self.data_format = data_format
        self.column_format = pl.read_csv(data_format.format_path)
        self.use_checkpoints = use_checkpoints
        self.data = None

    def _merge_group(self, file_group):
        return filters.merge_cols(
            [df for df in self.data if df.file_name.str.contains(file_group)],
            self.data_format.merge_key,
        )

    def _handle_merge(self):
        new_dfs = []
        for file_group in self.data_format.merge_files:
            new_dfs.append(self._merge_group(file_group))
        return new_dfs

    def _set_checkpoint(self, curr_step):
        files = self._create_checkpoint_names(curr_step)
        for i, file in enumerate(files):
            self.data[i].to_parquet(file)

    def _get_checkpoint(self, curr_step):
        files = self._get_checkpoint_names(curr_step)
        dfs = []
        for file in files:
            dfs.append(pl.read_parquet(file))

        if len(dfs) == 1:
            dfs = dfs[0]
        return dfs

    def _has_checkpoint(self, curr_step):
        return len(self._get_checkpoint_names(curr_step)) > 0

    def _get_checkpoint_names(self, curr_step):
        CHECKPOINT_NAME = f"{self.data_format.data_name}-checkpoint{curr_step}"
        files = []
        for file in os.listdir(
            r"C:\Users\nicho\Documents\research\ga-tax-assessment\pipeline\checkpoints"
        ):
            if file in CHECKPOINT_NAME:
                files.append(file + ".parquet")
        return files

    def _create_checkpoint_names(self, curr_step):
        CHECKPOINT_NAME = f"{self.data_format.data_name}-checkpoint{curr_step}"
        if type(self.data) == list:
            return [CHECKPOINT_NAME + ".parquet" for df in self.data]
        return CHECKPOINT_NAME + ".parquet"

    def execute(self):
        """Executes the data pipeline as described by the county DataFormat configuration"""
        # Read data, skip steps if checkpoint is found
        curr_step = 0

        if self._has_checkpoint(curr_step) and self.use_checkpoints:
            self.data = self._get_checkpoint(curr_step)
        else:
            self.data = filters.read_data(self.data_format, self.column_format)
            self._set_checkpoint(curr_step)
        curr_step += 1

        # Merge files (if necessary)
        if self.use_checkpoints and self._has_checkpoint:
            self.data = self._get_checkpoint(curr_step)
        elif self.data_format.merge_files:
            self.data = self._handle_merge()
            self._set_checkpoint(curr_step)

        curr_step += 1

        # Append files
        if self.use_checkpoints and self._has_checkpoint:
            self.data = self._get_checkpoint(curr_step)
        else:
            filters.append_cols(self.data)

        # Feed data through pipes
        for pipe_filter in self.data_format.pipes:
            self.data = pipe_filter(self.data)

        # Export data
        filters.export_to_csv(self.data, self.data_format.out_file)
        filters.export_to_parquet(self.data, self.data_format.out_file)
