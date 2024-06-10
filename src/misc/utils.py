import os
import pandas as pd


def get_rel_files(data_info: pd.DataFrame) -> list[str]:
    rel_files = data_info["source_file"].tolist()
    rel_files = [f for f in rel_files if not isinstance(f, float)]
    return rel_files


def get_rel_cols(path: str, data_info: pd.DataFrame) -> list[str]:
    FILE_POSTFIXES = [".DAT", ".CSV", ".XLSX"]
    FILE_POSTFIXES += [postfix.lower() for postfix in FILE_POSTFIXES]
    file_name = os.path.split(path)[1]

    for postfix in FILE_POSTFIXES:
        file_name = file_name.replace(postfix, "")

    col_names = data_info[data_info["source_file"] == file_name]["old_name"].tolist()
    for name in col_names:
        if ";" in name:
            names = name.split(";")
            col_names.remove(name)
            col_names += names

        if "+" in name:
            names = name.split("+")
            col_names.remove(name)
            col_names += names

    return col_names


def execute_instructions(data: pd.DataFrame, data_info: pd.DataFrame) -> pd.DataFrame:
    for row in data_info.itertuples():
        old_name = row.old_name
        if ";" in old_name:
            cols = row.old_name.split(";")
            new_name = row.new_name
            data[new_name] = data[cols].apply(
                lambda row: "".join(row.values.astype(str)), axis=1
            )

        elif "+" in old_name:
            cols = row.old_name.split("+")
            new_name = row.new_name
            data[new_name] = data[cols].sum(axis=1)

    return data


def rename_cols(data: pd.DataFrame, data_info: pd.DataFrame) -> pd.DataFrame:
    valid_cols = data_info.dropna(subset=["old_name"])
    valid_cols = data_info.where(
        ~data_info["old_name"].str.contains(";")
        & ~data_info["old_name"].str.contains("\+")
    )
    old_names = valid_cols["old_name"].tolist()
    name_mapping = zip(old_names, valid_cols["new_name"].tolist())
    return data.rename(columns=dict(name_mapping))


def merge_data(data: list[pd.DataFrame], key: str, how: str) -> pd.DataFrame:
    return pd.concat(data, axis=1, join=key, how=how)


def append_data(data: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(data, axis=0)
