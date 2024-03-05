# we have a dict of files and their corresponding dataframes
# we want to append groups of files matching a pattern
# or concat groups of files matching a certain pattern
# ex: for fulton, append all files with NF, SF and YR
# ex: for cobb, append all CASMT files from each year residential folder
# ex: for paulding, we need to merge files within each subfolder by year,
# then append all resulting subfolder dfs


# WANT TO DROP UN-NEEDED VARS FIRST due to data size
# we also want to retain file source

import re
import pandas as pd


def _make_group_list(groups, keys):
    group_list = list(zip(*groups))
    return {
        keys[i]: list(match for match in group if match)
        for i, group in enumerate(group_list)
    }


def _group_files(files_str, pattern, keys):
    """
    Returns a list of files matching the pattern
    """
    groups = re.findall(pattern, files_str, re.IGNORECASE)
    return _make_group_list(groups, keys)


def _generate_pattern(keys):
    """
    Returns a regex pattern for the keys
    """
    return "|".join(f"(?:^|,)([^,]*?{re.escape(key)}[^,]*)" for key in keys)


def group_files(file_paths, keys):
    # TODO IGNORE CASE
    groups = dict.fromkeys(keys, [])
    for file in files:
        for key in keys:
            if key in file:
                groups[key].append(file)

    return groups


def join_files(files, append_keys, merge_keys):
    """
    Returns a list of dataframes created from the files
    """
    append_pattern = _generate_pattern(append_keys)
    merge_pattern = _generate_pattern(merge_keys)
    files_str = ",".join(files)

    append_list = (
        _group_files(files_str, append_pattern, append_keys) if append_pattern else []
    )
    merge_list = (
        _group_files(files_str, merge_pattern, merge_keys) if merge_pattern else []
    )

    return append_list, merge_list
