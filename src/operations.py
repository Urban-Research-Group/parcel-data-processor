from functools import reduce
import pandas as pd


def append(data):
    return pd.concat(data).reset_index(drop=True)


def merge(data, key, merge_type):
    return reduce(
        lambda left, right: pd.merge(left, right, on=key, how=merge_type), data
    )
