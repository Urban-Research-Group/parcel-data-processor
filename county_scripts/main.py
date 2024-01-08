import concat_files
import read_files
import pandas as pd
from functools import reduce

# mapping processor
# county config

files = read_files.get_files_for_county('cobb', 'ASMT')
append_keys = ['CASMT']
merge_keys = ['ASMT']
append_list, merge_list = concat_files.join_files(files, append_keys, merge_keys)

append_files = read_files.create_dfs_from_files(append_list)
for file in append_files:
    file.drop(columns=['PARCEL', 'TAXDIST', 'TAXYEAR'], inplace=True)
appened = pd.concat(append_files)

merge_files = read_files.create_dfs_from_files(merge_list)
for file in merge_files:
    file.drop(columns=['PARCEL', 'TAXDIST', 'TAXYEAR'], inplace=True)

# go backwards, 
for file in merge_files[-1]:
    reduce(lambda left,right: pd.merge(left,right,on=merge_key), dfs)

