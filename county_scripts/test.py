import concat_files as c
import read_files as f

files = f.get_files_for_county('cobb', 'ASMT')
append_keys = ['CASMT', 'ASMT']
merge_keys = []
append_list, merge_list = c.join_files(files, append_keys, merge_keys)
print("")
print("")
for key, value in append_list.items():
    print(f"{key}")
    for item in value:
        print(item + '\n')
    print("")
# TODO: returns empty list inside list if no match
# include dot