## my code -- goal: read in the data form 2014-2023 paulding and save data dictionary
## read in dbf files -> turn them into data frames -> pull the variable names for each file

import os
import re
import pandas as pd
from dbfread import DBF

# path to main folder
input_folder = 'data/paulding'
output_folder = 'data/paulding/converted'

# get paths to all files inside subdir ending in .dbf
def get_dbf_files(input_folder):
    dbf_paths = [os.path.join(root, file)
                 for root, _, files in os.walk(input_folder)
                 for file in files
                 if file.endswith('.dbf')]

    return sorted(dbf_paths)


def dbf_to_csv(dbf_paths, output_folder):

    for dbf_path in dbf_paths:
        
        # read & convert .dbf file using dbfread
        table = DBF(dbf_path)
        df = pd.DataFrame(iter(table))

        # save df as csv
        csv_file = os.path.join(output_folder, os.path.splitext(dbf_path.replace(input_folder + "/", ""))[0] + '.csv')
        output_dir = csv_file.rsplit('/', 1)[0]
        print(output_dir)

        os.makedirs(output_dir, exist_ok=True)

        df.to_csv(csv_file, index=False)

        print(f"Converted {dbf_path} to {csv_file}")


dbf_paths = get_dbf_files(input_folder)
dbf_to_csv(dbf_paths, output_folder)

## issues: -- implement try catch or smth
# error at data/paulding/2019 DIGEST VENDOR FILES/PE_REASON.dbf
# ValueError: invalid literal for int() with base 10: b'5   3'
# ValueError: could not convert string to float: b'5   3'