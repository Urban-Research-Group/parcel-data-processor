## paulding-dbf-conversions.py -- goal: read in the data from 2014-2023 paulding & save as csv
## read in dbf files -> turn them into data frames -> save as csvs

import os
import re
import pandas as pd
from dbfread import DBF

###################### custom functions ######################

# get paths to all files inside subdir ending in .dbf
def get_dbf_files(input_folder):
    dbf_paths = [os.path.join(root, file)
                 for root, _, files in os.walk(input_folder)
                 for file in files
                 if file.endswith('.dbf')]

    return sorted(dbf_paths)


def dbf_to_csv(dbf_paths, output_folder):

    failed_files = []

    for dbf_path in dbf_paths:
        print("Attempting path: " + dbf_path)

        try:
            # read & convert .dbf file using dbfread
            table = DBF(dbf_path)
            df = pd.DataFrame(iter(table))
        except Exception as e:
           print(f"Error reading {dbf_path}: {e}")
           failed_files.append((dbf_path, "Error reading"))
           continue   
        
        try:
            # save df as csv
            csv_file = os.path.join(output_folder, os.path.splitext(dbf_path.replace(input_folder + "/", ""))[0] + '.csv')
            output_dir = csv_file.rsplit('/', 1)[0]
            os.makedirs(output_dir, exist_ok=True)
            df.to_csv(csv_file, index=False)

            print(f"Successfully converted {dbf_path} to {csv_file}")
        except Exception as e:
            print(f"Error saving {dbf_path} as CSV: {e}")
            failed_files.append((dbf_path, "Error saving"))
            continue

    if failed_files:
        df_failed_files = pd.DataFrame(failed_files, columns=["File Path", "Error Type"])
        return df_failed_files
    else:
        return print("No failed files")

###################### ################ ######################

# path to main folder
input_folder = 'data/paulding'
output_folder = input_folder + '/converted'

dbf_paths = get_dbf_files(input_folder)

## remove paths that contain the pattern '2019' -- these already have .csv's in original data 
filtered_paths = [path for path in dbf_paths if '2019' not in path] 

dbf_to_csv(filtered_paths, output_folder)

