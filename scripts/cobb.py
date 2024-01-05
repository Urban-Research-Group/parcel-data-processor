import os
import config

COUNTY = "cobb"
PATH_TO_COUNTY = config.PATH_TO_DATA + f"\{COUNTY}\parcels\\"


def main():
    dfs = []
    for folder in os.listdir(PATH_TO_COUNTY):
        folder_path = PATH_TO_COUNTY + folder
        # do we want residential and commercial
        for file in os.listdir(folder_path):
            print(file)


main()
