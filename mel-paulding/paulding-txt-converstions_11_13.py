## my code -- goal: read in the data form 2011-2013 paulding and save data dictionary
## dictionary format {var : (start, end)}

import os
import pandas as pd
import re
import csv


###################### custom functions ######################

# looks for data dictionary and txt files inside folders from relevant years
# return dictionary of structure {root_path: [.xslx, .txt]}
# defaults to 2011-2013
def get_relevant_files(input_folder, years):
    files_dict = {}

    for root, dirs, files in os.walk(input_folder):
        # check if the current directory contains any of the specified years
        if any(year in root for year in years):
            # get .xlsx and .txt files from the current directory
            xlsx_files = [os.path.join(root, file) for file in files if file.endswith('.xlsx')]
            txt_files = [os.path.join(root, file) for file in files if file.endswith('.txt')]

            # save paths in dict
            if xlsx_files or txt_files:
                files_dict[root] = xlsx_files + txt_files
    return files_dict

# cleans data dictionary table
def clean_paulding(table):
    ## clean
    table[['start', 'end']] = table.iloc[:, 1].str.split('-', expand=True) # separate second col

    table['start'].fillna(table['range'], inplace=True)
    table['end'].fillna(table['start'], inplace=True) # when end is na -- the range is a single num
    
    table['start'] = table['start'].astype(int) # change to int types
    table['end'] = table['end'].astype(int)

    table = table.sort_values(by='start', ascending=True)

    # remove excess white space between words
    table.iloc[:, 0] = table.iloc[:, 0].apply(lambda x: re.sub(r'\s+', ' ', x.strip()))
    cleaned_table = table.rename(columns={table.columns[0]: "variable_name",
                                      table.columns[1]: "range",
                                      table.columns[2]: "length"})

    return cleaned_table

# reads in the txt lines and dictionary and inserts commas where specified
def process_lines(txt_lines, dictionary):
    print("Attempting to process .txt file.")
    #get expected locations of delims
    delim_locs = dictionary['end'].tolist()
    delim_locs = [int(num) for num in delim_locs]
    expected_line_len = dictionary['end'].iloc[-1]

    #process the lines and insert commas
    processed_lines = []
    for line in txt_lines:
        i = 0
        # check if the line length matches expected length
        if len(line) != expected_line_len:
            raise ValueError(f"Line at index {i} has length {len(line)}, does not match expected length {expected_line_len}")
        else:
            ## replace instances of commas as " " to not be confused as delimiter
            line = line.replace(",", " ")

            ## insert commas
            segments = []
            previous_loc = 0
            for loc in sorted(delim_locs):
                segments.append(line[previous_loc:loc].rstrip()) ## segment into pieces by delim locations
                previous_loc = loc
            processed_line = ','.join(segments)

        processed_lines.append(processed_line)    
        i = i + 1

    print("Successfully processed .txt file.")

    return processed_lines

def processed_file_to_csv(input_folder):

    print("Attempting to save file as .csv")
    output_folder = input_folder + '/converted'
    csv_file_path = os.path.join(output_folder, os.path.splitext(txt.replace(input_folder + "/", ""))[0] + '.csv')
    output_dir = csv_file_path.rsplit('/', 1)[0]
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Write the processed lines to a CSV file
        with open(csv_file_path, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write the header
            csv_writer.writerow(paulding_df['variable_name'].tolist())
            # Write the processed lines
            for line in processed_lines:
                csv_writer.writerow(line.split(','))
        return print(f"File successfully saved to {csv_file_path}.")
    except ValueError as e:
        return print(f"Error saving file {csv_file_path} \n {e}")

###################### ################ ######################


input_folder = 'data/paulding'
years = ['2011', '2012', '2013']

paths = get_relevant_files(input_folder, years)

for subdir, files in paths.items():
    print("Working on: " + subdir)
    # save paths
    dict = files[0]
    txt = files[1]

    paulding_df = pd.read_excel(dict)
    paulding_df = clean_paulding(paulding_df)
    print("Data dictionary: \n",paulding_df)
    
    with open(txt, "r") as f:
        lines = f.readlines()

    processed_lines = process_lines(lines, paulding_df)
    processed_file_to_csv(input_folder)

