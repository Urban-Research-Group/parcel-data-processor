## my code -- goal: read in the data form 2011-2013 paulding and save data dictionary
## dictionary format {var : (start, end)}

import os
import pandas as pd
import re
import csv

###################### custom functions ######################

# cleans data dictionary table
def clean_paulding(table):
    ## clean
    table[['start', 'end']] = table.iloc[:, 1].str.split('-', expand=True) # separate second col

    table['start'].fillna(table['range'], inplace=True)
    table['end'].fillna(table['start'], inplace=True) # when end is na -- the range is a single num
    
    table['start'] = table['start'].astype(int) # change to int types
    table['end'] = table['end'].astype(int)

    table = table.sort_values(by='start', ascending=True) ### @mel error here 5/24

    # remove excess white space between words
    table.iloc[:, 0] = table.iloc[:, 0].apply(lambda x: re.sub(r'\s+', ' ', x.strip()))
    cleaned_table = table.rename(columns={table.columns[0]: "variable_name",
                                      table.columns[1]: "range",
                                      table.columns[2]: "length"})

    return(cleaned_table)


###################### ################ ######################

years = [2011, 2012, 2013]
paths = ["/Users/melissajuarez/Documents/GITHUB REPOS/parcel-data-processor/data/paulding/2011 DIGEST VENDOR FILES/Record_Layout_Paulding_Georgia.xlsx", 
         "/Users/melissajuarez/Documents/GITHUB REPOS/parcel-data-processor/data/paulding/2012 DIGEST VENDOR FILES/Paulding_GA_Record_Layout.xlsx", 
         "/Users/melissajuarez/Documents/GITHUB REPOS/parcel-data-processor/data/paulding/2013 DIGEST VENDOR FILE/Paulding_GA_Record_Layout.xlsx"]

for file in paths:
    paulding_df = pd.read_excel(paths[0])
    paulding_df = clean_paulding(paulding_df)


###########

table = pd.read_excel(paths[0])
df = clean_paulding(table)

### need to create function to add delimiters inside txt file.

# 1. read txt file
path = '/Users/melissajuarez/Documents/GITHUB REPOS/parcel-data-processor/data/paulding/2011 DIGEST VENDOR FILES/PAULDING_GA_2011_REAL_DIGEST.txt'

with open(path, "r") as f:
    lines = f.readlines()


# 2. get expected locations of delims
delim_locs = df['end'].tolist()
delim_locs = [int(num) for num in delim_locs]

expected_line_len = df['end'].iloc[-1]

# 3. process the lines and add in commas
processed_lines = []
for line in lines:
    i = 0

    # check if the line length matches expected length
    if len(line) != expected_line_len:
        raise ValueError(f"Line at index {i} has length {len(line)}, does not match expected length {expected_line_len}")
    else:
        ## replace instances of commas as " "
        line = line.replace(",", " ")

        segments = []
        previous_loc = 0
        for loc in sorted(delim_locs):
            segments.append(line[previous_loc:loc]) ## segment into pieces by delim locations
            previous_loc = loc
        processed_line = ','.join(segments)

    processed_lines.append(processed_line)    
    i = i + 1

# 4. write processed lines into a csv
try:
    # Write the processed lines to a CSV file
    with open('/Users/melissajuarez/Documents/GITHUB REPOS/parcel-data-processor/data/paulding/2011 DIGEST VENDOR FILES/PAULDING_GA_2011_REAL_DIGEST.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write the header
        csv_writer.writerow(df['variable_name'].tolist())
        # Write the processed lines
        for line in processed_lines:
            csv_writer.writerow(line.split(','))
except ValueError as e:
    print(e)