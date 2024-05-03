## my code -- goal: read in the data form 2011-2013 paulding and save data dictionary
## dictionary format {var : (start, end)}

import camelot
import pandas as pd
import re

###################### custom functions ######################
def read_paulding(pdf_path):
    ## read & concat pages
    table = camelot.read_pdf(pdf_path, pages='all')
    df_list = []
    for i in range(len(table)):
        tbl = table[i-1].df
        tbl = tbl[tbl.iloc[:, 2].str.strip() != ""] # remove where 3rd col is just whitespace
        df_list.append(tbl)
    long_table = pd.concat(df_list)

    return(long_table)

def clean_paulding(table):
    ## clean
    table[['start', 'end']] = table.iloc[:, 1].str.split('-', expand=True) # separate second col
    table['start'] = table['start'].astype(int) # change to int types
    table['end'].fillna(table['start'], inplace=True) # when end is na -- the range is a single num
    table['end'] = table['end'].astype(int)

    table = table.sort_values(by='start', ascending=True)

    # remove excess white space between words
    table.iloc[:, 0] = table.iloc[:, 0].apply(lambda x: re.sub(r'\s+', ' ', x.strip()))
    cleaned_table = table.rename(columns={table.columns[0]: "variable_name"})

    return(cleaned_table)

def create_dict_paulding(table):
    ## create data dictionary
    data_dictionary = {}
    for i, row in table.iterrows():
        variable_name = row['variable_name']
        start = row['start']
        end = row['end']

        # add info to dictionary
        data_dictionary[variable_name] = (start, end)
    
    return(data_dictionary)

###################### ################ ######################

years = [2011, 2012, 2013]
paths = ["/Users/melissajuarez/Documents/GITHUB REPOS/parcel-data-processor/data/paulding/2011 DIGEST VENDOR FILES/Record_Layout_Paulding_Georgia.pdf", "/Users/melissajuarez/Documents/GITHUB REPOS/parcel-data-processor/data/paulding/2012 DIGEST VENDOR FILES/Paulding_GA_Record_Layout.pdf", "/Users/melissajuarez/Documents/GITHUB REPOS/parcel-data-processor/data/paulding/2013 DIGEST VENDOR FILE/Paulding_GA_Record_Layout.pdf"]

dicts = []
for file in paths:
    paulding_df = read_paulding(pdf_path = file)

    paulding_df = clean_paulding(paulding_df)

    paulding_dict = create_dict_paulding(paulding_df)
    dicts.append(paulding_dict)

paulding_dicts = pd.DataFrame(years, dicts)