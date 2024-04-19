
import pandas as pd
import numpy as np
import datetime


## read in datasets
output_df = pd.read_csv('data/outputfulton.csv')
output_df.head()

# small sample to test efficiency
sample_df = output_df.sample(100000)

## either do manual tests for each column or use exec() on excel column snippets???
validations_df = pd.read_excel('data/data_format_MJ.xlsx', sheet_name='data validation') ## validation script
validation_criteria = validations_df[['final_variable_name', 'data_validation_logic']].to_numpy()
#column_names = validations_df['final_variable_name'].to_numpy() #@mel need to change to go by the col names of the output df

sample_df[sample_df['tax_year'] >= 1980]
name = sample_df
eval(f"{name}[{name}['tax_year'] >= 1980]")
eval("sample_df[sample_df['tax_year'] >= 1980]")

## pseudo
for column in sample_df.columns :
    columns_where_failed = []
    # find the logic corresponding to that column in the validation dataset -- takes first occurence of col name
    snippet = validations_df[validations_df['final_variable_name'] == column].iloc[0, 1]
    print(snippet)

    # isolate column as np array
    column = sample_df[[column]].to_numpy()
    print(column)

    # evaluate snippet
    eval(snippet)

    # if failed, add column name to columns_where_failed


## trial
tax_year = np.array([1970, 1600, 1999])
snippet = "tax_year >= 1980"
validations_df[validations_df['final_variable_name'] == 'tax_year'].iloc[0, 1]

eval(snippet)

columns_where_failed = np.empty(len(tax_year), dtype=object)
print(columns_where_failed)

for x in eval(snippet):
    if True:
