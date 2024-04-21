from typing import List
import PyPDF2
import re
import os


def extract_text_from_pdf(pdf_path: str):
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
    return text


def process_text(lines: List[str]):
    var = {}
    curr_group = None
    for line in lines:
        file_name_pat = r"[A-Z]\)\s*([A-Z]+)[\s\)\(]+"
        if file_name := re.search(file_name_pat, line):
            file_name = file_name.group(1)
            var[file_name] = []
            curr_group = file_name
            continue

        if not curr_group:  # No starting group
            continue

        var_name_pat = r"\d+\)\s*([\w-]+)"
        number_pat = r"[\s\)]+(\d+(-\d+)?)[\s\-a-zA-Z]"

        if var_name := re.search(var_name_pat, line):
            var_name = var_name.group(1)

        if number := re.search(number_pat, line):
            number = number.group(1)

        if var_name and number:
            var[curr_group].append((var_name, number))

    return var


pdf_file = "C:\\Users\\Nick\\Documents\\code\\ga-tax-assessment\\data\\cobb\\TAXDATA2011\\Residential 2011\\AA407CCIS_LAYOUT.pdf"
root_path = r"C:\Users\Nick\Documents\code\ga-tax-assessment\data\cobb\TAXDATA2011\Residential 2011"

text = extract_text_from_pdf(pdf_file)
comma_idx_full = process_text(text.split("\n"))
comma_idx = {k: [] for k in comma_idx_full.keys()}
for k, var_list in comma_idx_full.items():
    for var in var_list:
        split = var[1].split("-")
        if len(split) > 1:
            val = int(split[1])
        else:
            val = int(split[0])

        comma_idx[k].append((var[0], val))

files = []
for f in os.listdir(root_path):
    if f.endswith(".DAT"):
        files.append(f)

for f in files:
    f_comma_idx = comma_idx[f.replace(".DAT", "")]

    with open(f"{root_path}/{f}", "r") as data:
        lines = data.readlines()
        lines = [line.replace(",", "") for line in lines]

    for line_i in range(len(lines)):
        for var in f_comma_idx:
            i = int(var[1])
            lines[line_i] = lines[line_i][:i] + "," + lines[line_i][i:]

    # write lines to new file
    with open(f"{root_path}/{f.replace('.DAT', '.csv')}", "w") as new_data:
        new_data.writelines(lines)


# do this for each cobb folder
# for each folder, use this to create new CSV files from the DAT files

# read in the DAT file
# for each line, place comma at each layout
