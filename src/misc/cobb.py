from typing import List
import PyPDF2
import re
import os


class bcolors:
    HEADER = "\033[95m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"


def extract_lines_from_pdf(pdf_path: str) -> List[str]:
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
    return text.split("\n")


def extract_variables(lines: List[str]) -> dict:
    var_list = {}
    curr_group = None
    for line in lines:
        file_name_pat = r"[A-Z]\)\s*([A-Z]+)[\s\)\(]+"
        var_name_pat = r"\d+\)\s*([\w-]+)"
        # column_width_pat = r"[\s\)]+(\d+(-\d+)?)[\s\-a-zA-Z]"
        column_width_pat = r"\((\d+)\)"

        if file_name := re.search(file_name_pat, line):
            file_name = file_name.group(1)
            var_list[file_name] = []
            curr_group = file_name

        if not curr_group:  # No starting group
            continue

        if var_name := re.search(var_name_pat, line):
            var_name = var_name.group(1)

        if number := re.search(column_width_pat, line):
            number = number.group(1)

        if var_name and number:
            var_list[curr_group].append((var_name, number))

    return var_list


def clean_variables(var_list: dict) -> dict:
    res = {k: [] for k in var_list}
    for k, var_list in var_list.items():
        column_pos = 0
        for var in var_list:
            column_pos += int(var[1])
            res[k].append((var[0], int(column_pos)))
    print(res)
    return res


def convert_lines(raw: List[str], var_columns: dict) -> None:
    # remove commas from DAT format to convert to CSV
    res = [line.replace(",", "") for line in raw]

    # sort in decreasing order to avoid the previous insertion
    # impacting the next insertion by changing the index
    var_columns = sorted(var_columns, key=lambda x: x[1], reverse=True)

    for line_i in range(len(res)):
        for var in var_columns:
            char_i = var[1]
            res[line_i] = res[line_i][:char_i] + "," + res[line_i][char_i:]
            # raw[line_i] = re.sub(
            #    r"(?<!\w) (?!\w)", "", raw[line_i]
            # )  # removes excess whitespace

    return res


def main(path_to_dirs: List[str]) -> int:
    for path_to_dir in path_to_dirs:
        print("\n--------------------")
        print(f"{bcolors.HEADER}MAIN DIR: {path_to_dir}{bcolors.ENDC}")
        SUBDIR_NAMES = ["residential", "commercial"]
        path_to_subdirs = [
            os.path.join(path_to_dir, d)
            for d in os.listdir(path_to_dir)
            if any(name in d.lower() for name in SUBDIR_NAMES)
        ]

        for path_to_subdir in path_to_subdirs:
            print("\nCurrently processing: ", path_to_subdir)
            try:
                format_files = (
                    f for f in os.listdir(path_to_subdir) if f.endswith(".pdf")
                )
                format_file_name = next(format_files, None)
            except NotADirectoryError:
                print(
                    f"{bcolors.FAIL}{path_to_subdir} is not a directory{bcolors.ENDC}"
                )
                continue

            if not format_file_name:
                print(
                    f"{bcolors.WARNING}No PDF file found in {path_to_subdir}{bcolors.ENDC}"
                )
                continue

            format_file_path = os.path.join(path_to_subdir, format_file_name)
            print(
                f"{bcolors.OKGREEN}Using format file: {format_file_path}{bcolors.ENDC}"
            )
            raw = extract_lines_from_pdf(format_file_path)
            print("Extracted lines from pdf")
            var_list = extract_variables(raw)
            var_list = clean_variables(var_list)

            print(f"{bcolors.HEADER}---WRITING---{bcolors.ENDC}")
            for f in os.listdir(path_to_subdir):
                if not f.endswith(".DAT"):
                    continue

                file_path = os.path.join(path_to_subdir, f)
                file_name = f.split("/")[-1].replace(".DAT", "")
                with open(file_path, "r") as file:
                    raw = file.readlines()

                new_lines = convert_lines(raw, var_list[file_name])

                new_file_path = file_path.replace(".DAT", ".csv")
                print(f"{bcolors.OKGREEN}{new_file_path}{bcolors.ENDC}")
                with open(new_file_path, "w") as new_data:
                    # header line
                    new_data.write(
                        ",".join(var[0] for var in var_list[file_name]) + "\n"
                    )
                    # data
                    new_data.writelines(new_lines)

    return 0


CURR_PATH = os.path.dirname(os.path.realpath(__file__))
REL_PATH_TO_DATA = os.path.join("..", "..", "data", "cobb")
PATH = os.path.join(CURR_PATH, REL_PATH_TO_DATA)

dirs = [os.path.join(PATH, dir) for dir in os.listdir(PATH) if "." not in dir]
print(main(dirs))

# TODO: verify column width in output
# TODO: clean files with format file processor
