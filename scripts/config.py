import os

PARENT_DIR = os.path.dirname(os.getcwd())
PATH_TO_DATA = PARENT_DIR + r"\data\\"
PATH_TO_MAPPINGS = PATH_TO_DATA + r"structure\\"

VARIABLE_MAPPING_PATHS = {
    "cherokee": {
        "parcel": "cherokee_parcel_cols_2020-2022.csv",
        "sales": "cherokee_sales_cols_2020-2022.csv",
        "appeals": "cherokee_appeals_cols_2020-2022.csv",
    },
    "fulton": {
        "parcel": "fulton_parcel_cols_2020-2022.csv",
        "sales": "fulton_sales_cols_2020-2022.csv",
        "appeals": "fulton_appeals_cols_2020-2022.csv",
    },
}
