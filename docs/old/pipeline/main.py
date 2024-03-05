from pipeline import DataFormat
from pipeline import Pipeline


fulton_format = DataFormat(
    data_name="fulton_parcel",
    files_path=r"C:\Users\nicho\Documents\research\ga-tax-assessment\data\fulton\parcels",
    separator=None,
    format_path=r"C:\Users\nicho\Documents\research\ga-tax-assessment\pipeline\fulton_cols.csv",
    merge_files=[
        "2011",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
    ],
    merge_key="parid",
    pipes=None,
    out_file="fulton_all_parcels",
)


def main():
    # create dataclass and pass into pipeline
    Pipeline(fulton_format, True).execute()


if __name__ == "__main__":
    main()
