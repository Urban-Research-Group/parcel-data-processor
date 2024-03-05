import polars as pl
import os

####################
# If I have to run again, put source as nf, sf, etc. instead of just fulton
# and re-order columns
####################

FILES_PATH = "../data/fulton/parcels/"
COL_MAP = dict(pl.read_csv("fulton_cols.csv")[["old_name", "new_name"]].iter_rows())
DTYPE_MAP = dict(pl.read_csv("fulton_cols.csv")[["new_name", "dtype"]].iter_rows())
CONVERT_DTYPES = {
    "Int16": pl.Int16,
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "Float16": pl.Float32,
    "Float32": pl.Float32,
    "Float64": pl.Float64,
    "String": pl.Utf8,
}


def fill_null_with_zero(df, col):
    return df.with_columns(
        pl.when(pl.col(col).is_null()).then(pl.lit(0)).otherwise(pl.col(col))
    )


def fill_null_with_empty(df, col):
    return df.with_columns(
        pl.when(
            (pl.col(col).is_null())
            | (pl.col(col).str.to_uppercase() == "NULL")
            | (pl.col(col).str.to_uppercase() == "NAN")
        )
        .then(pl.lit(""))
        .otherwise(pl.col(col))
    )


def list_to_str(lst):
    return "".join(map(str, lst))


dfs = []

for file in os.listdir(FILES_PATH):
    print(f"Reading {file}")
    dfs.append(
        pl.read_excel(
            source=FILES_PATH + file,
            read_csv_options={
                "infer_schema_length": 0,
                "columns": list(COL_MAP.keys()),
            },
            xlsx2csv_options={"ignore_formats": ["float"]},
        )
    )

for df in dfs:
    df = df.select(~pl.col("Parid").str.contains("COUNT"))

df = pl.concat(dfs)
df = df.rename(COL_MAP)

for key, val in DTYPE_MAP.items():
    DTYPE_MAP[key] = CONVERT_DTYPES[val]

for column in df.columns:
    if DTYPE_MAP[column] in pl.NUMERIC_DTYPES:
        df = df.with_columns(
            pl.col(column).str.extract_all(r"[0-9]").apply(list_to_str).keep_name()
        )
    df = df.select(pl.col(column).cast(DTYPE_MAP[column]), pl.all().exclude(column))

for column in df.columns:
    if df[column].dtype in pl.NUMERIC_DTYPES:
        df = fill_null_with_zero(df, column)
    else:
        df = fill_null_with_empty(df, column)

df = df.with_columns(
    pl.concat_str(
        [
            pl.col("site_addrno"),
            pl.lit(" "),
            pl.col("site_addrdir"),
            pl.lit(" "),
            pl.col("site_addrstr"),
            pl.lit(" "),
            pl.col("site_addrsuf"),
            pl.lit(" "),
            pl.col("site_addrsuf2"),
        ]
    )
    .str.replace_all(r"\s+", " ")
    .alias("street_addr")
)

df = df.with_columns(
    pl.concat_str(
        [
            pl.col("owner_addrno"),
            pl.lit(" "),
            pl.col("owner_addradd"),
            pl.lit(" "),
            pl.col("owner_addrdir"),
            pl.lit(" "),
            pl.col("owner_addrstr"),
            pl.lit(" "),
            pl.col("owner_addrsuf"),
            pl.lit(" "),
            pl.col("owner_addrsuf2"),
            pl.lit(" "),
            pl.col("owner_unitno"),
        ]
    )
    .str.replace_all(r"\s+", " ")
    .alias("owner_addr")
)

df = df.with_columns(pl.lit("FULTON").alias("source_county"))

df.write_csv("all_parcels_fulton.csv")
df.write_parquet("all_parcels_fulton.parquet")
