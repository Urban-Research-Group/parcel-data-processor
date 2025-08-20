import pandas as pd
import geopandas as gpd
import os
os.chdir('/Users/melissajuarezc/Documents/GITHUB REPOS/parcel-data-processor/')

# --------- FULTON SCRIPT ---------

# 1. Read the digest csv
FILES_PATH = 'output/fulton/3-ownership_keys/'
fulton_df = pd.read_csv(
    FILES_PATH + "fulton_digest_full_final.csv",
    dtype={
        "taxyr": "Int64",
        "parid": str,
        "nbhd": str,
        "situs_adrno": "Int64",
        "situs_adrdir": str,
        "situs_adrstr": str,
        "situs_adrsuf": str,
        "situs_adrsuf2": str,
        "situs_cityname": str,
        "class": str,
        "luc": str,
        "livunit": "Int64",
        "calcacres": float,
        "ofcard": "Int64",
        "chgrsn": str,
        "taxdist": str,
        "own1": str,
        "own2": str,
        "owner_adrno": "Int64",
        "owner_adrdir": str,
        "owner_adrstr": str,
        "owner_adrsuf": str,
        "owner_adrsuf2": str,
        "owner_cityname": str,
        "statecode": str,
        "country": str,
        "unitno": str,
        "zip1": str,
        "reascd": str,
        "spcflg": str,
        "aprland": float,
        "aprbldg": float,
        "revcode": "Int64",
        "revreas": str,
        "revland": float,
        "revbldg": float,
        "revtot": str,
        "aprtot": float,
        "card": "Int64",
        "stories": float,
        "extwall": "Int64",
        "yrblt": "Int64",
        "effyr": "Int64",
        "sf": "Int64",
        "grade": str,
        "cdu": str,
        "pctcomp": float,
        "cur": str,
        "note1": str,
        "note2": str,
        "mod_own_adrstr": str,
        "mod_unitno": str,
        "owner_addr": str,
        "mod_own_adrno": str,
        "own_corp_flag": bool,
        "rental_flag": bool
    }
)

# 2. Read the GeoJSON
gdf = gpd.read_file('raw-geo-files/Fulton_Tax_Parcels_2022.geojson')
gdf['ParcelID'] = gdf['ParcelID'].astype(str).str.replace(' ', '').str.upper()

# 3. Project and compute centroid
gdf = gdf.to_crs(epsg=3857)
gdf['centroid'] = gdf.geometry.centroid

gdf = gdf.set_geometry('centroid').to_crs(epsg=4326)
gdf['longitude'] = gdf.geometry.x
gdf['latitude'] = gdf.geometry.y

gdf = gdf.set_geometry('geometry')
gdf_coords = gdf[['ParcelID', 'longitude', 'latitude']]

# 4. Merge
fulton_merged = fulton_df.merge(gdf_coords, left_on='parid', right_on='ParcelID', how='left')

# 5. Print merge stats
matched = fulton_merged[~fulton_merged['longitude'].isna()]
missing = fulton_merged[fulton_merged['longitude'].isna()]
print(f"Matched: {len(matched)} records")
print(f"Unmatched: {len(missing)} records ({len(missing)/len(fulton_merged)*100:.2f}%)")

# 6. Save
fulton_merged.to_csv("output/fulton/4-geocoded/fulton_digest_geo_full_final.csv", index=False)
