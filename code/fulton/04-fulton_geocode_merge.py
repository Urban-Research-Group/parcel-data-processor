import pandas as pd
import geopandas as gpd

# --------- GWINNETT SCRIPT ---------

# 1. Read the digest CSV
csv_path = "/content/gwinnett_digest_full_final.csv"
df = pd.read_csv(csv_path, low_memory=False)

# 2. Standardize parcel ID column (remove spaces and uppercase)
df['TAXPIN'] = df['pin'].astype(str).str.replace(' ', '').str.upper()

# 3. Read the full GeoJSON file
gdf = gpd.read_file("/content/Gwinnett_Property_and_Tax_2024.geojson")
gdf['TAXPIN'] = gdf['TAXPIN'].astype(str).str.replace(' ', '').str.upper()

# 4. Project to planar CRS and calculate centroid
gdf = gdf.to_crs(epsg=3857)
gdf['centroid'] = gdf.geometry.centroid

# 5. Reproject back to lat/lon (EPSG:4326)
gdf = gdf.set_geometry('centroid').to_crs(epsg=4326)
gdf['longitude'] = gdf.geometry.x
gdf['latitude'] = gdf.geometry.y

# 6. Restore original geometry if needed
gdf = gdf.set_geometry('geometry')

# 7. Extract coordinates for merge
gdf_coords = gdf[['TAXPIN', 'longitude', 'latitude']]

# 8. Merge with digest
df_merged = df.merge(gdf_coords, on='TAXPIN', how='left')

# 9. Print merge stats
matched = df_merged[~df_merged['longitude'].isna()]
missing = df_merged[df_merged['longitude'].isna()]
print(f"Matched: {len(matched)} records")
print(f"Unmatched: {len(missing)} records ({len(missing)/len(df_merged)*100:.2f}%)")

# 10. Save result
df_merged.to_csv("/content/gwinnett_digest_with_coords.csv", index=False)

# --------- FULTON SCRIPT ---------

# 1. Read the digest XLSX
fulton_df = pd.read_excel("/content/fulton_ALL_ownership_2011_2022.csv.xlsx")
fulton_df['TAXPIN'] = fulton_df['parid'].astype(str).str.replace(' ', '').str.upper()

# 2. Read the GeoJSON
gdf = gpd.read_file("/content/Fulton_Tax_Parcels_2022.geojson")
gdf['ParcelID'] = gdf['ParcelID'].astype(str).str.replace(' ', '').str.upper()
gdf['TAXPIN'] = gdf['ParcelID']

# 3. Project and compute centroid
gdf = gdf.to_crs(epsg=3857)
gdf['centroid'] = gdf.geometry.centroid

gdf = gdf.set_geometry('centroid').to_crs(epsg=4326)
gdf['longitude'] = gdf.geometry.x
gdf['latitude'] = gdf.geometry.y

gdf = gdf.set_geometry('geometry')
gdf_coords = gdf[['TAXPIN', 'longitude', 'latitude']]

# 4. Merge
fulton_merged = fulton_df.merge(gdf_coords, on='TAXPIN', how='left')

# 5. Print merge stats
matched = fulton_merged[~fulton_merged['longitude'].isna()]
missing = fulton_merged[fulton_merged['longitude'].isna()]
print(f"Matched: {len(matched)} records")
print(f"Unmatched: {len(missing)} records ({len(missing)/len(fulton_merged)*100:.2f}%)")

# 6. Save
fulton_merged.to_csv("/content/fulton_digest_with_coords.csv", index=False)
