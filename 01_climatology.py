"""
NOAA Coral Reef Watch Climatology Processor
===========================================

This script automates the downloading, processing, and masking of NOAA Coral Reef Watch 
Climatology data (NetCDF format). It clips global data to a specific region of interest 
and masks it using local shapefiles to produce XYZ text files for further analysis.

"""

import os
import requests
import glob
import warnings
import xarray as xr
import rioxarray
import geopandas as gpd
import pandas as pd

# Suppress warnings
warnings.filterwarnings("ignore")

# ==========================================
# 1. CONFIGURATION (USER SETTINGS)
# ==========================================

# Base Directory: Uses the current script's directory by default
BASE_DIR = os.getcwd()

# Input/Output Directories
DIRS = {
    "raw_input": os.path.join(BASE_DIR, "input_raw_climatology"),
    "shapefiles": os.path.join(BASE_DIR, "input_shapefiles"),
    "output": os.path.join(BASE_DIR, "output_xyz")
}

# NOAA Server Configuration
BASE_URL_NOAA = "https://www.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1_op/climatology/nc/"
TARGET_FILES = [
    "ct5km_climatology_v3.1.nc"
]

# Region of Interest (ROI) for Clipping (Latitude/Longitude)
# Example: Lombok Strait, Indonesia
ROI_BOUNDS = {
    "lat_min": -9.2,
    "lat_max": -8.0,
    "lon_min": 115.0,
    "lon_max": 116.5
}

# Shapefiles for Masking (filename only)
# Ensure these files exist in the 'input_shapefiles' folder
SITE_SHAPEFILES = {
    "GM": "gili_matra_buffer_5km.shp",
    "GN": "gita_nada_buffer_5km.shp",
    "NP": "nusa_penida_buffer_5km.shp"
}

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def setup_directories():
    """Create necessary directories if they don't exist."""
    print("--- Setting up directories ---")
    for key, path in DIRS.items():
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"Created: {path}")
        else:
            print(f"Exists: {path}")
    print("-" * 30)

def download_noaa_data():
    """Download climatology NetCDF files from NOAA server."""
    print("\n--- Checking Data Availability ---")
    
    for filename in TARGET_FILES:
        file_path = os.path.join(DIRS["raw_input"], filename)
        
        if os.path.exists(file_path):
            print(f"✅ {filename} already exists. Skipping download.")
        else:
            url = BASE_URL_NOAA + filename
            print(f"⬇️ Downloading: {filename} ...")
            print(f"   Source: {url}")
            
            try:
                response = requests.get(url, stream=True, timeout=120)
                response.raise_for_status()
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                print("   -> Download Complete!")
            except Exception as e:
                print(f"   ❌ Download Failed: {e}")
                print("   👉 Tip: You can manually download the file and place it in 'input_raw_climatology'")

def process_climatology():
    """Process NetCDF files: Clip to ROI -> Mask with Shapefiles -> Export to XYZ."""
    print("\n--- Starting Data Processing ---")
    
    # Check for .nc files
    raw_files = glob.glob(os.path.join(DIRS["raw_input"], "*.nc"))
    
    if not raw_files:
        print(f"❌ ERROR: No .nc files found in {DIRS['raw_input']}")
        return

    for i, file_path in enumerate(raw_files, 1):
        filename = os.path.basename(file_path)
        filename_no_ext = os.path.splitext(filename)[0]
        
        print(f"\n[{i}/{len(raw_files)}] Processing: {filename}")

        try:
            # Open Dataset
            ds = xr.open_dataset(file_path)

            # Normalize coordinates names
            if 'lat' in ds.coords:
                ds = ds.rename({'lat': 'latitude', 'lon': 'longitude'})
            
            # Ensure sorting for proper slicing
            ds = ds.sortby(['latitude', 'longitude'])

            # Handle Slicing (ROI)
            # Check if latitude is ascending or descending
            lat_slice = slice(ROI_BOUNDS["lat_min"], ROI_BOUNDS["lat_max"])
            if ds.latitude[0] > ds.latitude[-1]:
                lat_slice = slice(ROI_BOUNDS["lat_max"], ROI_BOUNDS["lat_min"])
            
            ds_roi = ds.sel(
                latitude=lat_slice, 
                longitude=slice(ROI_BOUNDS["lon_min"], ROI_BOUNDS["lon_max"])
            )

            # Validate Slice Result
            if ds_roi.dims['latitude'] == 0 or ds_roi.dims['longitude'] == 0:
                print(" -> ERROR: ROI Slice is empty. Check your coordinates.")
                continue

            # Set Spatial Dimensions
            ds_roi = ds_roi.rio.set_spatial_dims("longitude", "latitude")
            ds_roi = ds_roi.rio.write_crs("EPSG:4326", inplace=True)

            # Loop through Site Shapefiles
            for code, shp_name in SITE_SHAPEFILES.items():
                shp_path = os.path.join(DIRS["shapefiles"], shp_name)
                
                if not os.path.exists(shp_path):
                    print(f"    -> [SKIP] Shapefile not found: {shp_name}")
                    continue
                
                try:
                    # Load Shapefile
                    gdf = gpd.read_file(shp_path)
                    
                    # Ensure CRS matches
                    if gdf.crs != ds_roi.rio.crs:
                        gdf = gdf.to_crs(ds_roi.rio.crs)

                    # Clip (Mask) Data
                    clipped = ds_roi.rio.clip(gdf.geometry, gdf.crs, drop=True)
                    
                    # Convert to DataFrame for CSV/XYZ export
                    df = clipped.to_dataframe().reset_index()
                    
                    # Filter Columns
                    ignore_cols = ['latitude', 'longitude', 'spatial_ref', 'crs', 'band', 'time']
                    data_vars = [c for c in df.columns if c not in ignore_cols and c != 'month']
                    
                    if not data_vars:
                        print(f"    -> Warning: No data variables found for {code}")
                        continue
                    
                    target_var = data_vars[0]

                    # Export Logic (Monthly vs Static)
                    if 'month' in df.columns:
                        unique_months = df['month'].unique()
                        for m in unique_months:
                            df_month = df[df['month'] == m]
                            xyz = df_month[['longitude', 'latitude', target_var]].dropna()
                            
                            if xyz.empty: continue
                            
                            out_name = f"{code}_{filename_no_ext}_month_{int(m)}.xyz"
                            xyz.to_csv(os.path.join(DIRS["output"], out_name), 
                                       sep=' ', header=False, index=False)
                        print(f"    -> {code}: Success (12 Months exported)")
                    else:
                        xyz = df[['longitude', 'latitude', target_var]].dropna()
                        
                        if xyz.empty:
                            print(f"    -> {code}: Empty result (NaNs)")
                            continue
                        
                        out_name = f"{code}_{filename_no_ext}.xyz"
                        xyz.to_csv(os.path.join(DIRS["output"], out_name), 
                                   sep=' ', header=False, index=False)
                        print(f"    -> {code}: Success (Static exported)")

                except Exception as e_site:
                    print(f"    -> Error processing site {code}: {e_site}")

            ds.close()
            ds_roi.close()

        except Exception as e_file:
            print(f" -> ERROR opening file {filename}: {e_file}")

    print("\n=== All Processes Completed ===")
    print(f"Output files are located in: {DIRS['output']}")

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    setup_directories()
    download_noaa_data()
    process_climatology()
