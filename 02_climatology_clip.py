"""
NOAA Climatology Statistics Calculator
======================================

This script calculates the Maximum Monthly Mean (MMM) and Monthly Mean Climatology 
for specific sites using Shapefiles and NOAA Coral Reef Watch v3.1 NetCDF data.

Workflow:
1. Loads NOAA Climatology NetCDF file.
2. Slices the data to a specific Region of Interest (ROI) to save memory.
3. Iterates through site Shapefiles to clip the raster data.
4. Calculates spatial averages for each month (Jan-Dec).
5. Determines the MMM (max of monthly means).
6. Exports a formatted text report.

"""

import os
import glob
import warnings
import xarray as xr
import rioxarray
import geopandas as gpd
import numpy as np
import pandas as pd

# Suppress warnings
warnings.filterwarnings("ignore")

# ==========================================
# 1. CONFIGURATION
# ==========================================

# Base Directory (Current Folder)
BASE_DIR = os.getcwd()

# Directories
DIRS = {
    "nc_input": os.path.join(BASE_DIR, "01_Global_Input"),   # Place .nc file here
    "shp_input": os.path.join(BASE_DIR, "input_shapefiles"), # Place .shp files here
    "output": os.path.join(BASE_DIR, "01_Climatology_Output") # Results go here
}

# Target NetCDF File
NC_FILENAME = "ct5km_climatology_v3.1.nc"

# Output Report Filename
REPORT_FILENAME = "mmm_mean_site_FINAL.txt"

# Region of Interest (ROI) for Slicing (Lombok Area)
# Reducing the area reduces RAM usage
ROI_BOUNDS = {
    "lat_min": -9.2,
    "lat_max": -8.2,
    "lon_min": 115.3,
    "lon_max": 116.3
}

# Site Configuration (Code: Shapefilename)
# Ensure these files exist in 'input_shapefiles'
SITES = {
    "GM": "gili_matra_buffer_5km.shp",
    "GN": "gita_nada_buffer_5km.shp",
    "NP": "nusa_penida_buffer_5km.shp"
}

# NOAA CRW Variable Names (Do not change unless version changes)
MONTH_VARS = [
    'sst_clim_january', 'sst_clim_february', 'sst_clim_march', 
    'sst_clim_april', 'sst_clim_may', 'sst_clim_june',
    'sst_clim_july', 'sst_clim_august', 'sst_clim_september', 
    'sst_clim_october', 'sst_clim_november', 'sst_clim_december'
]

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def setup_directories():
    """Ensure output directory exists."""
    if not os.path.exists(DIRS["output"]):
        os.makedirs(DIRS["output"])
        print(f"Created output directory: {DIRS['output']}")

def calculate_site_climatology():
    print("--- STARTING CLIMATOLOGY CALCULATION ---")
    setup_directories()

    # Define File Paths
    input_nc_path = os.path.join(DIRS["nc_input"], NC_FILENAME)
    output_report_path = os.path.join(DIRS["output"], REPORT_FILENAME)

    # 1. Check Input File
    if not os.path.exists(input_nc_path):
        print(f"❌ ERROR: NetCDF file not found: {input_nc_path}")
        print(f"   Please download '{NC_FILENAME}' and place it in '01_Global_Input'.")
        return

    print(f"📂 Opening dataset: {NC_FILENAME}")
    
    try:
        ds = xr.open_dataset(input_nc_path)

        # 2. Normalize Dimensions
        if 'lat' in ds.coords:
            ds = ds.rename({'lat': 'latitude', 'lon': 'longitude'})
        ds = ds.sortby(['latitude', 'longitude'])

        print("✂️  Slicing Region of Interest (ROI) to save RAM...")
        
        # Handle Lat/Lon Slicing (Ascending/Descending check)
        lat_slice = slice(ROI_BOUNDS["lat_min"], ROI_BOUNDS["lat_max"])
        if ds.latitude[0] > ds.latitude[-1]:
            lat_slice = slice(ROI_BOUNDS["lat_max"], ROI_BOUNDS["lat_min"])
        
        ds_roi = ds.sel(
            latitude=lat_slice, 
            longitude=slice(ROI_BOUNDS["lon_min"], ROI_BOUNDS["lon_max"])
        )

        # Set CRS
        ds_roi = ds_roi.rio.set_spatial_dims("longitude", "latitude").rio.write_crs("EPSG:4326")

        results = []
        print("\n--- PROCESSING SITES ---")

        # 3. Process Each Site
        for code, shp_name in SITES.items():
            shp_path = os.path.join(DIRS["shp_input"], shp_name)
            
            if not os.path.exists(shp_path):
                print(f"⚠️  [SKIP] Shapefile not found: {shp_name}")
                continue

            print(f"🔄 Processing: {code}")
            
            try:
                # Load Shapefile
                gdf = gpd.read_file(shp_path)
                
                # Align CRS
                if gdf.crs != ds_roi.rio.crs:
                    gdf = gdf.to_crs(ds_roi.rio.crs)
                
                # Clip Raster
                try:
                    clipped = ds_roi.rio.clip(gdf.geometry, gdf.crs, drop=True)
                except Exception as e_clip:
                    print(f"   -> Error clipping: {e_clip}")
                    continue
                
                site_means = []

                # Calculate Mean for 12 Months
                for var_name in MONTH_VARS:
                    if var_name in clipped.data_vars:
                        # Spatial Mean
                        val = clipped[var_name].mean(dim=['latitude', 'longitude'], skipna=True).item()
                        site_means.append(val)
                    else:
                        print(f"   ⚠️ Warning: Variable {var_name} missing.")
                        site_means.append(np.nan)

                # Calculate MMM
                clean_means = [x for x in site_means if not np.isnan(x)]
                
                if len(clean_means) == 12:
                    mmm_value = max(clean_means)
                    results.append({
                        'name': code,
                        'mmm': mmm_value,
                        'means': clean_means
                    })
                    print(f"   ✅ Success. MMM: {mmm_value:.4f}")
                else:
                    print("   ❌ Failed: Incomplete monthly data.")

            except Exception as e:
                print(f"   ❌ Error processing {code}: {e}")

        # 4. Write Output Report
        if results:
            with open(output_report_path, 'w') as f:
                for res in results:
                    f.write(f"SITE: {res['name']}\n")
                    f.write("Averaged Maximum Monthly Mean:\n")
                    f.write(f"{res['mmm']:.4f}\n\n")
                    
                    f.write("Averaged Monthly Mean (Jan-Dec):\n")
                    means_str = " ".join([f"{val:.4f}" for val in res['means']])
                    f.write(f"{means_str}\n")
                    
                    f.write("\n" + "="*40 + "\n\n")
            
            print(f"\n🎉 DONE! Report saved to: {output_report_path}")
        else:
            print("\n⚠️ No results generated.")

        ds.close()
        ds_roi.close()

    except Exception as e_main:
        print(f"CRITICAL ERROR: {e_main}")

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    calculate_site_climatology()
