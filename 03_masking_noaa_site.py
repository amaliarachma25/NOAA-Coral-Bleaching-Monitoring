"""
NOAA NetCDF to XYZ Converter (Masking Tool)
===========================================

This script processes NetCDF (.nc) raster files by clipping them against 
specific site Shapefiles (polygons). The output is converted into XYZ (ASCII) 
text files for further analysis.

Features:
- Batched processing of multiple NetCDF files.
- Auto-detection of coordinate names (lon/lat vs longitude/latitude).
- Spatial clipping using GeoPandas and RioXarray.
- Exports clean XYZ data (removing NaNs outside the mask).

"""

import os
import glob
import warnings
import xarray as xr
import rioxarray
import geopandas as gpd
import pandas as pd

# Suppress warnings
warnings.filterwarnings("ignore")

# ==========================================
# 1. CONFIGURATION
# ==========================================

# Base Directory (Current Folder)
BASE_DIR = os.getcwd()

# Directory Structure
DIRS = {
    # Place your clipped NetCDF files here (e.g., from the previous step)
    "input_nc": os.path.join(BASE_DIR, "02_Clip_Input"),
    
    # Place your Shapefiles (.shp, .shx, .dbf) here
    "input_shp": os.path.join(BASE_DIR, "input_shapefiles"),
    
    # Results will be saved here
    "output_xyz": os.path.join(BASE_DIR, "03_XYZ_Output")
}

# Shapefile Configuration
# Format -> "Code": "Filename.shp"
# The keys (GM, GN, NP) will be used as prefixes in the output filenames.
SITES = {
    "GM": "gili_matra_buffer_5km.shp",
    "GN": "gita_nada_buffer_5km.shp",
    "NP": "nusa_penida_buffer_5km.shp"
}

# Files to ignore (e.g., temporary layers)
IGNORE_PREFIXES = ["Clip_Indo", "Layer_"]

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def setup_directories():
    """Ensure output directory exists."""
    if not os.path.exists(DIRS["output_xyz"]):
        os.makedirs(DIRS["output_xyz"])
        print(f"Created output directory: {DIRS['output_xyz']}")

def process_masking():
    print("--- STARTING MASKING PROCESS (NetCDF -> XYZ) ---")
    print(f"Input: {DIRS['input_nc']}")
    print(f"Output: {DIRS['output_xyz']}\n")
    
    setup_directories()

    # Get all .nc files
    nc_files = glob.glob(os.path.join(DIRS["input_nc"], "*.nc"))
    
    if not nc_files:
        print(f"❌ ERROR: No .nc files found in {DIRS['input_nc']}")
        return

    # Loop through each Raster file
    for i, nc_path in enumerate(nc_files, 1):
        filename_full = os.path.basename(nc_path)
        filename_no_ext = os.path.splitext(filename_full)[0]
        
        # Check for ignored prefixes
        if any(filename_full.startswith(prefix) for prefix in IGNORE_PREFIXES):
            continue

        print(f"[{i}/{len(nc_files)}] Processing: {filename_full}")

        try:
            # 1. Open Raster
            ds = xr.open_dataset(nc_path)

            # Detect coordinate names (normalize to x, y)
            if 'lat' in ds.coords:
                ds = ds.rio.set_spatial_dims("lon", "lat")
                x_name, y_name = 'lon', 'lat'
            elif 'latitude' in ds.coords:
                ds = ds.rio.set_spatial_dims("longitude", "latitude")
                x_name, y_name = 'longitude', 'latitude'
            else:
                print("   -> Skip (Coordinates not recognized)")
                continue

            # Set CRS (NOAA usually uses EPSG:4326)
            ds = ds.rio.write_crs("EPSG:4326", inplace=True)

            # Loop through each Region (Shapefile)
            for code, shp_name in SITES.items():
                shp_path = os.path.join(DIRS["input_shp"], shp_name)
                
                if not os.path.exists(shp_path):
                    print(f"   -> [SKIP] Shapefile not found: {shp_name}")
                    continue

                try:
                    # 2. Open Shapefile
                    gdf = gpd.read_file(shp_path)
                    
                    # Ensure CRS matches
                    if gdf.crs != ds.rio.crs:
                        gdf = gdf.to_crs(ds.rio.crs)

                    # 3. Clip (Masking)
                    try:
                        clipped = ds.rio.clip(gdf.geometry, gdf.crs, drop=True)
                    except Exception:
                        print(f"   -> {code}: No overlap found (Skipping)")
                        continue
                    
                    # 4. Convert to XYZ (Pandas DataFrame)
                    df = clipped.to_dataframe().reset_index()
                    
                    # Identify data variable column (ignore coords and metadata)
                    ignore_cols = [x_name, y_name, 'time', 'spatial_ref', 'crs', 'band']
                    data_vars = [col for col in df.columns if col not in ignore_cols]
                    
                    if not data_vars:
                        print(f"   -> Warning: No data variable in {code}")
                        continue
                        
                    target_var = data_vars[0]
                    
                    # Filter only X, Y, Value
                    xyz_df = df[[x_name, y_name, target_var]]
                    
                    # Remove NaNs (masked areas)
                    xyz_df = xyz_df.dropna()

                    if xyz_df.empty:
                        print(f"   -> {code}: Empty result (All NaNs)")
                        continue

                    # 5. Save to .XYZ
                    # Output format: CODE_OriginalName.xyz
                    output_filename = f"{code}_{filename_no_ext}.xyz"
                    output_path = os.path.join(DIRS["output_xyz"], output_filename)
                    
                    # Save as space-separated values, no header, no index
                    xyz_df.to_csv(output_path, sep=' ', header=False, index=False)
                    
                    print(f"   -> OK: {output_filename}")

                except Exception as e_shp:
                    print(f"   -> Error processing region {code}: {e_shp}")

            ds.close()

        except Exception as e_file:
            print(f"   -> Error opening file: {e_file}")

    print("\n=== PROCESSING COMPLETE ===")

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    process_masking()
