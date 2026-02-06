"""
NOAA Coral Bleaching Integrated Analysis Tool
=============================================

This script performs the final stage of the analysis pipeline:
1. Calculates Climatology Statistics (MMM - Maximum Monthly Mean) from global NetCDF data.
2. Analyzes daily XYZ data (SST, HotSpot, DHW) for specific sites.
3. Generates a comprehensive text report containing:
   - Site Metadata
   - Climatology Baselines
   - Daily Bleaching Alert Area (BAA) and Degree Heating Weeks (DHW).

Methodology based on NOAA Coral Reef Watch v3.1.
"""

import pandas as pd
import numpy as np
import os
import datetime
import re
from collections import deque
import xarray as xr
import rioxarray
import geopandas as gpd
import warnings

# Suppress warnings
warnings.filterwarnings("ignore")

# ==========================================
# 1. CONFIGURATION
# ==========================================

# Base Directory (Current Folder)
BASE_DIR = os.getcwd()

# Directory Structure
DIRS = {
    # INPUT 1: Raw Global Climatology NetCDF
    "clim_input": os.path.join(BASE_DIR, "01_Global_Input"),
    
    # INPUT 2: Shapefiles for masking
    "shp_input": os.path.join(BASE_DIR, "input_shapefiles"),
    
    # INPUT 3: Daily masked XYZ files (Output from previous script)
    "xyz_input": os.path.join(BASE_DIR, "03_XYZ_Output"),
    
    # OUTPUT: Final Report Location
    "final_output": os.path.join(BASE_DIR, "04_Final_Reports")
}

# Climatology File Name
CLIM_FILENAME = "ct5km_climatology_v3.1.nc"

# Site Configuration (Code: Shapefilename)
SITES = {
    "GM": "gili_matra_buffer_5km.shp",
    "GN": "gita_nada_buffer_5km.shp",
    "NP": "nusa_penida_buffer_5km.shp"
}

# Site Full Names for Report
SITE_FULLNAMES = {
    "GM": "Gili Matra", 
    "GN": "Gita Nada", 
    "NP": "Nusa Penida"
}

# Region of Interest (ROI) for Climatology Slicing
ROI_BOUNDS = {
    "lat_min": -9.2, "lat_max": -8.2,
    "lon_min": 115.3, "lon_max": 116.3
}

# NetCDF Variable Names for Months
MONTH_VARS = [
    'sst_clim_january', 'sst_clim_february', 'sst_clim_march', 
    'sst_clim_april', 'sst_clim_may', 'sst_clim_june',
    'sst_clim_july', 'sst_clim_august', 'sst_clim_september', 
    'sst_clim_october', 'sst_clim_november', 'sst_clim_december'
]

# ==========================================
# 2. MODULE: CLIMATOLOGY CALCULATION
# ==========================================
def calculate_climatology_data():
    """
    Calculates Maximum Monthly Mean (MMM) and Monthly Means from NetCDF.
    Returns a dictionary of results per site.
    """
    print("\n" + "="*50)
    print("   STEP 1: CALCULATING CLIMATOLOGY (NetCDF)")
    print("="*50)

    input_path = os.path.join(DIRS["clim_input"], CLIM_FILENAME)

    if not os.path.exists(input_path):
        print(f"❌ ERROR: Climatology file not found: {input_path}")
        print("   Please download 'ct5km_climatology_v3.1.nc' into '01_Global_Input'.")
        return {}

    try:
        ds = xr.open_dataset(input_path)
        
        # Normalize coordinates
        if 'lat' in ds.coords: ds = ds.rename({'lat': 'latitude', 'lon': 'longitude'})
        ds = ds.sortby(['latitude', 'longitude'])

        # Slice ROI to save RAM
        print("✂️  Slicing ROI...")
        lat_slice = slice(ROI_BOUNDS["lat_min"], ROI_BOUNDS["lat_max"])
        if ds.latitude[0] > ds.latitude[-1]: # Handle descending latitude
            lat_slice = slice(ROI_BOUNDS["lat_max"], ROI_BOUNDS["lat_min"])
            
        ds_roi = ds.sel(latitude=lat_slice, longitude=slice(ROI_BOUNDS["lon_min"], ROI_BOUNDS["lon_max"]))
        ds_roi = ds_roi.rio.set_spatial_dims("longitude", "latitude").rio.write_crs("EPSG:4326")
        
        clim_results = {}

        for code, shp_name in SITES.items():
            shp_path = os.path.join(DIRS["shp_input"], shp_name)
            
            if not os.path.exists(shp_path):
                print(f"⚠️  Shapefile not found: {shp_name}")
                continue

            print(f"🔄 Processing: {code}")
            gdf = gpd.read_file(shp_path)
            
            if gdf.crs != ds_roi.rio.crs:
                gdf = gdf.to_crs(ds_roi.rio.crs)

            try:
                # Clip NetCDF with Shapefile
                clipped = ds_roi.rio.clip(gdf.geometry, gdf.crs, drop=True)
                
                site_means = []
                # Loop 12 Months
                for var_name in MONTH_VARS:
                    if var_name in clipped.data_vars:
                        val = clipped[var_name].mean(dim=['latitude', 'longitude'], skipna=True).item()
                        site_means.append(val)
                    else:
                        site_means.append(np.nan)

                clean_means = [x for x in site_means if not np.isnan(x)]
                
                if len(clean_means) == 12:
                    mmm_value = max(clean_means)
                    clim_results[code] = {
                        "mmm": mmm_value,
                        "monthly_means": clean_means
                    }
                    print(f"   ✅ {code} MMM: {mmm_value:.4f}")
                else:
                    print(f"   ❌ {code}: Incomplete monthly data.")

            except Exception as e:
                print(f"   ❌ Error processing {code}: {e}")

        ds.close()
        return clim_results

    except Exception as e:
        print(f"CRITICAL ERROR CLIMATOLOGY: {e}")
        return {}

# ==========================================
# 3. MODULE: DAILY ANALYSIS (LOGIC)
# ==========================================
class RegionAnalyzer:
    def __init__(self, name, code, climatology_data):
        self.name = name
        self.code = code
        self.stress_window = deque(maxlen=84) # 12 weeks (84 days) for DHW
        self.baa_window = deque(maxlen=7)     # 7 days for BAA Composite
        self.center_lat = 0.0
        self.center_lon = 0.0
        self.coord_set = False
        
        # Load Climatology Data
        self.mmm = climatology_data.get('mmm', 0.0)
        self.monthly_means = climatology_data.get('monthly_means', [0.0]*12)

    def process_day(self, date_obj, file_hs, file_sst=None, file_ssta=None):
        try:
            # 1. Read HotSpot (HS) - Mandatory
            df_hs = pd.read_csv(file_hs, sep='\s+', header=None, names=['lon', 'lat', 'val'])
            df_hs = df_hs.dropna()
            if df_hs.empty: return None

            # Set polygon center coordinates (once)
            if not self.coord_set:
                self.center_lon = df_hs['lon'].mean()
                self.center_lat = df_hs['lat'].mean()
                self.coord_set = True

            # 2. Calculate 90th Percentile HS
            hs_values = df_hs['val'].values
            hs_90 = np.percentile(hs_values, 90)
            
            # Find index of pixel closest to 90th percentile to get SST/SSTA at that specific point
            idx_p90 = (np.abs(hs_values - hs_90)).argmin()
            
            # 3. Get SST & SSTA (Optional but recommended)
            sst_val = -999.0; sst_min = -999.0; sst_max = -999.0; ssta_val = -999.0
            
            if file_sst and os.path.exists(file_sst):
                try:
                    df_sst = pd.read_csv(file_sst, sep='\s+', header=None, names=['lon', 'lat', 'val'])
                    sst_val = df_sst['val'].iloc[idx_p90]
                    sst_min = df_sst['val'].min()
                    sst_max = df_sst['val'].max()
                except: pass
            
            if file_ssta and os.path.exists(file_ssta):
                try:
                    df_ssta = pd.read_csv(file_ssta, sep='\s+', header=None, names=['lon', 'lat', 'val'])
                    ssta_val = df_ssta['val'].iloc[idx_p90]
                except: pass

            # 4. Calculate DHW (Accumulation)
            # DHW accumulates only if HS >= 1.0 degree C
            daily_stress = 0.0
            if hs_90 >= 1.0:
                daily_stress = hs_90 / 7.0 # Convert daily HS to weekly equivalent
            
            self.stress_window.append(daily_stress)
            current_dhw = sum(self.stress_window)

            # 5. Calculate BAA (Bleaching Alert Area)
            # Logic: 0=No Stress, 1=Watch, 2=Warning, 3=Alert Lvl 1, 4=Alert Lvl 2
            daily_alert_level = 0
            
            if hs_90 <= 0.0:
                daily_alert_level = 0
            elif 0.0 < hs_90 < 1.0:
                daily_alert_level = 1
            else: # hs_90 >= 1.0
                if current_dhw < 4.0:
                    daily_alert_level = 2
                elif 4.0 <= current_dhw < 8.0:
                    daily_alert_level = 3
                elif current_dhw >= 8.0:
                    daily_alert_level = 4

            # Use 7-day rolling maximum for BAA
            self.baa_window.append(daily_alert_level)
            final_baa = max(self.baa_window) if self.baa_window else 0

            return {
                "date": date_obj,
                "sst_min": sst_min,
                "sst_max": sst_max,
                "sst_90": sst_val,
                "ssta_90": ssta_val,
                "hs_90": max(0, hs_90),
                "dhw": current_dhw,
                "baa": final_baa
            }
        except Exception as e:
            print(f"Error reading daily file {file_hs}: {e}")
            return None

# ==========================================
# 4. MAIN EXECUTION FLOW
# ==========================================
def main():
    if not os.path.exists(DIRS["final_output"]):
        os.makedirs(DIRS["final_output"])

    # --- PART 1: CLIMATOLOGY ---
    clim_data_store = calculate_climatology_data()
    
    if not clim_data_store:
        print("⚠️  Warning: Climatology Calculation failed or returned empty.")
        print("   Continuing with default values (0.0).")

    # --- PART 2: DAILY ANALYSIS ---
    print("\n" + "="*50)
    print(f"   STEP 2: DAILY ANALYSIS (Input: {DIRS['xyz_input']})")
    print("="*50)
    
    if not os.path.exists(DIRS["xyz_input"]):
        print(f"❌ Input folder not found: {DIRS['xyz_input']}")
        print("   Please run the 'masking_to_xyz.py' script first.")
        return

    # Map files by Region -> Date -> Type (HS/SST/SSTA)
    files_map = {} 
    
    for f in os.listdir(DIRS["xyz_input"]):
        if not f.endswith(".xyz"): continue
        
        name_lower = f.lower()
        
        # Detect Region
        region = None
        for code in SITES.keys():
            if f"{code.lower()}_" in name_lower:
                region = code
                break
        if not region: continue
            
        # Detect Date
        date_match = re.search(r"(\d{8})", f)
        if not date_match: continue
        date_str = date_match.group(1)
        
        # Detect Type
        ftype = "UNKNOWN"
        if "hs" in name_lower and "hotspot" not in name_lower: ftype = "HS"
        elif "hotspot" in name_lower: ftype = "HS"
        elif "ssta" in name_lower: ftype = "SSTA"
        elif "sst" in name_lower: ftype = "SST"
        
        if region not in files_map: files_map[region] = {}
        if date_str not in files_map[region]: files_map[region][date_str] = {}
        
        files_map[region][date_str][ftype] = os.path.join(DIRS["xyz_input"], f)

    # --- PART 3: GENERATE REPORTS ---
    for code, dates_dict in files_map.items():
        region_fullname = SITE_FULLNAMES.get(code, code)
        print(f"\n📈 Processing Site: {region_fullname} ({code})")
        
        # Retrieve climatology data for this site
        site_clim = clim_data_store.get(code, {'mmm': 0.0, 'monthly_means': [0.0]*12})
        
        analyzer = RegionAnalyzer(region_fullname, code, site_clim)
        sorted_dates = sorted(dates_dict.keys())
        results_buffer = []

        # Process Day by Day
        for d_str in sorted_dates:
            files = dates_dict[d_str]
            if "HS" not in files: continue # HotSpot file is mandatory
            
            dt_obj = datetime.datetime.strptime(d_str, "%Y%m%d")
            data = analyzer.process_day(
                dt_obj, 
                files["HS"], 
                files.get("SST"), 
                files.get("SSTA")
            )
            if data: results_buffer.append(data)

        if not results_buffer:
            print(f"   -> No valid data found for {code}")
            continue

        # Write Report File
        output_filename = os.path.join(DIRS["final_output"], f"{region_fullname.replace(' ','_')}_NOAA_Report.txt")
        
        with open(output_filename, "w") as f:
            # HEADER
            f.write("Name:\n")
            f.write(f"{region_fullname}\n\n")
            f.write("Polygon Middle Longitude:\n")
            f.write(f"{analyzer.center_lon:.4f} \n\n")
            f.write("Polygon Middle Latitude:\n")
            f.write(f"{analyzer.center_lat:.4f} \n\n")
            
            # CLIMATOLOGY
            f.write("Averaged Maximum Monthly Mean:\n")
            f.write(f"{analyzer.mmm:.4f}\n\n")
            f.write("Averaged Monthly Mean (Jan-Dec):\n")
            means_str = " ".join([f"{val:.4f}" for val in analyzer.monthly_means])
            f.write(f"{means_str}\n\n")
            
            # DATES
            first_dt = results_buffer[0]['date']
            dhw_valid_dt = first_dt + datetime.timedelta(weeks=12) 
            baa_valid_dt = dhw_valid_dt + datetime.timedelta(days=7) 
            
            f.write("First Valid DHW Date:\n")
            f.write(f"{dhw_valid_dt.strftime('%Y %m %d')}\n\n")
            f.write("First Valid BAA Date:\n")
            f.write(f"{baa_valid_dt.strftime('%Y %m %d')}\n\n")
            
            # DATA TABLE
            header = "YYYY MM DD SST_MIN SST_MAX SST@90th_HS SSTA@90th_HS 90th_HS>0 DHW_from_90th_HS>1 BAA_7day_max"
            f.write(header + "\n")
            
            for row in results_buffer:
                d = row['date']
                line = (
                    f"{d.year:4d} {d.month:02d} {d.day:02d} "
                    f"{row['sst_min']:7.4f} {row['sst_max']:7.4f} "
                    f"{row['sst_90']:11.4f} {row['ssta_90']:12.4f} "
                    f"{row['hs_90']:9.4f} "
                    f"{row['dhw']:18.4f} "
                    f"{row['baa']:12d}"
                )
                f.write(line + "\n")
                
        print(f"✅ Report saved: {output_filename}")

    print("\n" + "="*50)
    print("ALL PROCESSES COMPLETED")
    print("="*50)

if __name__ == "__main__":
    main()
