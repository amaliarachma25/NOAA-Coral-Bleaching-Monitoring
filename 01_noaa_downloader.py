"""
NOAA Coral Reef Watch Daily Data Downloader
===========================================

This script automates the downloading of daily NetCDF files (SST, SSTA, HotSpot, DHW) 
from the NOAA Coral Reef Watch v3.1 server.

Features:
- Handles inconsistent naming conventions on the NOAA server (e.g., 'coraltemp' vs 'sst').
- Checks if files already exist to avoid re-downloading.
- Automatically creates output directories.

"""

import requests
import os
from datetime import datetime, timedelta
import time

# ==========================================
# 1. CONFIGURATION
# ==========================================

# Base Directory: Creates a 'downloaded_data' folder in the same place this script is run
BASE_DIR = os.getcwd()
SAVE_DIR = os.path.join(BASE_DIR, "01_Global_Input")

# Ensure output directory exists
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR, exist_ok=True)
    print(f"Directory created: {SAVE_DIR}")
else:
    print(f"Output directory: {SAVE_DIR}")

# DATE RANGE (YYYY, MM, DD) -> Change this as needed
START_DATE = datetime(2026, 1, 20)
END_DATE   = datetime(2026, 1, 21) 

# NOAA Base URL
BASE_URL = "https://www.star.nesdis.noaa.gov/pub/socd/mecb/crw/data/5km/v3.1_op/nc/v1.0/daily"

# Configuration Mapping: 
# "Key": ["Server Folder", "Server Filename Prefix", "Local Output Prefix"]
VAR_CONFIG = {
    "sst":  ["sst",  "coraltemp",  "NOAA_SST"],   # NOAA uses 'coraltemp' for SST files
    "ssta": ["ssta", "ct5km_ssta", "NOAA_SSTA"],
    "hs":   ["hs",   "ct5km_hs",   "NOAA_HS"],
    "dhw":  ["dhw",  "ct5km_dhw",  "NOAA_DHW"]
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
}

# ==========================================
# 2. DOWNLOAD FUNCTION
# ==========================================

def run_downloader():
    print(f"\n--- STARTING DOWNLOAD ---")
    print(f"Target: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    print(f"Location: {SAVE_DIR}\n")
    
    current_date = START_DATE
    while current_date <= END_DATE:
        # Date formats
        date_str_url = current_date.strftime("%Y%m%d") # Format for URL: 20260120
        year = current_date.strftime("%Y")
        date_disp = current_date.strftime('%Y-%m-%d')
        
        print(f"[{date_disp}] Checking data...", end=" ")

        for var_key, config in VAR_CONFIG.items():
            folder_server = config[0]
            prefix_server = config[1]
            prefix_local  = config[2]

            # Construct Local Filename
            local_filename = f"{prefix_local}_{date_str_url}.nc"
            local_path = os.path.join(SAVE_DIR, local_filename)

            # --- CHECK IF FILE EXISTS ---
            if os.path.exists(local_path) and os.path.getsize(local_path) > 1000:
                # print(f"[{var_key.upper()} Exists]", end=" ")
                continue
            
            # --- CONSTRUCT URL ---
            filename_server = f"{prefix_server}_v3.1_{date_str_url}.nc"
            url = f"{BASE_URL}/{folder_server}/{year}/{filename_server}"

            try:
                response = requests.get(url, headers=HEADERS, stream=True, timeout=60)
                
                if response.status_code == 200:
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=1024*1024):
                            f.write(chunk)
                    print(f"[{var_key.upper()} OK]", end=" ")
                elif response.status_code == 404:
                    print(f"[{var_key.upper()} Not Found]", end=" ")
                else:
                    print(f"[{var_key.upper()} Err {response.status_code}]", end=" ")
                    
            except Exception as e:
                print(f"[{var_key.upper()} Error]", end=" ")
                
            time.sleep(0.2) # Polite delay

        print("") # New line
        current_date += timedelta(days=1)

    print("\n--- DOWNLOAD COMPLETE ---")

if __name__ == "__main__":
    run_downloader()
