import requests
import os
from datetime import datetime, timedelta
import time

# --- 1. KONFIGURASI ---
save_dir = r"D:\magang\coral\python_coral_env"
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

# RENTANG TANGGAL (1 Jan - 22 Jan)
start_date = datetime(2026, 1, 20)
end_date   = datetime(2026, 1, 21) 

# --- 2. KONFIGURASI NAMA FILE SPESIAL ---
base_url = "https://www.star.nesdis.noaa.gov/pub/socd/mecb/crw/data/5km/v3.1_op/nc/v1.0/daily"

# Dictionary ini mengatur: { "Kunci": ["Nama Folder Server", "Prefix Nama File Server", "Nama File Lokal"] }
var_config = {
    # SST Punya nama file aneh: "coraltemp" bukan "ct5km_sst"
    "sst":  ["sst",  "coraltemp",  "NOAA_SST"],  
    
    # Yang lain normal: "ct5km_..."
    "ssta": ["ssta", "ct5km_ssta", "NOAA_SSTA"],
    "hs":   ["hs",   "ct5km_hs",   "NOAA_HS"],
    "dhw":  ["dhw",  "ct5km_dhw",  "NOAA_DHW"]
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def download_final_fix():
    print(f"--- DOWNLOAD FINAL (FIX SSTA vs CORALTEMP) ---")
    print(f"Target: {start_date.strftime('%Y-%m-%d')} s/d {end_date.strftime('%Y-%m-%d')}")
    
    current_date = start_date
    while current_date <= end_date:
        date_str_url = current_date.strftime("%Y%m%d") # 20260101
        year = current_date.strftime("%Y")
        date_disp = current_date.strftime('%Y-%m-%d')
        
        print(f"[{date_disp}] Cek kelengkapan data...")

        for var_key, config in var_config.items():
            folder_server = config[0] # sst, ssta, dll
            prefix_server = config[1] # coraltemp, ct5km_ssta, dll
            prefix_local  = config[2] # NOAA_SST, NOAA_SSTA, dll

            # Nama file lokal yang kita inginkan
            local_filename = f"{prefix_local}_{date_str_url}.nc"
            local_path = os.path.join(save_dir, local_filename)

            # --- CEK APAKAH SUDAH ADA? ---
            if os.path.exists(local_path) and os.path.getsize(local_path) > 1000:
                # File sudah ada dan sehat -> SKIP
                continue
            
            # --- KONSTRUKSI URL YANG BENAR ---
            # Format: .../daily/{folder}/{year}/{prefix}_v3.1_{date}.nc
            filename_server = f"{prefix_server}_v3.1_{date_str_url}.nc"
            url = f"{base_url}/{folder_server}/{year}/{filename_server}"

            print(f"   -> Download {var_key.upper()} ({filename_server}) ... ", end='', flush=True)

            try:
                response = requests.get(url, headers=headers, stream=True, timeout=60)
                
                if response.status_code == 200:
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=1024*1024):
                            f.write(chunk)
                    print("BERHASIL!")
                elif response.status_code == 404:
                    print(f"GAGAL (404 Not Found di Server)")
                else:
                    print(f"GAGAL (Status: {response.status_code})")
                    
            except Exception as e:
                print(f"ERROR: {e}")
                
            time.sleep(0.5) # Jeda sedikit

        current_date += timedelta(days=1)

    print("\n--- SEMUA DATA HARUSNYA SUDAH LENGKAP ---")

if __name__ == "__main__":
    download_final_fix()