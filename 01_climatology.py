import os
import requests
import glob
import warnings
import xarray as xr
import rioxarray
import geopandas as gpd
import pandas as pd

# Abaikan warning (termasuk warning GIL)
warnings.filterwarnings("ignore")

# ==========================================
# 1. KONFIGURASI PATH (SESUAIKAN DI SINI)
# ==========================================

# Folder Utama Proyek
base_dir = r"D:\magang\CORAL\output\01_climatology" 

# Folder Output
input_global_dir = os.path.join(base_dir, "00_Raw_Climatology") 
output_xyz_dir = os.path.join(base_dir, "03_Masking_Site_Clim") 
shp_base_dir = os.path.join(base_dir, "SHP_SITE")               

# Buat folder jika belum ada
os.makedirs(input_global_dir, exist_ok=True)
os.makedirs(output_xyz_dir, exist_ok=True)

# --- PERBAIKAN LINK DI SINI ---
# Link yang benar tidak menggunakan "_op" untuk climatology
BASE_URL_NOAA = "https://www.star.nesdis.noaa.gov/pub/sod/mecb/crw/data/5km/v3.1_op/climatology/nc/"

# Daftar File
target_files = [
    "ct5km_climatology_v3.1.nc"
]

# Koordinat & Shapefile
lat_min, lat_max = -9, -8
lon_min, lon_max = 115.2, 116.2

regions = {
    "gm": os.path.join(shp_base_dir, "gili_matra_buffer_5km.shp"),
    "gn": os.path.join(shp_base_dir, "gita_nada_buffer_5km.shp"),
    "np": os.path.join(shp_base_dir, "nusa_penida_buffer_5km.shp")
}

# ==========================================
# 2. FUNGSI DOWNLOADER
# ==========================================
def download_noaa_data():
    print("--- CEK KETERSEDIAAN DATA CLIMATOLOGY ---")
    
    for filename in target_files:
        file_path = os.path.join(input_global_dir, filename)
        
        # Cek apakah file sudah ada
        if os.path.exists(file_path):
            print(f"✅ {filename} sudah ada. Skip download.")
        else:
            url = BASE_URL_NOAA + filename
            print(f"⬇️ Sedang mendownload: {filename} ...")
            print(f"   Sumber: {url}")
            
            try:
                # Verifikasi SSL kadang bermasalah di jaringan kantor/kampus,
                # set verify=False jika masih error SSL (tapi coba True dulu)
                response = requests.get(url, stream=True)
                response.raise_for_status() 
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                print("   -> Download Selesai!")
            except Exception as e:
                print(f"   ❌ GAGAL Download: {e}")
                print("   👉 SOLUSI: Download manual dari link di atas dan taruh di folder '00_Raw_Climatology'")

# ==========================================
# 3. FUNGSI PROCESSING
# ==========================================
def process_climatology():
    print("\n--- MULAI PEMROSESAN DATA ---")
    
    raw_files = glob.glob(os.path.join(input_global_dir, "*.nc"))
    
    if not raw_files:
        print("ERROR: Tidak ada file .nc yang ditemukan. Cek koneksi internet/download.")
        return

    for i, file_path in enumerate(raw_files, 1):
        filename = os.path.basename(file_path)
        filename_no_ext = os.path.splitext(filename)[0]
        
        print(f"\n[{i}/{len(raw_files)}] Memproses: {filename}")

        try:
            ds = xr.open_dataset(file_path)

            if 'lat' in ds.coords:
                ds = ds.rename({'lat': 'latitude', 'lon': 'longitude'})
            
            ds = ds.sortby(['latitude', 'longitude'])

            # Handle slicing
            slice_lat = slice(lat_min, lat_max) if ds.latitude[0] < ds.latitude[-1] else slice(lat_max, lat_min)
            ds_lombok = ds.sel(latitude=slice_lat, longitude=slice(lon_min, lon_max))

            if ds_lombok.dims['latitude'] == 0 or ds_lombok.dims['longitude'] == 0:
                print(" -> GAGAL: Area Lombok kosong.")
                continue

            ds_lombok = ds_lombok.rio.set_spatial_dims("longitude", "latitude")
            ds_lombok = ds_lombok.rio.write_crs("EPSG:4326", inplace=True)

            for code, shp_path in regions.items():
                if not os.path.exists(shp_path):
                    print(f"    -> [SKIP] SHP {code} tidak ditemukan.")
                    continue
                
                gdf = gpd.read_file(shp_path)
                if gdf.crs != ds_lombok.rio.crs:
                    gdf = gdf.to_crs(ds_lombok.rio.crs)

                try:
                    clipped = ds_lombok.rio.clip(gdf.geometry, gdf.crs, drop=True)
                    df = clipped.to_dataframe().reset_index()
                    
                    ignore_cols = ['latitude', 'longitude', 'spatial_ref', 'crs', 'band', 'time']
                    data_vars = [c for c in df.columns if c not in ignore_cols and c != 'month']
                    
                    if not data_vars: continue
                    target_var = data_vars[0]

                    if 'month' in df.columns:
                        unique_months = df['month'].unique()
                        for m in unique_months:
                            df_month = df[df['month'] == m]
                            xyz = df_month[['longitude', 'latitude', target_var]].dropna()
                            if xyz.empty: continue
                            
                            out_name = f"{code}_{filename_no_ext}_bulan_{int(m)}.xyz"
                            xyz.to_csv(os.path.join(output_xyz_dir, out_name), 
                                      sep=' ', header=False, index=False)
                        print(f"    -> {code}: OK (12 Bulan)")
                    else:
                        xyz = df[['longitude', 'latitude', target_var]].dropna()
                        if xyz.empty: continue
                        
                        out_name = f"{code}_{filename_no_ext}.xyz"
                        xyz.to_csv(os.path.join(output_xyz_dir, out_name), 
                                  sep=' ', header=False, index=False)
                        print(f"    -> {code}: OK (Statis)")

                except Exception as e_site:
                    print(f"    -> Error {code}: {e_site}")

            ds.close()
            ds_lombok.close()

        except Exception as e_file:
            print(f" -> ERROR File {filename}: {e_file}")

    print("\n=== SELESAI SEMUA ===")

if __name__ == "__main__":
    download_noaa_data()
    process_climatology()