import os
import xarray as xr
import rioxarray
import geopandas as gpd
import pandas as pd
import numpy as np
import warnings

# Abaikan warning
warnings.filterwarnings("ignore")

# ==========================================
# 1. KONFIGURASI PATH
# ==========================================
base_dir = r"D:\magang\CORAL\output\01_climatology"

# File input (Gunakan file yang kamu sebutkan tadi)
input_nc = os.path.join(base_dir, "00_Raw_Climatology", "ct5km_climatology_v3.1.nc")
shp_base_dir = os.path.join(base_dir, "SHP_SITE")
output_report = os.path.join(base_dir, "mmm_mean_site_FINAL.txt")

# Koordinat Kotak Lombok (Slicing agar RAM Aman)
lat_min, lat_max = -9.2, -8.2
lon_min, lon_max = 115.3, 116.3

regions = {
    "gm": os.path.join(shp_base_dir, "gili_matra_buffer_5km.shp"),
    "gn": os.path.join(shp_base_dir, "gita_nada_buffer_5km.shp"),
    "np": os.path.join(shp_base_dir, "nusa_penida_buffer_5km.shp")
}

# DAFTAR NAMA VARIABEL BULAN (Sesuai isi file kamu)
month_vars = [
    'sst_clim_january', 'sst_clim_february', 'sst_clim_march', 
    'sst_clim_april', 'sst_clim_may', 'sst_clim_june',
    'sst_clim_july', 'sst_clim_august', 'sst_clim_september', 
    'sst_clim_october', 'sst_clim_november', 'sst_clim_december'
]

def calculate_site_climatology():
    if not os.path.exists(input_nc):
        print(f"❌ File tidak ditemukan: {input_nc}")
        return

    print(f"📂 Membuka dataset: {os.path.basename(input_nc)}")
    ds = xr.open_dataset(input_nc)

    # --- TAHAP 1: NORMALISASI & SLICING ---
    if 'lat' in ds.coords:
        ds = ds.rename({'lat': 'latitude', 'lon': 'longitude'})
    ds = ds.sortby(['latitude', 'longitude'])

    print("✂️  Memotong area Lombok (Saving RAM)...")
    # Slicing Spasial
    if ds.latitude[0] > ds.latitude[-1]:
        ds_lombok = ds.sel(latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max))
    else:
        ds_lombok = ds.sel(latitude=slice(lat_min, lat_max), longitude=slice(lon_min, lon_max))

    ds_lombok = ds_lombok.rio.set_spatial_dims("longitude", "latitude").rio.write_crs("EPSG:4326")

    results = []
    print("\n--- MULAI PERHITUNGAN SITE ---")

    for code, shp_path in regions.items():
        if not os.path.exists(shp_path):
            continue

        print(f"🔄 Processing Site: {code.upper()}")
        gdf = gpd.read_file(shp_path)
        if gdf.crs != ds_lombok.rio.crs:
            gdf = gdf.to_crs(ds_lombok.rio.crs)
            
        try:
            # 1. Clip sesuai Shapefile (Masking)
            clipped = ds_lombok.rio.clip(gdf.geometry, gdf.crs, drop=True)
            
            site_means = []

            # 2. LOOP MANUAL KE-12 BULAN
            for var_name in month_vars:
                # Cek apakah variabel bulan tersebut ada di file
                if var_name in clipped.data_vars:
                    # Hitung rata-rata spasial untuk variabel bulan tersebut
                    # Hasilnya adalah 1 angka (scalar)
                    val = clipped[var_name].mean(dim=['latitude', 'longitude'], skipna=True).item()
                    site_means.append(val)
                else:
                    print(f"   ⚠️ Warning: Variabel {var_name} tidak ditemukan!")
                    site_means.append(np.nan)

            # 3. Hitung MMM (Max dari 12 nilai rata-rata bulanan)
            # Pastikan tidak ada NaN agar perhitungan akurat
            clean_means = [x for x in site_means if not np.isnan(x)]
            
            if len(clean_means) == 12:
                mmm_value = max(clean_means) # Cari nilai tertinggi
                
                results.append({
                    'name': code.upper(),
                    'mmm': mmm_value,
                    'means': clean_means
                })
                print(f"   ✅ OK. MMM: {mmm_value:.4f}")
            else:
                print("   ❌ Gagal: Data bulan tidak lengkap (kurang dari 12).")

        except Exception as e:
            print(f"   ❌ Error pada {code}: {e}")

    # --- TAHAP 3: SIMPAN OUTPUT FORMAT KHUSUS ---
    with open(output_report, 'w') as f:
        for res in results:
            f.write(f"SITE: {res['name']}\n")
            f.write("Averaged Maximum Monthly Mean:\n")
            f.write(f"{res['mmm']:.4f}\n\n")
            
            f.write("Averaged Monthly Mean (Jan-Dec):\n")
            # Menggabungkan 12 angka menjadi string satu baris
            means_str = " ".join([f"{val:.4f}" for val in res['means']])
            f.write(f"{means_str}\n")
            
            f.write("\n" + "="*40 + "\n\n")

    ds.close()
    print(f"\n🎉 SELESAI! Laporan tersimpan di: {output_report}")

if __name__ == "__main__":
    calculate_site_climatology()