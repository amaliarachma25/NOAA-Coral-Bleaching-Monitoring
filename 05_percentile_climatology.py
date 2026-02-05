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

# Abaikan warning
warnings.filterwarnings("ignore")

# ==============================================================================
# 1. KONFIGURASI PATH & FILE
# ==============================================================================

# --- KONFIGURASI CLIMATOLOGY (INPUT) ---
base_dir_clim = r"D:\magang\CORAL\output\01_climatology"
input_nc = os.path.join(base_dir_clim, "00_Raw_Climatology", "ct5km_climatology_v3.1.nc")
shp_base_dir = os.path.join(base_dir_clim, "SHP_SITE")

# Mapping Kode Wilayah ke File Shapefile
regions_shp = {
    "GM": os.path.join(shp_base_dir, "gili_matra_buffer_5km.shp"),
    "GN": os.path.join(shp_base_dir, "gita_nada_buffer_5km.shp"),
    "NP": os.path.join(shp_base_dir, "nusa_penida_buffer_5km.shp")
}

# --- KONFIGURASI DATA HARIAN (INPUT) ---
INPUT_FOLDER_XYZ = "03_Masking_Site"  # Folder berisi file .xyz harian

# --- KONFIGURASI OUTPUT ---
OUTPUT_FOLDER = "NOAA_Final_Reports_Integrated"

# Variabel Bulan dalam NetCDF
month_vars = [
    'sst_clim_january', 'sst_clim_february', 'sst_clim_march', 
    'sst_clim_april', 'sst_clim_may', 'sst_clim_june',
    'sst_clim_july', 'sst_clim_august', 'sst_clim_september', 
    'sst_clim_october', 'sst_clim_november', 'sst_clim_december'
]

# Koordinat Slicing (Lombok Area)
lat_min, lat_max = -9.2, -8.2
lon_min, lon_max = 115.3, 116.3

# ==============================================================================
# 2. MODUL CLIMATOLOGY (XARRAY & GEOPANDAS)
# ==============================================================================
def calculate_climatology_data():
    """
    Menghitung MMM dan Monthly Mean dari file NetCDF menggunakan Shapefile.
    Mengembalikan dictionary berisi data klimatologi per site.
    """
    print("\n" + "="*50)
    print("   MEMULAI PERHITUNGAN CLIMATOLOGY (NetCDF)")
    print("="*50)

    if not os.path.exists(input_nc):
        print(f"❌ File NC tidak ditemukan: {input_nc}")
        return {}

    # Buka Dataset
    try:
        ds = xr.open_dataset(input_nc)
        
        # Normalisasi nama dimensi
        if 'lat' in ds.coords: ds = ds.rename({'lat': 'latitude', 'lon': 'longitude'})
        ds = ds.sortby(['latitude', 'longitude'])

        # Slicing Area Lombok (Hemat RAM)
        print("✂️  Memotong area Lombok...")
        if ds.latitude[0] > ds.latitude[-1]:
            ds_lombok = ds.sel(latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max))
        else:
            ds_lombok = ds.sel(latitude=slice(lat_min, lat_max), longitude=slice(lon_min, lon_max))
        
        ds_lombok = ds_lombok.rio.set_spatial_dims("longitude", "latitude").rio.write_crs("EPSG:4326")
        
        clim_results = {}

        for code, shp_path in regions_shp.items():
            if not os.path.exists(shp_path):
                print(f"⚠️  Shapefile tidak ditemukan untuk {code}: {shp_path}")
                continue

            print(f"🔄 Processing Climatology: {code}")
            gdf = gpd.read_file(shp_path)
            
            # Samakan CRS
            if gdf.crs != ds_lombok.rio.crs:
                gdf = gdf.to_crs(ds_lombok.rio.crs)

            try:
                # Clip NetCDF dengan Shapefile
                clipped = ds_lombok.rio.clip(gdf.geometry, gdf.crs, drop=True)
                
                site_means = []
                # Loop 12 Bulan
                for var_name in month_vars:
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
                    print(f"   ❌ {code}: Data bulan tidak lengkap.")

            except Exception as e:
                print(f"   ❌ Error processing {code}: {e}")

        ds.close()
        return clim_results

    except Exception as e:
        print(f"CRITICAL ERROR CLIMATOLOGY: {e}")
        return {}

# ==============================================================================
# 3. MODUL HARIAN (PANDAS & NUMPY) -- FIXED BAA LOGIC --
# ==============================================================================
class RegionAnalyzer:
    def __init__(self, name, code, climatology_data):
        self.name = name
        self.code = code
        self.stress_window = deque(maxlen=84) # 12 minggu untuk DHW
        self.baa_window = deque(maxlen=7)     # [BARU] 7 hari untuk BAA Composite
        self.center_lat = 0.0
        self.center_lon = 0.0
        self.coord_set = False
        
        # Data Climatology dari Tahap 1
        self.mmm = climatology_data.get('mmm', 0.0)
        self.monthly_means = climatology_data.get('monthly_means', [0.0]*12)

    def process_day(self, date_obj, file_hs, file_sst=None, file_ssta=None):
        try:
            # 1. Baca HS
            df_hs = pd.read_csv(file_hs, sep='\s+', header=None, names=['lon', 'lat', 'val'])
            df_hs = df_hs.dropna()
            if df_hs.empty: return None

            # Ambil koordinat pusat (Rata-rata seluruh piksel dalam mask)
            if not self.coord_set:
                self.center_lon = df_hs['lon'].mean()
                self.center_lat = df_hs['lat'].mean()
                self.coord_set = True

            # 2. Hitung 90th Percentile HS
            hs_values = df_hs['val'].values
            hs_90 = np.percentile(hs_values, 90)
            
            # Cari indeks piksel untuk mengambil SST di lokasi yang sama
            idx_p90 = (np.abs(hs_values - hs_90)).argmin()
            
            # 3. Ambil SST & SSTA (Jika ada)
            sst_val = -999.0
            sst_min = -999.0
            sst_max = -999.0
            ssta_val = -999.0
            
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

            # 4. Hitung DHW (Akumulasi)
            daily_stress = 0.0
            if hs_90 >= 1.0:
                daily_stress = hs_90 / 7.0
            
            self.stress_window.append(daily_stress)
            current_dhw = sum(self.stress_window)

            # 5. Hitung BAA (Logika Baru: Instantaneous -> 7-Day Max)
            # Tentukan level alert instan hari ini
            # 0: No Stress, 1: Watch, 2: Warning, 3: Alert Lvl 1, 4: Alert Lvl 2
            daily_alert_level = 0
            
            if hs_90 <= 0.0:
                daily_alert_level = 0 # No Stress
            elif 0.0 < hs_90 < 1.0:
                daily_alert_level = 1 # Watch
            else: # hs_90 >= 1.0
                if current_dhw < 4.0:
                    daily_alert_level = 2 # Warning (Possible Bleaching)
                elif 4.0 <= current_dhw < 8.0:
                    daily_alert_level = 3 # Alert Level 1
                elif current_dhw >= 8.0:
                    daily_alert_level = 4 # Alert Level 2

            # Masukkan ke window 7 hari
            self.baa_window.append(daily_alert_level)
            
            # Ambil nilai maksimum dalam 7 hari terakhir (Composite)
            final_baa = max(self.baa_window) if self.baa_window else 0

            return {
                "date": date_obj,
                "sst_min": sst_min,
                "sst_max": sst_max,
                "sst_90": sst_val,
                "ssta_90": ssta_val,
                "hs_90": max(0, hs_90),
                "dhw": current_dhw,
                "baa": final_baa # Menggunakan hasil composite
            }
        except Exception as e:
            print(f"Error reading daily file {file_hs}: {e}")
            return None

# ==============================================================================
# 4. MAIN EXECUTION
# ==============================================================================
def main():
    if not os.path.exists(OUTPUT_FOLDER): os.makedirs(OUTPUT_FOLDER)

    # --- TAHAP 1: HITUNG CLIMATOLOGY ---
    clim_data_store = calculate_climatology_data()
    
    if not clim_data_store:
        print("⚠️  Peringatan: Data Climatology Kosong/Gagal. Melanjutkan dengan nilai default 0.")

    # --- TAHAP 2: INVENTARISASI FILE HARIAN ---
    print("\n" + "="*50)
    print(f"   MEMULAI ANALISIS HARIAN (Folder: {INPUT_FOLDER_XYZ})")
    print("="*50)
    
    files_map = {} 
    
    # Scanning folder
    for f in os.listdir(INPUT_FOLDER_XYZ):
        if not f.endswith(".xyz"): continue
        
        name_lower = f.lower()
        if "np_" in name_lower: region = "NP"
        elif "gm_" in name_lower: region = "GM"
        elif "gn_" in name_lower: region = "GN"
        else: continue
            
        date_match = re.search(r"(\d{8})", f)
        if not date_match: continue
        date_str = date_match.group(1)
        
        ftype = "UNKNOWN"
        if "hs" in name_lower and "hotspot" not in name_lower: ftype = "HS"
        elif "hotspot" in name_lower: ftype = "HS"
        elif "ssta" in name_lower: ftype = "SSTA"
        elif "sst" in name_lower: ftype = "SST"
        
        if region not in files_map: files_map[region] = {}
        if date_str not in files_map[region]: files_map[region][date_str] = {}
        
        files_map[region][date_str][ftype] = os.path.join(INPUT_FOLDER_XYZ, f)

    # --- TAHAP 3: PROSES DAN TULIS LAPORAN ---
    full_names = {"NP": "Nusa Penida", "GM": "Gili Matra", "GN": "Gita Nada"}
    
    for code, dates_dict in files_map.items():
        region_fullname = full_names.get(code, code)
        print(f"\n📈 Memproses Wilayah: {region_fullname} ({code})")
        
        # Ambil data climatology spesifik untuk region ini
        # Jika tidak ada hasil hitungan, gunakan default
        site_clim = clim_data_store.get(code, {'mmm': 0.0, 'monthly_means': [0.0]*12})
        
        analyzer = RegionAnalyzer(region_fullname, code, site_clim)
        sorted_dates = sorted(dates_dict.keys())
        results_buffer = []

        # Loop Harian
        for d_str in sorted_dates:
            files = dates_dict[d_str]
            if "HS" not in files: continue
            
            dt_obj = datetime.datetime.strptime(d_str, "%Y%m%d")
            data = analyzer.process_day(
                dt_obj, 
                files["HS"], 
                files.get("SST"), 
                files.get("SSTA")
            )
            if data: results_buffer.append(data)

        if not results_buffer: continue

        # Menulis File Output
        output_filename = os.path.join(OUTPUT_FOLDER, f"{region_fullname.replace(' ','_')}_NOAA_Combined.txt")
        
        with open(output_filename, "w") as f:
            # HEADER
            f.write("Name:\n")
            f.write(f"{region_fullname}\n\n")
            f.write("Polygon Middle Longitude:\n")
            f.write(f"{analyzer.center_lon:.4f} \n\n")
            f.write("Polygon Middle Latitude:\n")
            f.write(f"{analyzer.center_lat:.4f} \n\n")
            
            # --- DATA CLIMATOLOGY DARI HASIL HITUNGAN TAHAP 1 ---
            f.write("Averaged Maximum Monthly Mean:\n")
            f.write(f"{analyzer.mmm:.4f}\n\n")
            
            f.write("Averaged Monthly Mean (Jan-Dec):\n")
            # Format list menjadi string dengan spasi
            means_str = " ".join([f"{val:.4f}" for val in analyzer.monthly_means])
            f.write(f"{means_str}\n\n")
            
            # --- TANGGAL VALID ---
            first_dt = results_buffer[0]['date']
            dhw_valid_dt = first_dt + datetime.timedelta(weeks=12) # 12 minggu
            baa_valid_dt = dhw_valid_dt + datetime.timedelta(days=7) # +7 hari
            
            f.write("First Valid DHW Date:\n")
            f.write(f"{dhw_valid_dt.strftime('%Y %m %d')}\n\n")
            f.write("First Valid BAA Date:\n")
            f.write(f"{baa_valid_dt.strftime('%Y %m %d')}\n\n")
            
            # TABEL DATA
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
                
        print(f"✅ Laporan tersimpan: {output_filename}")

    print("\n" + "="*50)
    print("PROSES SELESAI SEMUA")
    print("="*50)

if __name__ == "__main__":
    main()