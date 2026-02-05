import xarray as xr
import rioxarray
import geopandas as gpd
import os
import glob
import pandas as pd
import warnings

# Abaikan warning
warnings.filterwarnings("ignore")

# --- 1. KONFIGURASI FOLDER ---
# Folder INPUT: Tempat file .nc berada (Hasil download/clip sebelumnya)
# Pastikan path ini benar!
input_nc_dir = r"D:\magang\coral\output\02_output_clip_lombok" 

# Folder OUTPUT: Tempat menyimpan file .xyz
output_dir = r"D:\magang\coral\output\03_site_xyz_masked"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# --- 2. KONFIGURASI SHAPEFILE (MASK) ---
# Dictionary: { "KODE_WILAYAH": "PATH_KE_SHAPEFILE" }
regions = {
    "gm": r"D:\magang\CORAL\bahan\Basemap buat Lia\Basemap buat Lia\basemap\gili_matra_buffer_5km.shp",
    "gn": r"D:\magang\CORAL\bahan\Basemap buat Lia\Basemap buat Lia\basemap\gita_nada_buffer_5km.shp",
    "np": r"D:\magang\CORAL\bahan\Basemap buat Lia\Basemap buat Lia\basemap\nusa_penida_buffer_5km.shp"
}

def masking_process():
    print("--- PROSES MASKING SHAPEFILE KE XYZ ---")
    print(f"Input Folder: {input_nc_dir}")
    print(f"Output Folder: {output_dir}\n")

    # Ambil semua file .nc
    nc_files = glob.glob(os.path.join(input_nc_dir, "*.nc"))
    
    if not nc_files:
        print("[ERROR] Tidak ada file .nc di folder input!")
        return

    # Loop setiap file Raster (.nc)
    for i, nc_path in enumerate(nc_files, 1):
        filename_full = os.path.basename(nc_path)
        filename_no_ext = os.path.splitext(filename_full)[0] # Hilangkan .nc
        
        # Skip file jika itu file sisa/temp
        if filename_full.startswith("Clip_Indo") or filename_full.startswith("Layer_"):
            # Opsional: Bisa diatur mau skip atau tidak. 
            # Jika ingin memproses file hasil clip sebelumnya, hapus blok if ini.
            pass 

        print(f"[{i}/{len(nc_files)}] Memproses: {filename_full}")

        try:
            # 1. Buka Raster
            ds = xr.open_dataset(nc_path)

            # Deteksi nama koordinat (lat/lon vs latitude/longitude)
            if 'lat' in ds.coords:
                ds = ds.rio.set_spatial_dims("lon", "lat")
                x_name, y_name = 'lon', 'lat'
            elif 'latitude' in ds.coords:
                ds = ds.rio.set_spatial_dims("longitude", "latitude")
                x_name, y_name = 'longitude', 'latitude'
            else:
                print("   -> Skip (Koordinat tidak dikenali)")
                continue

            # Set CRS Raster (NOAA biasanya EPSG:4326)
            ds = ds.rio.write_crs("EPSG:4326", inplace=True)

            # Loop setiap Wilayah (Shapefile)
            for code, shp_path in regions.items():
                try:
                    # 2. Buka Shapefile
                    gdf = gpd.read_file(shp_path)
                    
                    # Pastikan CRS Shapefile sama dengan Raster
                    if gdf.crs != ds.rio.crs:
                        gdf = gdf.to_crs(ds.rio.crs)

                    # 3. Lakukan Clipping (Masking)
                    # drop=True akan memotong kotak sesuai batas shp
                    clipped = ds.rio.clip(gdf.geometry, gdf.crs, drop=True)
                    
                    # 4. Konversi ke Format XYZ (ASCII)
                    # Convert ke Pandas DataFrame
                    df = clipped.to_dataframe().reset_index()
                    
                    # Kita cari nama variabel datanya (misal: analysed_sst, degree_heating_week)
                    # Biasanya kolom selain lat, lon, time, spatial_ref
                    data_vars = [col for col in df.columns if col not in [x_name, y_name, 'time', 'spatial_ref', 'crs']]
                    
                    if not data_vars:
                        print(f"   -> Warning: Tidak ada variabel data di {code}")
                        continue
                        
                    target_var = data_vars[0] # Ambil variabel pertama
                    
                    # Filter: Ambil hanya kolom X, Y, Z
                    xyz_df = df[[x_name, y_name, target_var]]
                    
                    # Hapus nilai NaN (Area di luar polygon shp yang kena masking)
                    xyz_df = xyz_df.dropna()

                    if xyz_df.empty:
                        print(f"   -> {code}: Kosong (Shapefile tidak overlap dengan data?)")
                        continue

                    # 5. Simpan ke .XYZ
                    # Nama file: np_coraltemp_v3.1_yyyymmdd.xyz
                    output_filename = f"{code}_{filename_no_ext}.xyz"
                    output_path = os.path.join(output_dir, output_filename)
                    
                    # Simpan sebagai CSV tapi separator spasi, tanpa header, tanpa index
                    xyz_df.to_csv(output_path, sep=' ', header=False, index=False)
                    
                    print(f"   -> OK: {output_filename}")

                except Exception as e_shp:
                    print(f"   -> Error pada wilayah {code}: {e_shp}")

            ds.close()

        except Exception as e_file:
            print(f"   -> Gagal membuka file: {e_file}")

    print("\n--- SELESAI SEMUA ---")

if __name__ == "__main__":
    masking_process()