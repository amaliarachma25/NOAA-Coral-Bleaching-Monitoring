import importlib

# Daftar library yang mau dicek
libraries = [
    'pandas', 
    'numpy', 
    'xarray', 
    'rioxarray', 
    'geopandas', 
    'shapely', 
    'netCDF4'
]

print("=== HASIL PENGECEKAN LIBRARY ===")

all_good = True
for lib in libraries:
    try:
        importlib.import_module(lib)
        print(f"✅ [OK] {lib} sudah terinstall.")
    except ImportError:
        print(f"❌ [X]  {lib} BELUM terinstall!")
        all_good = False

print("================================")

if all_good:
    print("Sip! Semua library siap digunakan.")
else:
    print("Masih ada yang kurang. Silakan install yang tandanya [X].")