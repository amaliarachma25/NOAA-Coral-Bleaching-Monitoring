
# 🌊 Automated Pipeline: NOAA Coral Reef Watch Data Processing

**Project:** Coral Health Monitoring (SST, DHW, SSTA)

This repository contains an automated Python workflow designed to download satellite data from **NOAA Coral Reef Watch**, spatially clip it to a specific region (Lombok Strait), and extract site-specific data based on conservation area boundaries using Shapefiles.

## 📋 Workflow Overview

1. **Setup:** Automatic initialization of project directories in Google Drive.
2. **Download:** Fetch daily global datasets directly from NOAA Servers.
3. **Regional Clip:** Crop global data into a smaller, manageable region (Lombok).
4. **Site Masking:** Clip data to specific conservation areas (Gili Matra, Gita Nada, Nusa Penida) and convert to `.xyz` format.


## ⚠️ MANDATORY PRE-REQUISITES

**Before running the scripts, you must upload the required Shapefiles.**

Ensure the following files (`.shp`, `.shx`, `.dbf`, etc.) are uploaded to this specific path in your Google Drive:
**Location:** `/MAGANG/CORAL/SHP_SITE/`

**Required Files:**

* `gili_matra_buffer_5km.shp`
* `gita_nada_buffer_5km.shp`
* `nusa_penida_buffer_5km.shp`

---

## 📂 Project Folder Structure

Please ensure your Google Drive structure (`My Drive/MAGANG/CORAL/...`) matches the map below to prevent path errors.

```text
My Drive/
└── MAGANG/
    └── CORAL/  (Project Root)
        │
        ├── [PIPELINE 1: DAILY DATA PROCESSING]
        │   ├── 01_Global/                     # [Input]   Raw Daily NOAA downloads (.nc)
        │   ├── 02_Clip_Lombok/                # [Process] Daily files clipped to Lombok area
        │   ├── SHP_SITE/                      # [Manual]  UPLOAD SHAPEFILES HERE (.shp)
        │   └── 03_masking_site/               # [Output]  Extracted coordinates (.xyz) per site
        │
        ├── [PIPELINE 2: CLIMATOLOGY PROCESSING]
        │   └── 01_climatology/
        │       ├── SHP_SITE/                  # [Manual]  Copy of Shapefiles for climatology
        │       ├── 00_raw_climatology/        # [Input]   Raw Climatology downloads (.nc)
        │       ├── 03_masking_site_clim/      # [Output]  Extracted Climatology per site
        │       └── average_climatology.txt    # [Result]  MMM & Monthly Mean Statistics
        │
        └── [PIPELINE 3: FINAL ANALYSIS]
            └── NOAA_Final_Reports/            # [Final]   Integrated Reports (BAA, DHW, Percentile)
                                               # (Merges data from 03_masking_site & average_climatology.txt)

```
