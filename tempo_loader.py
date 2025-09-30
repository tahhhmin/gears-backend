import os
import psycopg2
import numpy as np
from netCDF4 import Dataset

# Database connection (set via env variables)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "tempo")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")


def load_tempo_to_postgis(nc_path, date_str):
    print(f"📂 Opening {nc_path}")
    ds = Dataset(nc_path, "r")

    lats = ds["/geolocation/latitude"][:]
    lons = ds["/geolocation/longitude"][:]
    no2_tropo = ds["/product/vertical_column_troposphere"][:]
    no2_strato = ds["/product/vertical_column_stratosphere"][:]
    no2_total = ds["/support_data/vertical_column_total"][:]
    qf = ds["/product/main_data_quality_flag"][:]

    rows, cols = lats.shape
    print(f"📊 Shape: {rows} x {cols}")

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()

    count = 0
    for i in range(rows):
        for j in range(cols):
            lat = float(lats[i, j])
            lon = float(lons[i, j])

            if np.isnan(lat) or np.isnan(lon):
                continue

            tropo = float(no2_tropo[i, j])
            strato = float(no2_strato[i, j])
            total = float(no2_total[i, j])
            flag = int(qf[i, j])

            cur.execute("""
                INSERT INTO tempo_no2 (timestamp, latitude, longitude, 
                                       no2_tropo, no2_strato, no2_total, quality_flag, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_Point(%s, %s), 4326));
            """, (date_str, lat, lon, tropo, strato, total, flag, lon, lat))

            count += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Inserted {count} rows into PostGIS")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python tempo_loader.py <NETCDF_FILE> <DATE: YYYY-MM-DD>")
        sys.exit(1)

    load_tempo_to_postgis(sys.argv[1], sys.argv[2])
