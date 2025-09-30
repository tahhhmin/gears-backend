# tempo_reader.py
import os
import sys
import json
import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from shapely.geometry import Polygon, mapping
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPolygon

if len(sys.argv) < 2:
    print("Usage: python tempo_reader.py <FILE> [LAT LON RADIUS]")
    sys.exit(1)

filepath = sys.argv[1]
lat0 = float(sys.argv[2]) if len(sys.argv) > 2 else None
lon0 = float(sys.argv[3]) if len(sys.argv) > 3 else None
radius = float(sys.argv[4]) if len(sys.argv) > 4 else 0.5

print(f"📂 Opening {filepath}")
ds = Dataset(filepath)

# Extract needed groups
lats = ds["/geolocation/latitude"][:]
lons = ds["/geolocation/longitude"][:]
lat_bounds = ds["/geolocation/latitude_bounds"][:]   # (131,2048,4)
lon_bounds = ds["/geolocation/longitude_bounds"][:]
no2 = ds["/product/vertical_column_troposphere"][:]

print(f"📊 Shapes: {lats.shape}, {lons.shape}, {no2.shape}")

# Select pixels near target (if lat/lon provided)
mask = np.ones_like(no2, dtype=bool)
if lat0 is not None and lon0 is not None:
    dists = np.sqrt((lats - lat0) ** 2 + (lons - lon0) ** 2)
    mask = dists <= radius
    if not mask.any():
        print("❌ No pixels found within radius")
        sys.exit(0)
    print(f"✅ Found {mask.sum()} pixels near ({lat0}, {lon0})")

# ---------------------------
# 1. Matplotlib Visualization
# ---------------------------
patches = []
colors = []

for i in range(lats.shape[0]):
    for j in range(lats.shape[1]):
        if not mask[i, j]:
            continue
        corners = list(zip(lon_bounds[i, j, :], lat_bounds[i, j, :]))
        poly = MplPolygon(corners, closed=True)
        patches.append(poly)
        colors.append(no2[i, j])

fig, ax = plt.subplots(figsize=(8, 6))
pc = PatchCollection(patches, cmap="viridis", edgecolor="k", linewidths=0.2)
pc.set_array(np.array(colors))
ax.add_collection(pc)
ax.autoscale_view()
plt.colorbar(pc, ax=ax, label="Tropospheric NO₂ (mol/m²)")
plt.title("TEMPO NO₂ Footprints")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.savefig("no2_footprints.png", dpi=200)
print("🖼 Saved map as no2_footprints.png")

# ---------------------------
# 2. GeoJSON Export for Mapbox
# ---------------------------
features = []
for i in range(lats.shape[0]):
    for j in range(lats.shape[1]):
        if not mask[i, j]:
            continue
        corners = list(zip(lon_bounds[i, j, :], lat_bounds[i, j, :]))
        poly = Polygon(corners)
        features.append({
            "type": "Feature",
            "geometry": mapping(poly),
            "properties": {"NO2": float(no2[i, j])}
        })

geojson = {"type": "FeatureCollection", "features": features}
with open("no2_footprints.geojson", "w") as f:
    json.dump(geojson, f)

print("🌍 Saved GeoJSON as no2_footprints.geojson")
