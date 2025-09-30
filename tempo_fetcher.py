import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load .env file
load_dotenv()

USERNAME = os.getenv("EARTHDATA_USERNAME")
PASSWORD = os.getenv("EARTHDATA_PASSWORD")
TOKEN = os.getenv("EARTHDATA_TOKEN")

if not (TOKEN or (USERNAME and PASSWORD)):
    print("❌ Please set EARTHDATA_TOKEN or EARTHDATA_USERNAME + EARTHDATA_PASSWORD in your .env file")
    exit(1)

# CMR Search URL
CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules"

# Dataset map (make sure these are correct short_names for TEMPO)
PRODUCT_MAP = {
    "NO2": "TEMPO_NO2_L2",
    "O3": "TEMPO_O3_L2",
    "HCHO": "TEMPO_HCHO_L2",
}

# ----------------------------------------------------------------
# AUTH HANDLER
# ----------------------------------------------------------------
def get_auth_headers():
    """Return headers/auth for Earthdata login"""
    headers = {"Accept": "application/json"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
        return headers, None
    return headers, (USERNAME, PASSWORD)

# ----------------------------------------------------------------
# GRANULE SEARCH
# ----------------------------------------------------------------
def search_granules(product, date, lat, lon, buffer=0.5):
    dataset = PRODUCT_MAP[product]

    min_lon = lon - buffer
    max_lon = lon + buffer
    min_lat = lat - buffer
    max_lat = lat + buffer
    bbox = f"{min_lon},{min_lat},{max_lon},{max_lat}"

    start = datetime.strptime(date, "%Y-%m-%d")
    end = start + timedelta(days=1)

    params = {
        "short_name": dataset,
        "temporal": f"{start.strftime('%Y-%m-%dT00:00:00Z')},{end.strftime('%Y-%m-%dT00:00:00Z')}",
        "bounding_box": bbox,
        "page_size": 200,
        "page_num": 1,
    }

    headers, auth = get_auth_headers()

    print(f"🔎 Searching {product} for {date} in {bbox}")
    r = requests.get(CMR_URL, params=params, headers=headers, auth=auth)

    if r.status_code != 200:
        print("❌ CMR query failed:", r.status_code)
        print("Response snippet:", r.text[:200])
        return []

    try:
        data = r.json()
        entries = data.get("feed", {}).get("entry", [])
        return entries
    except Exception as e:
        print("❌ Failed to parse JSON:", e)
        print("Raw response:", r.text[:500])
        return []

# ----------------------------------------------------------------
# GRANULE DOWNLOAD
# ----------------------------------------------------------------
def download_granule(entry, product, date):
    """Download the first available .nc science file from CMR entry"""
    links = entry.get("links", [])
    # Prefer actual .nc files, skip .met or others
    data_links = [
        l["href"] for l in links
        if "data#" in l.get("rel", "") and l["href"].endswith(".nc")
    ]

    if not data_links:
        print("❌ No .nc data links found in granule entry")
        return None

    url = data_links[0]
    headers, auth = get_auth_headers()

    print(f"⬇️  Downloading {url}")
    r = requests.get(url, headers=headers, auth=auth, stream=True)

    if r.status_code == 200:
        os.makedirs("downloads", exist_ok=True)
        filename = url.split("/")[-1]
        filepath = os.path.join("downloads", filename)
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
        print(f"✅ Saved {filepath}")
        return filepath
    else:
        print(f"❌ Download failed {r.status_code}")
        print("Response snippet:", r.text[:200])
        return None

# ----------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 5:
        print("Usage: python tempo_fetcher.py <PRODUCT> <LAT> <LON> <DATE: YYYY-MM-DD> [BUFFER]")
        exit(1)

    product = sys.argv[1].upper()
    lat = float(sys.argv[2])
    lon = float(sys.argv[3])
    date = sys.argv[4]
    buffer = float(sys.argv[5]) if len(sys.argv) > 5 else 0.5

    if product not in PRODUCT_MAP:
        print(f"❌ Unsupported product: {product}")
        exit(1)

    granules = search_granules(product, date, lat, lon, buffer)

    if not granules:
        print(f"No granules found for {product} on {date}")
        exit(0)

    print(f"✅ Found {len(granules)} granules")
    print("First granule title:", granules[0]["title"])

    # Download first granule
    download_granule(granules[0], product, date)
