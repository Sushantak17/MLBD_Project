#!/usr/bin/env python3
"""
UrbanStream – Dataset Downloader
Run this once before starting the producers.
Downloads all 3 required CSVs into the data/ folder automatically.

Usage:
  python3 download_data.py
"""

import csv
import io
import json
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# ── terminal colours ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}✓{RESET}  {msg}")
def warn(msg): print(f"  {YELLOW}⚠{RESET}  {msg}")
def err(msg):  print(f"  {RED}✗{RESET}  {msg}")
def info(msg): print(f"  {CYAN}→{RESET}  {msg}")


# ══════════════════════════════════════════════════════════════════════════════
# 1.  NYC Traffic Speed  (NYC Open Data Socrata API)
# ══════════════════════════════════════════════════════════════════════════════

def download_traffic():
    out = DATA_DIR / "nyc_traffic.csv"
    if out.exists() and out.stat().st_size > 10_000:
        ok(f"nyc_traffic.csv already exists ({out.stat().st_size // 1024} KB) – skipping.")
        return True

    print(f"\n{BOLD}[1/3] NYC Traffic Speed Data{RESET}")
    info("Source: NYC Open Data – DOT Traffic Speeds NBE (dataset i4gi-tjb9)")

    # Socrata SODA API – returns up to 50 000 rows as CSV, no login needed.
    # We pull the most recent 50 000 rows ordered by data_as_of DESC.
    url = (
        "https://data.cityofnewyork.us/resource/i4gi-tjb9.csv"
        "?$limit=50000"
        "&$order=data_as_of+DESC"
        "&$select=speed,link_id,data_as_of,borough,link_points,travel_time,status"
    )
    info(f"Fetching up to 50 000 rows via Socrata API …")

    try:
        req = urllib.request.Request(url, headers={"Accept": "text/csv"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()

        # Rename columns to match what the producer expects
        lines   = raw.decode("utf-8", errors="replace").splitlines()
        reader  = csv.DictReader(io.StringIO("\n".join(lines)))
        rows    = list(reader)

        # Map snake_case → UPPER as used in producer
        col_map = {
            "speed":       "SPEED",
            "link_id":     "LINK_ID",
            "data_as_of":  "DATA_AS_OF",
            "borough":     "BOROUGH",
            "link_points": "LINK_POINTS",
            "travel_time": "TRAVEL_TIME",
            "status":      "STATUS",
        }

        with open(out, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(col_map.values()), extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                mapped = {col_map.get(k, k): v for k, v in row.items()}
                writer.writerow(mapped)

        ok(f"nyc_traffic.csv saved – {len(rows):,} rows  ({out.stat().st_size // 1024} KB)")
        return True

    except Exception as exc:
        err(f"Download failed: {exc}")
        warn("Manual fallback:")
        print("    1. Open: https://data.cityofnewyork.us/resource/i4gi-tjb9.csv?$limit=50000")
        print("    2. Save the file as:  data/nyc_traffic.csv")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# 2.  OpenAQ Air Quality – NYC stations  (OpenAQ v3 API, no key needed)
# ══════════════════════════════════════════════════════════════════════════════

def download_pollution():
    out = DATA_DIR / "openaq_nyc.csv"
    if out.exists() and out.stat().st_size > 5_000:
        ok(f"openaq_nyc.csv already exists ({out.stat().st_size // 1024} KB) – skipping.")
        return True

    print(f"\n{BOLD}[2/3] OpenAQ Air Quality – NYC{RESET}")
    info("Source: OpenAQ v3 API (api.openaq.org) – pm25 + no2, no API key required")

    # OpenAQ v3 public measurements endpoint
    # We query by bounding box covering all of NYC and filter pm25 + no2.
    all_rows = []

    for parameter in ("pm25", "no2"):
        info(f"Fetching {parameter.upper()} measurements …")
        page     = 1
        per_page = 1000
        fetched  = 0
        max_rows = 5000   # 5 k rows per parameter = 10 k total

        while fetched < max_rows:
            url = (
                "https://api.openaq.org/v3/measurements"
                f"?coordinates=40.7128,-74.0060"
                f"&radius=30000"          # 30 km around NYC centre
                f"&parameters_id={'2' if parameter == 'pm25' else '7'}"  # pm25=2, no2=7
                f"&limit={per_page}"
                f"&page={page}"
                f"&order_by=datetime"
                f"&sort=desc"
            )

            try:
                req = urllib.request.Request(url, headers={
                    "Accept":     "application/json",
                    "User-Agent": "UrbanStream-Downloader/1.0",
                })
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read())
            except urllib.error.HTTPError as e:
                warn(f"  API returned {e.code} for {parameter} page {page} – stopping.")
                break
            except Exception as exc:
                warn(f"  Request failed ({exc}) – stopping.")
                break

            results = data.get("results", [])
            if not results:
                break

            for r in results:
                try:
                    loc   = r.get("location", {})
                    coords = loc.get("coordinates", {}) if isinstance(loc, dict) else {}
                    dt    = r.get("period", {}).get("datetimeFrom", {}).get("utc", "") or \
                            r.get("date", {}).get("utc", "")
                    all_rows.append({
                        "location":  loc.get("name", "") if isinstance(loc, dict) else str(loc),
                        "parameter": parameter,
                        "value":     r.get("value", ""),
                        "unit":      r.get("parameter", {}).get("units", "µg/m³")
                                     if isinstance(r.get("parameter"), dict) else "µg/m³",
                        "date_utc":  dt,
                        "latitude":  coords.get("latitude",  40.7128),
                        "longitude": coords.get("longitude", -74.0060),
                    })
                except Exception:
                    pass

            fetched += len(results)
            page    += 1
            time.sleep(0.3)   # be polite to the API

            if len(results) < per_page:
                break   # last page

        info(f"  Collected {fetched} {parameter.upper()} rows")

    if not all_rows:
        # Fallback: use EPA AirNow CSV (always available, no key)
        info("OpenAQ returned no data – trying EPA AirNow historical CSV fallback …")
        return _download_epa_fallback(out)

    fieldnames = ["location", "parameter", "value", "unit", "date_utc", "latitude", "longitude"]
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    ok(f"openaq_nyc.csv saved – {len(all_rows):,} rows  ({out.stat().st_size // 1024} KB)")
    return True


def _download_epa_fallback(out: Path) -> bool:
    """
    Fallback: synthesise a realistic OpenAQ-shaped CSV from EPA's
    public AQS annual summary for NY county (FIPS 36061 = Manhattan).
    This is a small static file always available from EPA's S3 bucket.
    """
    info("Fetching EPA AQS annual summary for New York County …")
    # EPA pre-built annual summary files – no auth, small (~2-5 MB zipped)
    import zipfile

    url = "https://aqs.epa.gov/aqsweb/airdata/annual_conc_by_monitor_2023.zip"
    zip_path = DATA_DIR / "_epa_annual_2023.zip"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "UrbanStream/1.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            chunk = 65536
            downloaded = 0
            with open(zip_path, "wb") as f:
                while True:
                    block = resp.read(chunk)
                    if not block:
                        break
                    f.write(block)
                    downloaded += len(block)
                    if total:
                        pct = downloaded / total * 100
                        print(f"\r    {pct:.0f}% ({downloaded // 1024} KB)", end="", flush=True)
        print()

        with zipfile.ZipFile(zip_path) as zf:
            csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
            raw = zf.read(csv_name).decode("utf-8", errors="replace")

        zip_path.unlink(missing_ok=True)

        reader  = csv.DictReader(io.StringIO(raw))
        rows    = []
        ny_params = {"PM2.5 - Local Conditions": "pm25", "Nitrogen dioxide (NO2)": "no2"}

        for row in reader:
            if row.get("State Code") != "36":   # New York State
                continue
            param_name = row.get("Parameter Name", "")
            if param_name not in ny_params:
                continue
            rows.append({
                "location":  row.get("Local Site Name") or row.get("Site Num", "NYC-EPA"),
                "parameter": ny_params[param_name],
                "value":     row.get("Arithmetic Mean", ""),
                "unit":      "µg/m³",
                "date_utc":  f"{row.get('Year',2023)}-01-01T00:00:00Z",
                "latitude":  row.get("Latitude",  40.7128),
                "longitude": row.get("Longitude", -74.0060),
            })

        if not rows:
            raise ValueError("No NY rows found in EPA annual summary")

        # Expand annual summaries into hourly-ish rows so the producer has volume
        expanded = []
        for r in rows:
            for h in range(24):
                import random, copy
                nr = copy.copy(r)
                base = float(nr["value"]) if nr["value"] else 10.0
                nr["value"]    = round(max(0, base + random.gauss(0, base * 0.2)), 2)
                nr["date_utc"] = f"2023-06-15T{h:02d}:00:00Z"
                expanded.append(nr)

        fieldnames = ["location", "parameter", "value", "unit", "date_utc", "latitude", "longitude"]
        with open(out, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(expanded)

        ok(f"openaq_nyc.csv saved via EPA fallback – {len(expanded):,} rows ({out.stat().st_size // 1024} KB)")
        return True

    except Exception as exc:
        err(f"EPA fallback also failed: {exc}")
        warn("Manual fallback for openaq_nyc.csv:")
        print("    Option A – OpenAQ website:")
        print("      1. Go to: https://explore.openaq.org")
        print("      2. Search 'New York' → filter PM2.5 + NO2")
        print("      3. Export CSV → save as data/openaq_nyc.csv")
        print("    Option B – Use the synthetic generator (no external data needed):")
        print("      python3 download_data.py --synthetic-pollution")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# 3.  Open-Meteo Weather  (free historical API, no key needed)
# ══════════════════════════════════════════════════════════════════════════════

def download_weather():
    out = DATA_DIR / "weather_nyc.csv"
    if out.exists() and out.stat().st_size > 5_000:
        ok(f"weather_nyc.csv already exists ({out.stat().st_size // 1024} KB) – skipping.")
        return True

    print(f"\n{BOLD}[3/3] Open-Meteo Historical Weather – NYC{RESET}")
    info("Source: open-meteo.com archive API (completely free, no key)")

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        "?latitude=40.7128"
        "&longitude=-74.0060"
        "&start_date=2023-01-01"
        "&end_date=2023-12-31"
        "&hourly=temperature_2m,relativehumidity_2m,windspeed_10m"
        "&timezone=America%2FNew_York"
        "&format=csv"
    )

    info("Fetching 1 year of hourly data (8 760 rows) …")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "UrbanStream/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")

        # Open-Meteo CSV has ~7 header/metadata lines before the actual data.
        # Find the line that starts with "time" (the real header).
        lines = raw.splitlines()
        header_idx = next(
            (i for i, l in enumerate(lines) if l.lower().startswith("time")),
            0,
        )
        clean_csv = "\n".join(lines[header_idx:])

        # Write directly – the producer already handles Open-Meteo column names
        with open(out, "w", encoding="utf-8") as fh:
            fh.write(clean_csv)

        row_count = clean_csv.count("\n") - 1
        ok(f"weather_nyc.csv saved – {row_count:,} rows  ({out.stat().st_size // 1024} KB)")
        return True

    except Exception as exc:
        err(f"Download failed: {exc}")
        warn("Manual fallback:")
        print("    1. Open: https://open-meteo.com/")
        print("    2. Enter location: New York City (lat 40.7128, lon -74.006)")
        print("    3. Select hourly variables:")
        print("       ✓ Temperature (2 m)")
        print("       ✓ Relative Humidity (2 m)")
        print("       ✓ Wind Speed (10 m)")
        print("    4. Set dates: 2023-01-01 → 2023-12-31")
        print("    5. Click 'Download CSV' → save as data/weather_nyc.csv")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Optional: synthetic pollution generator (--synthetic-pollution flag)
# ══════════════════════════════════════════════════════════════════════════════

def generate_synthetic_pollution():
    """Generate a realistic synthetic openaq_nyc.csv without any download."""
    import random, math
    out = DATA_DIR / "openaq_nyc.csv"

    print(f"\n{BOLD}Generating synthetic openaq_nyc.csv …{RESET}")

    stations = [
        ("Manhattan-34th St",    40.7484, -73.9967, "MN"),
        ("Manhattan-Harlem",     40.8116, -73.9465, "MN"),
        ("Manhattan-Lower",      40.7074, -74.0113, "MN"),
        ("Brooklyn-Williamsburg",40.7081, -73.9571, "BK"),
        ("Brooklyn-Sunset Park", 40.6501, -74.0046, "BK"),
        ("Queens-Flushing",      40.7675, -73.8330, "QN"),
        ("Queens-Jamaica",       40.7021, -73.7877, "QN"),
        ("Bronx-Hunts Point",    40.8040, -73.8897, "BX"),
        ("Bronx-Fordham",        40.8600, -73.8944, "BX"),
        ("Staten Island-NW",     40.6295, -74.1502, "SI"),
    ]

    rows = []
    base_pm25 = {"MN": 12.0, "BK": 9.0, "QN": 7.5, "BX": 11.0, "SI": 5.5}
    base_no2  = {"MN": 42.0, "BK": 31.0, "QN": 25.0, "BX": 38.0, "SI": 18.0}

    for month in range(1, 13):
        for day in range(1, 29):     # 28 days per month for simplicity
            for hour in range(0, 24, 3):   # every 3 hours
                dt = f"2023-{month:02d}-{day:02d}T{hour:02d}:00:00Z"
                for name, lat, lon, prefix in stations:
                    # Rush-hour spikes
                    peak = 1.4 if hour in (7, 8, 9, 17, 18, 19) else 1.0
                    # Winter baseline higher
                    season = 1.2 if month in (12, 1, 2) else 1.0

                    pm25_val = base_pm25[prefix] * peak * season + random.gauss(0, 2.0)
                    no2_val  = base_no2[prefix]  * peak * season + random.gauss(0, 5.0)

                    for param, value in [("pm25", pm25_val), ("no2", no2_val)]:
                        rows.append({
                            "location":  name,
                            "parameter": param,
                            "value":     round(max(0.1, value), 2),
                            "unit":      "µg/m³",
                            "date_utc":  dt,
                            "latitude":  round(lat, 6),
                            "longitude": round(lon, 6),
                        })

    fieldnames = ["location", "parameter", "value", "unit", "date_utc", "latitude", "longitude"]
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    ok(f"Synthetic openaq_nyc.csv generated – {len(rows):,} rows ({out.stat().st_size // 1024} KB)")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{BOLD}{'='*55}{RESET}")
    print(f"{BOLD}  UrbanStream – Dataset Downloader{RESET}")
    print(f"{BOLD}{'='*55}{RESET}")
    print(f"  Saving to: {DATA_DIR.resolve()}\n")

    if "--synthetic-pollution" in sys.argv:
        generate_synthetic_pollution()
        return

    results = {}
    results["traffic"]   = download_traffic()
    results["pollution"] = download_pollution()
    results["weather"]   = download_weather()

    print(f"\n{BOLD}{'='*55}{RESET}")
    print(f"{BOLD}  Summary{RESET}")
    print(f"{BOLD}{'='*55}{RESET}")

    all_ok = True
    for name, success in results.items():
        status = f"{GREEN}READY{RESET}" if success else f"{RED}MISSING{RESET}"
        print(f"  {status}  data/{name}_nyc.csv" if name != "traffic"
              else f"  {status}  data/nyc_traffic.csv")
        if not success:
            all_ok = False

    print()
    if all_ok:
        print(f"  {GREEN}{BOLD}All datasets ready!{RESET}")
        print(f"  You can now run:  python3 producers/run_all_producers.py")
    else:
        print(f"  {YELLOW}Some datasets missing – see manual fallback instructions above.{RESET}")
        print(f"  For pollution, you can also use the synthetic generator:")
        print(f"    python3 download_data.py --synthetic-pollution")
    print()


if __name__ == "__main__":
    main()
