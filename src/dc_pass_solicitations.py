"""
dc_pass_solicitations.py: Solicitation Metadata Fetcher

This script retrieves a comprehensive index of all solicitations from the DC
ArcGIS Government Operations MapServer. It serves as the initial step in the
data ingestion pipeline, providing the solicitation numbers needed for
subsequent attachment fetching and clause extraction.

Output Files:
- `data/raw/dc_pass_solicitations_index.csv`: Full list of solicitations with
  metadata (e.g., title, agency, status, post date).
- `data/derived/solicitations_sample.csv`: A small sample (first 50 rows) for
  quick verification.

Usage:
    python src/dc_pass_solicitations.py
"""
import os
import pandas as pd
import requests


# -----------------------
# Paths (directory layout)
# -----------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DATA_RAW = os.path.join(PROJECT_ROOT, "data", "raw")
DATA_DERIVED = os.path.join(PROJECT_ROOT, "data", "derived")
os.makedirs(DATA_RAW, exist_ok=True)
os.makedirs(DATA_DERIVED, exist_ok=True)

SOL_INDEX_OUT = os.path.join(DATA_RAW, "dc_pass_solicitations_index.csv")
SAMPLE_OUT = os.path.join(DATA_DERIVED, "solicitations_sample.csv")


# -----------------------
# Endpoint (ArcGIS layer)
# -----------------------
# NOTE: This is the PASS solicitations layer (not the attachments table).
ARCGIS_SOLICITATIONS_LAYER = (
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Government_Operations/MapServer/19/query"
)

HEADERS = {"User-Agent": "Req2Evidence/0.4 (research; dataset build)"}


def arcgis_query(where="1=1", out_fields="*", result_offset=0, result_record_count=1000):
    params = {
        "f": "json",
        "where": where,
        "outFields": out_fields,
        "returnGeometry": "false",
        "resultOffset": result_offset,
        "resultRecordCount": result_record_count,
    }
    r = requests.get(ARCGIS_SOLICITATIONS_LAYER, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch_all_solicitations():
    print("Fetching all solicitations from ArcGIS layer...")
    rows = []
    offset = 0
    page_size = 1000
    page = 0

    while True:
        page += 1
        print(f"Fetching page {page} (offset {offset})...")
        try:
            j = arcgis_query(result_offset=offset, result_record_count=page_size)
            feats = j.get("features") or []
            print(f"Page {page}: retrieved {len(feats)} records.")
            if not feats:
                print("No more records. Stopping.")
                break
            for f in feats:
                rows.append(f.get("attributes") or {})
            offset += len(feats)
            if len(feats) < page_size:
                print(f"Retrieved fewer than {page_size} records. End of data.")
                break
        except Exception as e:
            print(f"Error fetching page {page}: {e}")
            break

    print(f"Total records fetched: {len(rows)}")
    return pd.DataFrame(rows)


def main():
    print("Starting solicitation data fetch...")
    df = fetch_all_solicitations()
    print(f"Saving full dataset to {SOL_INDEX_OUT}...")
    df.to_csv(SOL_INDEX_OUT, index=False)
    print(f"Wrote: {SOL_INDEX_OUT} with {len(df)} rows.")

    # Optional sample output for quick sanity check
    cols = [c for c in ["SOLICITATIONNUMBER", "SOLICITATIONTITLE", "AGENCY_NAME", "STATUS", "POSTDATE"] if c in df.columns]
    print(f"Creating sample CSV with first 50 rows...")
    df[cols].head(50).to_csv(SAMPLE_OUT, index=False)
    print(f"Wrote: {SAMPLE_OUT} with 50 rows.")
    print("Execution complete.")


if __name__ == "__main__":
    main()
