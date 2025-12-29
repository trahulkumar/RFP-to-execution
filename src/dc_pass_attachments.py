"""
dc_pass_attachments.py: Attachment Metadata Fetcher and Document Scanner

This script performs two primary functions to support the solicitation clause
extraction pipeline:
1.  **Fetch Attachment Table**: Queries the DC ArcGIS Government Operations
    MapServer to retrieve a comprehensive index of all solicitation attachments.
2.  **Scan Document Counts**: For a subset of solicitations, it calls the DC OCP
    Solicitation Details API to count the actual number of documents available.
    This helps prioritize solicitations with high document yields in subsequent
    pipeline steps.

Output Files:
- `data/raw/dc_pass_attachments_index.csv`: Full list of attachments from ArcGIS.
- `data/derived/scan_from_attachments_index.csv`: Results of the document count scan.

Usage:
    python src/dc_pass_attachments.py [options]

Run `python src/dc_pass_attachments.py --help` for a full list of configuration options.
"""
import os
import time
import argparse
import pandas as pd
import requests

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_RAW = os.path.join(PROJECT_ROOT, "data", "raw")
DATA_DERIVED = os.path.join(PROJECT_ROOT, "data", "derived")
os.makedirs(DATA_RAW, exist_ok=True)
os.makedirs(DATA_DERIVED, exist_ok=True)

ATT_INDEX_OUT = os.path.join(DATA_RAW, "dc_pass_attachments_index.csv")
SCAN_OUT = os.path.join(DATA_DERIVED, "scan_from_attachments_index.csv")

ARCGIS_ATTACHMENTS_TABLE = (
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Government_Operations/MapServer/20/query"
)
DETAILS_API = "https://contracts.ocp.dc.gov/api/solicitations/details"

HEADERS = {"User-Agent": "Req2Evidence/0.6 (research; dataset build)"}


def arcgis_query(where="1=1", out_fields="*", result_offset=0, result_record_count=1000):
    params = {
        "f": "json",
        "where": where,
        "outFields": out_fields,
        "returnGeometry": "false",
        "resultOffset": result_offset,
        "resultRecordCount": result_record_count,
    }
    r = requests.get(ARCGIS_ATTACHMENTS_TABLE, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch_all_attachments_table():
    rows = []
    offset = 0
    page_size = 1000
    while True:
        j = arcgis_query(result_offset=offset, result_record_count=page_size)
        feats = j.get("features") or []
        if not feats:
            break
        for f in feats:
            rows.append(f.get("attributes") or {})
        offset += len(feats)
        if len(feats) < page_size:
            break
    return pd.DataFrame(rows)


def scan_portal_documents_count(solnums, max_scan=None, sleep_seconds=0.25):
    if max_scan is None:
        max_scan = len(solnums)

    out = []
    with requests.Session() as sess:
        for i, solnum in enumerate(solnums[:max_scan], 1):
            if i % 50 == 0 or i == 1:
                print(f"Scan progress {i}/{min(max_scan, len(solnums))}")

            try:
                r = sess.get(DETAILS_API, params={"id": solnum}, headers=HEADERS, timeout=60)
                ct = (r.headers.get("content-type") or "").lower()

                if r.status_code == 204:
                    out.append({"solicitation_number": solnum, "ok": False, "http_status": 204,
                                "content_type": ct, "documents_count": 0})
                    continue

                if r.status_code != 200 or "json" not in ct:
                    out.append({"solicitation_number": solnum, "ok": False, "http_status": r.status_code,
                                "content_type": ct, "documents_count": None})
                    continue

                j = r.json()
                docs = j.get("documents") or []
                out.append({"solicitation_number": solnum, "ok": True, "http_status": 200,
                            "content_type": ct, "documents_count": len(docs), "title": j.get("title")})
            except Exception as e:
                out.append({"solicitation_number": solnum, "ok": False, "http_status": None,
                            "content_type": None, "documents_count": None, "error": str(e)})

            time.sleep(sleep_seconds)

    return pd.DataFrame(out)


def main(max_scan=500, sleep_seconds=0.25):
    df = fetch_all_attachments_table()
    df.to_csv(ATT_INDEX_OUT, index=False)
    print(f"Wrote: {ATT_INDEX_OUT} rows={len(df)}")

    if "SOLICITATIONNUM" not in df.columns:
        print("No SOLICITATIONNUM column found; skipping scan.")
        return

    solnums = df["SOLICITATIONNUM"].dropna().astype(str).drop_duplicates().tolist()
    scan = scan_portal_documents_count(solnums, max_scan=max_scan, sleep_seconds=sleep_seconds)
    scan.to_csv(SCAN_OUT, index=False)
    print(f"Wrote: {SCAN_OUT} rows={len(scan)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-scan", type=int, default=500, help="How many solicitation numbers to call the portal API for.")
    ap.add_argument("--sleep", type=float, default=0.25, help="Seconds between portal calls.")
    args = ap.parse_args()
    main(max_scan=args.max_scan, sleep_seconds=args.sleep)
