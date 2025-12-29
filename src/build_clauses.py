"""
build_clauses.py: RAG Data Pipeline for Solicitation Clauses

This script automates the extraction of clauses from solicitation documents (PDFs)
found on the DC OCP website. It serves as a data ingestion pipeline for a RAG
(Retrieval-Augmented Generation) system.

Key Pipeline Steps:
1.  **Clean**: (Optional) Deletes previous output artifacts and logs.
2.  **Refresh**: (Optional) Runs upstream scripts (`dc_pass_solicitations.py` and
    `dc_pass_attachments.py`) to update the solicitation and attachment indices.
3.  **Prioritize**: Selects solicitations to process, prioritizing those known to
    have documents.
4.  **Download**: Fetches PDF attachments from the DC OCP API.
5.  **Parse**: Extracts text from PDFs using a hard-timeout multiprocessing worker
    to prevent hangs.
6.  **Split**: Breaks extracted text into individual clauses based on common
    patterns (e.g., numbered lists, bullet points).
7.  **Save**: Writes results to CSV files:
    - `clauses.csv`: All extracted clauses with metadata.
    - `clauses_rag.csv`: Filtered and de-duplicated clauses optimized for RAG.
    - `dc_pass_documents_provenance.csv`: Detailed log of every document attempted.
    - `build_clauses_summary.csv`: High-level run statistics.

Usage:
    python src/build_clauses.py [options]

Run `python src/build_clauses.py --help` for a full list of configuration options.
"""
import os
import re
import time
import argparse
import hashlib
import multiprocessing as mp
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests
from pypdf import PdfReader


# -----------------------
# Paths (directory layout)
# -----------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DATA_RAW = os.path.join(DATA_DIR, "raw")
DATA_DERIVED = os.path.join(DATA_DIR, "derived")
DATA_SRC = os.path.join(PROJECT_ROOT, "src")

ATT_INDEX = os.path.join(DATA_RAW, "dc_pass_attachments_index.csv")
PDF_DIR = os.path.join(DATA_RAW, "dc_pass_docs")
SCAN_IN = os.path.join(DATA_DERIVED, "scan_from_attachments_index.csv")

PROVENANCE_OUT = os.path.join(DATA_DERIVED, "dc_pass_documents_provenance.csv")
CLAUSES_OUT = os.path.join(DATA_DERIVED, "clauses.csv")
CLAUSES_RAG_OUT = os.path.join(DATA_DERIVED, "clauses_rag.csv")
RUN_SUMMARY_OUT = os.path.join(DATA_DERIVED, "build_clauses_summary.csv")
LOG_FILE = os.path.join(DATA_SRC, "run_status.log")

os.makedirs(DATA_RAW, exist_ok=True)
os.makedirs(DATA_DERIVED, exist_ok=True)
os.makedirs(DATA_SRC, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)


# -----------------------
# Remote endpoints
# -----------------------
DETAILS_API = "https://contracts.ocp.dc.gov/api/solicitations/details"
ATTACH_BASE = "https://contracts.ocp.dc.gov/solicitations/attachments/"


# -----------------------
# Settings
# -----------------------
HEADERS = {"User-Agent": "Req2Evidence/1.1 (research; dataset build)"}


# -----------------------
# Temporary files (optional post-run delete)
# -----------------------
TEMP_FILES_TO_DELETE = [
    os.path.join(DATA_DERIVED, "solicitations_sample.csv"),
]


# -----------------------
# Logging
# -----------------------
def log(msg: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# -----------------------
# Pre-run cleaning
# -----------------------
def wipe_directory_files(dir_path: str, keep_files=None, extensions=None) -> int:
    """
    Deletes files in a directory (non-recursive).
    - keep_files: set of basenames to keep
    - extensions: optional set/tuple like {'.csv','.log'} to restrict deletion
    Returns number deleted.
    """
    keep_files = keep_files or set()
    deleted = 0

    if not os.path.exists(dir_path):
        return 0

    for fn in os.listdir(dir_path):
        fp = os.path.join(dir_path, fn)
        if not os.path.isfile(fp):
            continue
        if fn in keep_files:
            continue
        if extensions is not None:
            ext = os.path.splitext(fn)[1].lower()
            if ext not in extensions:
                continue
        try:
            os.remove(fp)
            deleted += 1
        except Exception:
            pass

    return deleted


def clean_before_run(
    wipe_derived_all: bool = True,
    wipe_cached_pdfs: bool = True,
) -> dict:
    """
    Wipe prior outputs so each run starts fresh.

    - If wipe_derived_all=True: deletes all files inside data/derived (non-recursive).
    - If wipe_cached_pdfs=True: deletes all *.pdf inside data/raw/dc_pass_docs (non-recursive).
    """
    deleted_derived = 0
    deleted_raw_files = 0
    deleted_pdfs = 0

    # Wipe derived outputs entirely (safe; regenerated at end)
    if wipe_derived_all:
        deleted_derived = wipe_directory_files(DATA_DERIVED)

    # Optionally wipe cached PDFs
    if wipe_cached_pdfs:
        if os.path.exists(PDF_DIR):
            for fn in os.listdir(PDF_DIR):
                if fn.lower().endswith(".pdf"):
                    try:
                        os.remove(os.path.join(PDF_DIR, fn))
                        deleted_pdfs += 1
                    except Exception:
                        pass

    # Also remove the two raw indexes (they'll be rebuilt during refresh)
    for fp in [
        os.path.join(DATA_RAW, "dc_pass_attachments_index.csv"),
        os.path.join(DATA_RAW, "dc_pass_solicitations_index.csv"),
    ]:
        try:
            if os.path.exists(fp) and os.path.isfile(fp):
                os.remove(fp)
                deleted_raw_files += 1
        except Exception:
            pass

    return {
        "deleted_derived_files": deleted_derived,
        "deleted_raw_files": deleted_raw_files,
        "deleted_pdfs": deleted_pdfs,
    }


# -----------------------
# Upstream refresh
# -----------------------
def refresh_inputs(max_scan: int, scan_sleep: float) -> None:
    log("Refreshing inputs: running dc_pass_solicitations.py (best-effort)")
    try:
        from dc_pass_solicitations import main as solicitations_main
        solicitations_main()
        log("OK refreshed solicitations index.")
    except Exception as e:
        log(f"WARN solicitations refresh failed (continuing): {e}")

    log(f"Refreshing inputs: running dc_pass_attachments.py (max_scan={max_scan}, sleep={scan_sleep})")
    from dc_pass_attachments import main as attachments_main
    attachments_main(max_scan=max_scan, sleep_seconds=scan_sleep)
    log("OK refreshed attachments index + scan.")


# -----------------------
# Helpers
# -----------------------
def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def safe_filename(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"[^a-zA-Z0-9._-]+", "_", name)
    return (name[:200] if len(name) > 200 else name) or "download.pdf"


def split_into_clauses(text: str, min_len: int = 40, max_len: int = 1500):
    parts = [
        p.strip()
        for p in re.split(
            r"(?:\n|\r)+\s*(?:\d+(\.\d+)*[\)\.]|[A-Z][A-Z \-]{3,}|[•\-])\s+",
            text,
        )
        if p
    ]

    out = []
    for p in parts:
        p = re.sub(r"\s+", " ", p).strip()
        if len(p) < min_len:
            continue
        if len(p) <= max_len:
            out.append(p)
        else:
            for j in range(0, len(p), max_len):
                c = p[j : j + max_len].strip()
                if len(c) >= min_len:
                    out.append(c)
    return out


def get_details(session: requests.Session, solnum: str):
    r = session.get(DETAILS_API, params={"id": solnum}, headers=HEADERS, timeout=60)
    ct = (r.headers.get("content-type") or "").lower()

    if r.status_code == 204:
        return None, 204, ct

    r.raise_for_status()

    if "json" not in ct:
        raise ValueError(f"Expected JSON, got content-type={ct}")

    return r.json(), r.status_code, ct


def should_skip_by_size(resp: requests.Response, max_pdf_bytes: int) -> bool:
    # Prefer Content-Length if present; fall back to len(resp.content) once downloaded.
    try:
        cl = resp.headers.get("content-length")
        if cl:
            return int(cl) > max_pdf_bytes
    except Exception:
        pass
    return len(resp.content) > max_pdf_bytes


def download_pdf(session: requests.Session, token: str, download_timeout: int):
    url = ATTACH_BASE + token
    r = session.get(
        url,
        headers=HEADERS,
        timeout=(10, download_timeout),  # (connect_timeout, read_timeout)
        allow_redirects=True,
    )
    ct = (r.headers.get("content-type") or "").lower()

    is_pdf = (r.content[:4] == b"%PDF") or ("pdf" in ct)
    ok = (r.status_code == 200) and is_pdf
    return ok, r, url, ct


# -----------------------
# Hard-timeout PDF parse
# -----------------------
def _pdf_to_text_worker(pdf_bytes: bytes, max_pages: int, out_q: mp.Queue):
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        chunks = []
        for p in reader.pages[:max_pages]:
            chunks.append(p.extract_text() or "")
        out_q.put(("ok", "\n".join(chunks)))
    except Exception as e:
        out_q.put(("err", str(e)))


def pdf_to_text_timeout(pdf_bytes: bytes, max_pages: int, parse_timeout_s: float):
    q = mp.Queue()
    proc = mp.Process(target=_pdf_to_text_worker, args=(pdf_bytes, max_pages, q))
    t0 = time.time()
    proc.start()
    proc.join(parse_timeout_s)

    if proc.is_alive():
        proc.terminate()
        proc.join(1)
        return False, "", f"parse_timeout>{parse_timeout_s}s", time.time() - t0

    elapsed = time.time() - t0
    if q.empty():
        return False, "", "parse_failed_no_result", elapsed

    status, payload = q.get()
    if status == "ok":
        return True, payload, None, elapsed
    return False, "", payload, elapsed


def choose_solicitations(att_df: pd.DataFrame, max_solicitations: int):
    solnums = att_df["SOLICITATIONNUM"].dropna().astype(str).unique().tolist()

    # Prefer ones known to have docs (from scan file), for higher yield
    if os.path.exists(SCAN_IN):
        try:
            scan = pd.read_csv(SCAN_IN)
            if "solicitation_number" in scan.columns and "documents_count" in scan.columns:
                good = (
                    scan.loc[scan["documents_count"].fillna(0).astype(int) > 0, "solicitation_number"]
                    .astype(str)
                    .tolist()
                )
                good_set = set(good)
                solnums = [s for s in solnums if s in good_set] + [s for s in solnums if s not in good_set]
                log(f"Prioritized solicitations with documents_count>0 using {os.path.relpath(SCAN_IN, PROJECT_ROOT)}")
        except Exception as e:
            log(f"WARN could not use scan file for prioritization: {e}")

    return solnums[:max_solicitations]


def build_rag_file(df: pd.DataFrame, min_chars: int = 90, global_dedupe: bool = False):
    out = df.copy()
    out["clause_text"] = out["clause_text"].astype(str)
    out = out[out["clause_text"].str.strip().ne("")]

    out = out[out["clause_text"].str.len() >= min_chars]

    if "document_sha256" in out.columns:
        out = out.drop_duplicates(subset=["document_sha256", "clause_text"])

    if global_dedupe:
        out = out.drop_duplicates(subset=["clause_text"])

    return out


# -----------------------
# Post-run cleanup
# -----------------------
def cleanup_pdfs(pdf_dir: str) -> int:
    deleted = 0
    if not os.path.exists(pdf_dir):
        return 0
    for fn in os.listdir(pdf_dir):
        if fn.lower().endswith(".pdf"):
            try:
                os.remove(os.path.join(pdf_dir, fn))
                deleted += 1
            except Exception:
                pass
    return deleted


def cleanup_temp_files(paths) -> int:
    deleted = 0
    for p in paths:
        try:
            if os.path.exists(p) and os.path.isfile(p):
                os.remove(p)
                deleted += 1
        except Exception:
            pass
    return deleted


# -----------------------
# Main
# -----------------------
def main(
    # Clean behavior
    wipe_derived_all: bool = True,
    wipe_cached_pdfs_before: bool = True,

    # Refresh behavior
    refresh: bool = True,
    max_scan: int = 2000,
    scan_sleep: float = 0.25,

    # Run sizing
    max_solicitations: int = 200,
    max_docs_total: int = 800,
    sleep_seconds: float = 0.35,
    max_pages: int = 80,

    # RAG output controls
    rag_min_chars: int = 90,
    rag_global_dedupe: bool = False,

    # Per-file speed limits + size limit
    download_timeout: int = 15,
    parse_timeout: float = 8.0,
    max_pdf_mb: int = 10,

    # Cleanup after run
    cleanup_pdfs_after: bool = True,
    cleanup_temp_after: bool = True,
):
    max_pdf_bytes = int(max_pdf_mb) * 1024 * 1024

    # Start clean: wipe derived + cached PDFs (as requested)
    clean_stats = clean_before_run(
        wipe_derived_all=wipe_derived_all,
        wipe_cached_pdfs=wipe_cached_pdfs_before,
    )

    # Now reset log for this run and log the cleaning outcome
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("")
    log(
        "CLEAN start: "
        f"deleted_derived_files={clean_stats['deleted_derived_files']} "
        f"deleted_raw_files={clean_stats['deleted_raw_files']} "
        f"deleted_pdfs={clean_stats['deleted_pdfs']}"
    )

    if refresh:
        refresh_inputs(max_scan=max_scan, scan_sleep=scan_sleep)

    if not os.path.exists(ATT_INDEX):
        raise FileNotFoundError(f"Missing input: {ATT_INDEX}")

    att = pd.read_csv(ATT_INDEX)
    if "SOLICITATIONNUM" not in att.columns:
        raise ValueError("ATT_INDEX missing SOLICITATIONNUM column")

    solnums = choose_solicitations(att, max_solicitations=max_solicitations)
    log(f"Loaded ATT_INDEX rows={len(att)}; running solicitations={len(solnums)}; max_docs_total={max_docs_total}")

    prov_rows = []
    clause_rows = []

    start = time.time()
    docs_attempted = 0
    docs_downloaded = 0
    download_fail = 0
    nonpdf_skips = 0
    dl_timeout_skips = 0
    oversize_skips = 0
    parse_fail = 0
    parse_timeout_skips = 0
    details_fail = 0
    details_204 = 0
    text_len_zero = 0

    with requests.Session() as session:
        for i, solnum in enumerate(solnums, 1):
            if docs_downloaded >= max_docs_total:
                break

            try:
                details, http_status, ct = get_details(session, solnum)
            except Exception as e:
                details_fail += 1
                log(f"[SOL {i}/{len(solnums)}] DETAILS_FAIL id={solnum} err={e}")
                prov_rows.append({"solicitation_number": solnum, "stage": "details", "ok": False, "error": str(e)})
                time.sleep(sleep_seconds)
                continue

            if details is None:
                details_204 += 1
                log(f"[SOL {i}/{len(solnums)}] DETAILS_204 id={solnum}")
                prov_rows.append(
                    {"solicitation_number": solnum, "stage": "details", "ok": False, "http_status": http_status, "content_type": ct}
                )
                time.sleep(sleep_seconds)
                continue

            docs = details.get("documents") or []
            log(f"[SOL {i}/{len(solnums)}] id={solnum} docs={len(docs)} title={details.get('title')}")

            for d in docs:
                if docs_downloaded >= max_docs_total:
                    break

                token = d.get("id")
                if not token:
                    continue

                doc_name = d.get("name") or f"{token}.pdf"
                docs_attempted += 1

                try:
                    ok, resp, attempted, content_type = download_pdf(session, token, download_timeout=download_timeout)
                except requests.exceptions.Timeout:
                    dl_timeout_skips += 1
                    download_fail += 1
                    log(f"  DL_SKIP timeout>{download_timeout}s name={doc_name}")
                    time.sleep(sleep_seconds)
                    continue
                except Exception as e:
                    download_fail += 1
                    log(f"  DL_SKIP exception name={doc_name} err={e}")
                    time.sleep(sleep_seconds)
                    continue

                if not ok:
                    nonpdf_skips += 1
                    prov_rows.append(
                        {
                            "solicitation_number": solnum,
                            "title": details.get("title"),
                            "agency": ",".join(details.get("agencyNames") or []),
                            "docname": doc_name,
                            "document_token": token,
                            "attempted_url": attempted,
                            "resolved_url": resp.url,
                            "downloaded": False,
                            "http_status": resp.status_code,
                            "content_type": content_type,
                        }
                    )
                    log(f"  DL_SKIP nonpdf_or_fail http={resp.status_code} ctype={content_type} name={doc_name}")
                    time.sleep(sleep_seconds)
                    continue

                # Skip oversized PDFs (> max_pdf_mb)
                if should_skip_by_size(resp, max_pdf_bytes=max_pdf_bytes):
                    oversize_skips += 1
                    prov_rows.append(
                        {
                            "solicitation_number": solnum,
                            "title": details.get("title"),
                            "agency": ",".join(details.get("agencyNames") or []),
                            "docname": doc_name,
                            "document_token": token,
                            "attempted_url": attempted,
                            "resolved_url": resp.url,
                            "downloaded": True,
                            "parsed": False,
                            "parse_elapsed_s": None,
                            "parse_error": f"skipped_oversize>{max_pdf_mb}MB",
                            "extracted_text_len": 0,
                            "sha256": None,
                            "bytes": len(resp.content),
                            "pdf_path": None,
                        }
                    )
                    log(f"  DL_SKIP oversize bytes={len(resp.content)} limit_mb={max_pdf_mb} name={doc_name}")
                    time.sleep(sleep_seconds)
                    continue

                docs_downloaded += 1

                fname = safe_filename(doc_name)
                pdf_path = os.path.join(PDF_DIR, fname)
                with open(pdf_path, "wb") as f:
                    f.write(resp.content)

                h = sha256_bytes(resp.content)
                doc_key = f"dcpass_{h[:16]}"

                ok_parse, text, parse_err, parse_elapsed = pdf_to_text_timeout(
                    resp.content, max_pages=max_pages, parse_timeout_s=parse_timeout
                )

                if not ok_parse:
                    parse_fail += 1
                    if parse_err and "parse_timeout" in parse_err:
                        parse_timeout_skips += 1
                    log(f"  PARSE_SKIP elapsed_s={parse_elapsed:.1f} err={parse_err} name={doc_name}")
                    clauses = []
                    parsed = False
                    text_len = 0
                else:
                    text_len = len(text.strip())
                    if text_len == 0:
                        text_len_zero += 1
                    clauses = split_into_clauses(text)
                    parsed = True

                prov_rows.append(
                    {
                        "solicitation_number": solnum,
                        "title": details.get("title"),
                        "agency": ",".join(details.get("agencyNames") or []),
                        "docname": doc_name,
                        "document_token": token,
                        "attempted_url": attempted,
                        "resolved_url": resp.url,
                        "downloaded": True,
                        "parsed": parsed,
                        "parse_elapsed_s": round(parse_elapsed, 3),
                        "parse_error": parse_err,
                        "extracted_text_len": text_len,
                        "sha256": h,
                        "bytes": len(resp.content),
                        "pdf_path": os.path.relpath(pdf_path, PROJECT_ROOT),
                    }
                )

                for j, c in enumerate(clauses):
                    clause_rows.append(
                        {
                            "solicitation_number": solnum,
                            "solicitation_title": details.get("title"),
                            "agency": ",".join(details.get("agencyNames") or []),
                            "source_url": resp.url,
                            "document_id": doc_key,
                            "document_sha256": h,
                            "attachment_name": doc_name,
                            "clause_id": f"{doc_key}_c{j:05d}",
                            "clause_text": c,
                        }
                    )

                elapsed = time.time() - start
                log(
                    f"  STATUS sol={i}/{len(solnums)} docs_downloaded={docs_downloaded}/{max_docs_total} "
                    f"clauses={len(clause_rows)} elapsed_s={elapsed:.1f}"
                )

                time.sleep(sleep_seconds)

    prov_df = pd.DataFrame(prov_rows)
    clauses_df = pd.DataFrame(clause_rows)

    prov_df.to_csv(PROVENANCE_OUT, index=False)
    clauses_df.to_csv(CLAUSES_OUT, index=False)

    rag_df = build_rag_file(clauses_df, min_chars=rag_min_chars, global_dedupe=rag_global_dedupe)
    rag_df.to_csv(CLAUSES_RAG_OUT, index=False)

    summary = pd.DataFrame(
        [
            {
                "ts_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "wipe_derived_all": wipe_derived_all,
                "wipe_cached_pdfs_before": wipe_cached_pdfs_before,
                "refresh_inputs": refresh,
                "max_scan": max_scan,
                "scan_sleep": scan_sleep,
                "solicitations_planned": len(solnums),
                "docs_attempted": docs_attempted,
                "docs_downloaded": docs_downloaded,
                "download_fail": download_fail,
                "nonpdf_skips": nonpdf_skips,
                "dl_timeout_skips": dl_timeout_skips,
                "oversize_skips": oversize_skips,
                "details_fail": details_fail,
                "details_204": details_204,
                "parse_fail": parse_fail,
                "parse_timeout_skips": parse_timeout_skips,
                "text_len_zero": text_len_zero,
                "clauses_rows": len(clauses_df),
                "clauses_rag_rows": len(rag_df),
                "rag_min_chars": rag_min_chars,
                "rag_global_dedupe": rag_global_dedupe,
                "download_timeout_s": download_timeout,
                "parse_timeout_s": parse_timeout,
                "max_pdf_mb": max_pdf_mb,
                "cleanup_pdfs_after": cleanup_pdfs_after,
                "cleanup_temp_after": cleanup_temp_after,
                "clean_start_deleted_derived_files": clean_stats["deleted_derived_files"],
                "clean_start_deleted_raw_files": clean_stats["deleted_raw_files"],
                "clean_start_deleted_pdfs": clean_stats["deleted_pdfs"],
            }
        ]
    )
    summary.to_csv(RUN_SUMMARY_OUT, index=False)

    deleted_pdfs = 0
    deleted_temp = 0
    if cleanup_pdfs_after:
        deleted_pdfs = cleanup_pdfs(PDF_DIR)
    if cleanup_temp_after:
        deleted_temp = cleanup_temp_files(TEMP_FILES_TO_DELETE)

    log("----- RUN SUMMARY -----")
    log(
        f"docs_attempted={docs_attempted} downloaded={docs_downloaded} "
        f"download_fail={download_fail} nonpdf_skips={nonpdf_skips} dl_timeout_skips={dl_timeout_skips} oversize_skips={oversize_skips}"
    )
    log(f"details_fail={details_fail} details_204={details_204}")
    log(f"parse_fail={parse_fail} parse_timeout_skips={parse_timeout_skips} text_len_zero={text_len_zero}")
    log(f"clauses_rows={len(clauses_df)} clauses_rag_rows={len(rag_df)}")
    log(f"Wrote: {os.path.relpath(PROVENANCE_OUT, PROJECT_ROOT)}")
    log(f"Wrote: {os.path.relpath(CLAUSES_OUT, PROJECT_ROOT)}")
    log(f"Wrote: {os.path.relpath(CLAUSES_RAG_OUT, PROJECT_ROOT)}")
    log(f"Wrote: {os.path.relpath(RUN_SUMMARY_OUT, PROJECT_ROOT)}")
    if cleanup_pdfs_after:
        log(f"Cleanup: deleted_pdfs={deleted_pdfs} from {os.path.relpath(PDF_DIR, PROJECT_ROOT)}")
    if cleanup_temp_after:
        log(f"Cleanup: deleted_temp_files={deleted_temp}")


if __name__ == "__main__":
    mp.freeze_support()

    ap = argparse.ArgumentParser()

    # Start-clean behavior
    ap.add_argument("--no-wipe-derived", action="store_true", help="Do not wipe all files in data/derived before run.")
    ap.add_argument("--no-wipe-pdfs-before", action="store_true", help="Do not wipe cached PDFs before run.")

    # Refresh / upstream
    ap.add_argument("--no-refresh", action="store_true", help="Do not run upstream scripts; use existing CSVs.")
    ap.add_argument("--max-scan", type=int, default=2000)
    ap.add_argument("--scan-sleep", type=float, default=0.25)

    # Main run sizing
    ap.add_argument("--max-solicitations", type=int, default=200)
    ap.add_argument("--max-docs", type=int, default=800)
    ap.add_argument("--sleep", type=float, default=0.35)
    ap.add_argument("--max-pages", type=int, default=80)

    # RAG output controls
    ap.add_argument("--rag-min-chars", type=int, default=90)
    ap.add_argument("--rag-global-dedupe", action="store_true")

    # Per-file limits
    ap.add_argument("--download-timeout", type=int, default=15)
    ap.add_argument("--parse-timeout", type=float, default=8.0)
    ap.add_argument("--max-pdf-mb", type=int, default=10)

    # Cleanup after run
    ap.add_argument("--no-cleanup-pdfs", action="store_true")
    ap.add_argument("--no-cleanup-temp", action="store_true")

    args = ap.parse_args()

    main(
        wipe_derived_all=(not args.no_wipe_derived),
        wipe_cached_pdfs_before=(not args.no_wipe_pdfs_before),
        refresh=(not args.no_refresh),
        max_scan=args.max_scan,
        scan_sleep=args.scan_sleep,
        max_solicitations=args.max_solicitations,
        max_docs_total=args.max_docs,
        sleep_seconds=args.sleep,
        max_pages=args.max_pages,
        rag_min_chars=args.rag_min_chars,
        rag_global_dedupe=args.rag_global_dedupe,
        download_timeout=args.download_timeout,
        parse_timeout=args.parse_timeout,
        max_pdf_mb=args.max_pdf_mb,
        cleanup_pdfs_after=(not args.no_cleanup_pdfs),
        cleanup_temp_after=(not args.no_cleanup_temp),
    )
