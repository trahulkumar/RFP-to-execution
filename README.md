# Req2Evidence: RFP-to-Execution RAG Pipeline

This project provides a data ingestion pipeline for extracting solicitation clauses from the DC OCP website. The extracted data is optimized for use in a Retrieval-Augmented Generation (RAG) system.

## Pipeline Overview

The pipeline consists of three main scripts that should be run in order:

1.  **`src/dc_pass_solicitations.py`**: Fetches a comprehensive index of all solicitations from the DC ArcGIS MapServer.
2.  **`src/dc_pass_attachments.py`**: Retrieves attachment metadata and scans for document counts to prioritize solicitations with high document yields.
3.  **`src/build_clauses.py`**: The main execution script. It downloads PDF attachments, extracts text using a robust multiprocessing worker, and splits the text into individual clauses.

## Project Structure

- `src/`: Contains the Python source code for the pipeline.
- `data/raw/`: Stores raw data, including solicitation/attachment indices and downloaded PDFs.
- `data/derived/`: Stores processed data, such as extracted clauses and run summaries.
- `archive/`: (Optional) Directory for archived data or previous runs.

## Setup and Usage

### Prerequisites

- Python 3.13+
- Dependencies: `pandas`, `requests`, `pypdf`

### Running the Pipeline

You can run the full pipeline using `build_clauses.py` with the `--refresh` flag:

```bash
python src/build_clauses.py --refresh
```

Alternatively, run the scripts individually for more control:

```bash
python src/dc_pass_solicitations.py
python src/dc_pass_attachments.py --max-scan 500
python src/build_clauses.py --max-solicitations 200
```

Run any script with `--help` to see all available configuration options.

## Key Outputs

- `data/derived/clauses_rag.csv`: The primary output for RAG systems, containing de-duplicated and filtered clauses.
- `data/derived/build_clauses_summary.csv`: High-level statistics for the latest pipeline run.
- `src/run_status.log`: Detailed execution log.
