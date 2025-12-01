import os
import re
import csv
import requests
import pdfplumber
import yaml
from bs4 import BeautifulSoup
from csv_writer import CaseCSVWriter

# Load config
with open("config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

#configuration parameters loaded from the config file 
BASE_URL = cfg["urls"]["base_url"]
LISTING_URL = cfg["urls"]["listing_url"]
PDF_DIR = cfg["paths"]["pdf_dir"]
OUTPUT_DIR = cfg["paths"]["output_dir"]
LOG_DIR = cfg["paths"]["log_dir"]

# join the filenames with the correct base folders
REPORT_FILE = os.path.join(LOG_DIR, cfg["paths"]["report_file"])
OUTPUT_CSV = os.path.join(OUTPUT_DIR, cfg["paths"]["output_csv"])
DOWNLOAD_LOG = os.path.join(LOG_DIR, cfg["paths"]["download_log"])

MAX_PDF_PAGES = cfg["extraction"]["max_pdf_pages"]
HEADER_MAX_LINES = cfg["extraction"]["header_max_lines"]
LISTING_TIMEOUT = cfg["network"]["listing_timeout"]
PDF_TIMEOUT = cfg["network"]["pdf_timeout"]
DATE_PATTERN = re.compile(r"\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\b", re.I)

#handle directories for logs and pdfs 
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# logger for the download process and helpful while debug incase of failure and log retries   
def log_download_attempt(case_id, pdf_url, status, message=""):
    write_header = not os.path.exists(DOWNLOAD_LOG)
    with open(DOWNLOAD_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["case_id", "pdf_url", "status", "message"])
        writer.writerow([case_id, pdf_url, status, message])


# fetching the data from the provided source URL in the config 
def fetch_listing_cases():
    try:
        r = requests.get(LISTING_URL, timeout=LISTING_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"[ERROR] Could not load listing: {e}")
        return []

    ids = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue

        # accept both /gd/s/ and /gd/gd/ patterns and then extract the last part
        if "/gd/" in href and "_" in href:
            parts = href.split("/")
            candidate = parts[-1]
            # basic safety: year_prefix_courtcode_number, e.g. 2025_SGHCR_33
            if re.match(r"^\d{4}_[A-Z]+_\d+$", candidate):
                ids.append(candidate)

    ids = list(set(ids))
    print(f"[DEBUG] Found {len(ids)} potential case ids on listing page.")
    return ids


# downlaod the files from the URL 
def download_all_pdfs(case_ids):
    downloaded = []

    for cid in case_ids:
        # use the gd/gd pattern for PDFs
        pdf_url = f"{BASE_URL}/gd/gd/{cid}/pdf"
        save_path = os.path.join(PDF_DIR, f"{cid}.pdf")

        if os.path.exists(save_path):
            log_download_attempt(cid, pdf_url, "SKIPPED_ALREADY_EXISTS", "")
            downloaded.append(save_path)
            continue

        print(f"Downloading {pdf_url}...")
        try:
            r = requests.get(pdf_url, timeout=PDF_TIMEOUT)
            if r.status_code == 200 and b"%PDF" in r.content[:20]:
                with open(save_path, "wb") as f:
                    f.write(r.content)
                log_download_attempt(cid, pdf_url, "SUCCESS", "")
                downloaded.append(save_path)
            else:
                msg = f"Status {r.status_code}, not a valid PDF"
                print(f"[WARN] {msg} for {cid}")
                log_download_attempt(cid, pdf_url, "INVALID_PDF", msg)
        except Exception as e:
            msg = f"Exception: {e}"
            print(f"[WARN] Failed to download {cid}: {e}")
            log_download_attempt(cid, pdf_url, "FAILED", msg)

    return downloaded


# logic to extract the date from the documents 
def extract_decision_date(text):
    lines = text.splitlines()
    n = len(lines)

    # first check for an explicit "Decision Date" label
    for i, line in enumerate(lines):
        if "decision date" in line.lower():
            m = DATE_PATTERN.search(line)
            if m:
                return m.group(1).strip()
            if i + 1 < n:
                m2 = DATE_PATTERN.search(lines[i + 1])
                if m2:
                    return m2.group(1).strip()

    # next, look for generic "Date ..." lines
    for i, line in enumerate(lines):
        if "decision date" in line.lower():
            continue
        if re.search(r"\bDate\b", line):
            m = DATE_PATTERN.search(line)
            if m:
                return m.group(1).strip()

    # finally, use a header-based heuristic (modern SGHCR style)
    header_end = n
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("Introduction"):
            header_end = i
            break
        if re.match(r"^\s*\d+\s", stripped):
            header_end = i
            break

    header_end = min(header_end, HEADER_MAX_LINES, n)

    date_lines = []
    for i in range(header_end):
        line = lines[i]
        matches = DATE_PATTERN.findall(line)
        if matches:
            date_lines.append((i, line, matches))

    if not date_lines:
        return None

    single_date_lines = [(i, line, m) for (i, line, m) in date_lines if len(m) == 1]
    candidate_list = single_date_lines if single_date_lines else date_lines

    i, line, matches = candidate_list[-1]
    decision_date = matches[-1]
    return decision_date.strip()


# go through all pdfs and apply the extraction logic 
def extract_all_pdfs():
    processed = set()
    if os.path.exists(REPORT_FILE):
        with open(REPORT_FILE, "r") as f:
            processed = set(line.strip() for line in f.readlines())

    rows = []

    for filename in os.listdir(PDF_DIR):
        if not filename.endswith(".pdf"):
            continue

        cid = filename.replace(".pdf", "")
        if cid in processed:
            continue

        path = os.path.join(PDF_DIR, filename)

        try:
            text = ""
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages[:MAX_PDF_PAGES]:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"

            decision_date = extract_decision_date(text)
            rows.append([cid, decision_date])

            with open(REPORT_FILE, "a") as f:
                f.write(cid + "\n")

            print(f"[OK] {cid} – {decision_date}")
        except Exception as e:
            print(f"[ERROR] Failed on {cid}: {e}")

    return rows


# main function to call the workflow step by step
def main():
    print("Fetching case identifiers...")
    case_ids = fetch_listing_cases()
    print(f"Found {len(case_ids)} cases.\n")

    print("Downloading PDFs...")
    download_all_pdfs(case_ids)

    print("\nExtracting decision dates from PDFs...")
    rows = extract_all_pdfs()

    print("\nWriting results to CSV...")
    writer = CaseCSVWriter(OUTPUT_CSV, header=["case_id", "decision_date"])
    writer.append_rows(rows)

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()