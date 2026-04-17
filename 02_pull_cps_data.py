"""
Download CPS Basic Monthly microdata from IPUMS via the API.

Pulls Basic Monthly CPS for selected years, with variables needed
for sex × age × education analysis of LFPR and employment rates.

Requirements:
    pip install ipumspy

Usage:
    export IPUMS_API_KEY="your-api-key-here"
    python 1.download_data.py
"""

import gzip
import os
import re
import shutil
import sys
from pathlib import Path

from ipumspy import IpumsApiClient, MicrodataExtract

# Years to download — all 12 Basic Monthly samples per year
TARGET_YEARS = [1992, 2019, 2024]


def get_cps_samples(ipums):
    """Fetch all available CPS sample IDs from the IPUMS API."""
    print("Fetching available CPS samples from IPUMS...")
    all_samples = ipums.get_all_sample_info("cps")
    print(f"  Found {len(all_samples)} total CPS samples.")
    return all_samples


def select_basic_monthly_samples(all_samples, target_years):
    """Select Basic Monthly sample IDs for the specified years.

    Basic Monthly samples match pattern cpsYYYY_MMb or cpsYYYY_MMs.
    Exclude ASEC supplement samples (March 's' suffix).
    """
    pattern = re.compile(r"^cps(\d{4})_(\d{2})[bs]$")
    selected = []
    for sample_id in sorted(all_samples):
        m = pattern.match(sample_id)
        if not m:
            continue
        year = int(m.group(1))
        if year not in target_years:
            continue
        if sample_id.endswith("_03s"):
            continue
        selected.append(sample_id)
    return selected


def archive_existing_files(src_dir, archive_dir):
    """Move all files from src_dir into archive_dir."""
    files = list(src_dir.iterdir()) if src_dir.exists() else []
    files = [f for f in files if f.is_file()]
    if not files:
        return
    archive_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        dest = archive_dir / f.name
        print(f"  Archiving {f.name} -> {dest}")
        shutil.move(str(f), str(dest))


def read_renviron(key):
    """Read a value from ~/.Renviron if it exists."""
    renviron = Path.home() / ".Renviron"
    if not renviron.exists():
        return None
    for line in renviron.read_text().splitlines():
        line = line.strip()
        if line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.strip() == key:
            return v.strip().strip('"').strip("'")
    return None


def main():
    api_key = os.environ.get("IPUMS_API_KEY", "").strip()
    if not api_key:
        api_key = read_renviron("IPUMS_API_KEY") or ""
    if not api_key:
        print("Error: IPUMS_API_KEY not found in environment or ~/.Renviron")
        print("Get your API key at https://account.ipums.org/api_keys")
        sys.exit(1)

    ipums = IpumsApiClient(api_key)
    download_dir = Path("../data/")
    download_dir.mkdir(parents=True, exist_ok=True)

    # --- Archive existing files ---

    archive_dir = Path("../archive")
    print("Archiving existing files...")
    archive_existing_files(download_dir, archive_dir)

    # --- Discover available samples ---

    all_samples = get_cps_samples(ipums)
    samples = select_basic_monthly_samples(all_samples, TARGET_YEARS)

    print(f"  Selected {len(samples)} samples for years {TARGET_YEARS}")
    for s in samples:
        print(f"    {s}")

    # --- Define extract ---

    extract = MicrodataExtract(
        collection="cps",
        samples=samples,
        variables=[
            "YEAR", "SERIAL", "MONTH", "PERNUM",
            "WTFINL", "AGE", "SEX", "EMPSTAT",
            "LABFORCE", "CLASSWKR", "EDUC", "SCHLCOLL",
        ],
        description=f"CPS Basic Monthly {TARGET_YEARS}",
        data_format="stata",
        data_structure={"rectangular": {"on": "P"}},
    )

    # --- Submit extract ---

    print("Submitting CPS extract...")
    try:
        ipums.submit_extract(extract)
    except Exception as e:
        print(f"Error submitting extract: {e}")
        sys.exit(1)
    print(f"  Submitted (extract ID: {extract.extract_id})")

    # --- Wait for completion ---

    print("Waiting for extract to complete...")
    ipums.wait_for_extract(extract)
    print("  Extract complete.")

    # --- Download ---

    print("Downloading extract...")
    ipums.download_extract(extract, download_dir=download_dir)
    print(f"  Downloaded to {download_dir}")

    # --- Decompress .gz files ---

    for gz_file in download_dir.glob("*.gz"):
        out_file = gz_file.with_suffix("")
        print(f"  Decompressing {gz_file.name}...")
        with gzip.open(gz_file, "rb") as f_in, open(out_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        gz_file.unlink()

    print("Done.")


if __name__ == "__main__":
    main()
