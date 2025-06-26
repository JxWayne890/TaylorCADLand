"""
Taylor CAD ≥10-acre parcel extractor  – June 2025
──────────────────────────────────────────────────
This version reads the LAND-DETAIL file using the **official PTAD layout**
so acreage is parsed correctly.

Key PTAD columns we need
────────────────────────
Offset  Width  Field
0-11    12     Account Number (key used across all tables)
63      1      Size Indicator   (F = acres, S = square-feet, etc.)
64-75   12     Size Amount      (implied 2 decimals for acres)

We divide the 12-digit SizeAmount by 100 to get fractional acres.
Only SizeIndicator == "F" rows are kept; others are ignored.

The script merges those land rows with APPRAISAL_INFO to
produce a CSV of every parcel ≥ 10 acres with owner & mailing address.

Usage (local test):
    python python/taylor_parcel_parser.py roll.zip

Dependencies:
    pandas  (for APPRAISAL_INFO join)
    tqdm    (optional nice progress bar)
"""

import sys, zipfile, csv
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Helper – read APPRAISAL_INFO into DataFrame (fixed-width spec)
# ---------------------------------------------------------------------------
WIDTHS_INFO = [12,1,4,35,35,35,35,9]  # acct, pt, yr, owner1, owner2, mail1, mail2, zip
COLS_INFO   = ["acct","pt","yr","owner1","owner2","mail1","mail2","zip"]

def read_info_df(zip_path: Path):
    with zipfile.ZipFile(zip_path) as zf:
        member = next(m for m in zf.namelist() if "APPRAISAL_INFO" in m.upper())
        with zf.open(member) as fh:
            return pd.read_fwf(fh, widths=WIDTHS_INFO, names=COLS_INFO, dtype=str)

# ---------------------------------------------------------------------------
# Main: extract ≥10-acre parcels
# ---------------------------------------------------------------------------

ACRE_MIN = 10.0


def extract_large_parcels(zip_path: Path) -> pd.DataFrame:
    info = read_info_df(zip_path)
    print(f"▶ Loaded {len(info):,} property headers")

    large_rows = []

    with zipfile.ZipFile(zip_path) as zf:
        land_name = next(m for m in zf.namelist() if "LAND_DETAIL" in m.upper())
        with zf.open(land_name) as fh:
            for raw in tqdm(fh, desc="Scanning LAND_DETAIL", unit="row"):
                line = raw.decode("ascii", errors="ignore")
                if len(line) < 76:
                    continue
                size_ind  = line[63]
                size_amt  = line[64:76]
                if size_ind != "F":
                    continue  # skip sqft or other units
                try:
                    acres = int(size_amt) / 100  # implied 2 decimals
                except ValueError:
                    continue
                if acres < ACRE_MIN:
                    continue
                acct = line[0:12]
                large_rows.append((acct.strip(), acres))

    print(f"▶ Found {len(large_rows):,} parcels ≥ {ACRE_MIN} acres")

    # Build DataFrame and join to owner info
    df_land = pd.DataFrame(large_rows, columns=["acct","acres"])
    merged  = df_land.merge(info, on="acct", how="left")

    # tidy columns
    out = merged[[
        "owner1","owner2","mail1","mail2","zip","acres","acct"
    ]].rename(columns={
        "owner1":"OwnerName",
        "owner2":"CoOwner",
        "mail1":"MailAddr1",
        "mail2":"MailAddr2",
        "zip":"Zip",
        "acres":"Acreage",
        "acct":"Account"
    })

    return out

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python taylor_parcel_parser.py <roll.zip>")

    zip_file = Path(sys.argv[1]).expanduser().resolve()
    if not zip_file.exists():
        sys.exit(f"Zip not found: {zip_file}")

    df = extract_large_parcels(zip_file)
    out_csv = zip_file.with_name("taylor_10plus_acres.csv")
    df.to_csv(out_csv, index=False)
    print(f"✅ Saved {len(df):,} rows to {out_csv}")
