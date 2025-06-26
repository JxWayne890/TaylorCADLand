"""
Taylor CAD ≥10-Acre Extractor (dynamic owner offsets)
====================================================

• Acreage:   regex  ([A-Z])([0-9]{12})   → F000000123456 → 1 234.56 ac
• Owners:    find first alpha after column 12, then
             35-35-35-35-9  (owner1, owner2, mail1, mail2, zip)

Outputs taylor_10plus_acres.csv
"""

import sys, zipfile, re
from pathlib import Path
import pandas as pd
from tqdm import tqdm

SIZE_RE   = re.compile(r"([A-Z])([0-9]{12})")  # indicator + 12-digit size
ACRE_MIN  = 10.0

# ── helper: read APPRAISAL_INFO into dict {acct: raw_line} ───────────────
def build_info_dict(zpath: Path) -> dict[str, str]:
    with zipfile.ZipFile(zpath) as zf:
        name = next(m for m in zf.namelist() if "APPRAISAL_INFO" in m.upper())
        with zf.open(name) as fh:
            return {line[:12].decode(): line.decode("ascii", "ignore") for line in fh}

# ── owner-block slicer (dynamic) ─────────────────────────────────────────
def parse_owner_block(raw: str) -> tuple[str, str, str, str, str]:
    """Return Owner1, Owner2, Mail1, Mail2, Zip from a full INFO line."""
    i = 12
    while i < len(raw) and not raw[i].isalpha():
        i += 1
    owner1 = raw[i:i+35].strip()
    owner2 = raw[i+35:i+70].strip()
    mail1  = raw[i+70:i+105].strip()
    mail2  = raw[i+105:i+140].strip()
    zip9   = raw[i+140:i+149].strip()
    return owner1, owner2, mail1, mail2, zip9

# ── main extractor ───────────────────────────────────────────────────────
def extract_large_parcels(zpath: Path) -> pd.DataFrame:
    info_dict = build_info_dict(zpath)

    rows = []  # acct, acres, owner1, owner2, mail1, mail2, zip
    with zipfile.ZipFile(zpath) as zf:
        land = next(m for m in zf.namelist() if "LAND_DETAIL" in m.upper())
        with zf.open(land) as fh:
            for raw in tqdm(fh, desc="LAND_DETAIL", unit="row"):
                line = raw.decode("ascii", "ignore")
                m = SIZE_RE.search(line)
                if not m or m.group(1) != "F":
                    continue
                acres = int(m.group(2)) / 100
                if acres < ACRE_MIN:
                    continue
                acct = line[:12]
                info_raw = info_dict.get(acct)
                if not info_raw:
                    continue
                owner1, owner2, mail1, mail2, zip9 = parse_owner_block(info_raw)
                rows.append((owner1, owner2, mail1, mail2, zip9, acres, acct))

    df = pd.DataFrame(rows, columns=[
        "OwnerName", "CoOwner", "MailAddr1", "MailAddr2",
        "Zip", "Acreage", "Account"
    ])
    return df

# ── CLI ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python taylor_parcel_parser.py <roll.zip>")
    z = Path(sys.argv[1]).expanduser().resolve()
    out = z.with_name("taylor_10plus_acres.csv")
    extract_large_parcels(z).to_csv(out, index=False)
    print(f"✅ Saved {out}")
