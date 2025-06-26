"""
Taylor CAD ≥10-acre parcel extractor (regex version)
────────────────────────────────────────────────────
Why we changed it again
----------------------
The LAND-DETAIL file *does* follow the PTAD layout, but the exact positions
shifted because the county’s Land-Description field isn’t always 25 chars.
Rather than hard-code offsets yet again, this version extracts acreage with
an easy, bullet-proof regex:

    size indicator  = one upper-case letter (F, S, A …)
    size amount     = 12 consecutive digits right after the indicator

Regex:  r"([A-Z])([0-9]{12})"

If the indicator == "F" (acres) we convert `amount / 100` and keep the row
when acres ≥ 10.

This makes the parser robust even if the county tweaks column widths next
roll.
"""
import sys, zipfile, re
from pathlib import Path
import pandas as pd
from tqdm import tqdm

WIDTHS_INFO = [12,1,4,35,35,35,35,9]
COLS_INFO   = ["acct","pt","yr","owner1","owner2","mail1","mail2","zip"]
SIZE_REGEX  = re.compile(r"([A-Z])([0-9]{12})")  # indicator + 12-digit size amt

ACRE_MIN = 10.0

# ---------------------------------------------------------------------------

def read_info_df(zip_path: Path):
    with zipfile.ZipFile(zip_path) as zf:
        member = next(m for m in zf.namelist() if "APPRAISAL_INFO" in m.upper())
        with zf.open(member) as fh:
            return pd.read_fwf(fh, widths=WIDTHS_INFO, names=COLS_INFO, dtype=str)

# ---------------------------------------------------------------------------

def extract_large_parcels(zip_path: Path) -> pd.DataFrame:
    info = read_info_df(zip_path)
    rows = []

    with zipfile.ZipFile(zip_path) as zf:
        member = next(m for m in zf.namelist() if "LAND_DETAIL" in m.upper())
        with zf.open(member) as fh:
            for raw in tqdm(fh, desc="LAND_DETAIL", unit="row"):
                line = raw.decode("ascii", errors="ignore")
                m = SIZE_REGEX.search(line)
                if not m:
                    continue
                ind, amt_str = m.groups()
                if ind != "F":
                    continue  # we only want acreage rows
                acres = int(amt_str) / 100  # implied 2 decimals
                if acres < ACRE_MIN:
                    continue
                acct = line[0:12].strip()
                rows.append((acct, acres))

    df_land = pd.DataFrame(rows, columns=["acct","acres"])
    merged  = df_land.merge(info, on="acct", how="left")
    return merged[["owner1","owner2","mail1","mail2","zip","acres","acct"]] \
                  .rename(columns={
                      "owner1":"OwnerName",
                      "owner2":"CoOwner",
                      "mail1":"MailAddr1",
                      "mail2":"MailAddr2",
                      "zip":"Zip",
                      "acres":"Acreage",
                      "acct":"Account"
                  })

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python taylor_parcel_parser.py <roll.zip>")
    z = Path(sys.argv[1]).resolve()
    df = extract_large_parcels(z)
    out = z.with_name("taylor_10plus_acres.csv")
    df.to_csv(out, index=False)
    print(f"Saved {len(df):,} rows → {out}")
