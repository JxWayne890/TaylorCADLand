"""
Taylor CAD ≥10-acre parcel extractor
-----------------------------------
Reads the 2025 preliminary appraisal-roll ZIP, filters every land
segment with Legal_Acreage ≥ 10, joins owner info, and writes a CSV.

Usage (local):
    python python/taylor_parcel_parser.py TaylorCAD_2025_Preliminary_Appr_Roll_All_Prop_Types_02Jun25.zip
"""

import sys, zipfile
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# ---- column layouts (adjust if CAD changes spec) ---------------------------
WIDTHS_LAND = [12,4,6,4,2,1,9,15,15]      # acct, year, seq, class, prod, acres, mv, agv
COLS_LAND   = ["acct","yr","seq","class","prod","acres","mv","agv","dummy"]

WIDTHS_INFO = [12,1,4,35,35,35,35,9]       # acct, pt, year, owner1-4, zip
COLS_INFO   = ["acct","pt","yr","own1","own2","mail1","mail2","zip"]

DECIMALS = 4                               # acres stored as #####.####

def read_from_zip(zip_path:Path, member_key:str, widths, cols):
    with zipfile.ZipFile(zip_path) as z:
        name = next(m for m in z.namelist() if member_key in m.upper())
        return pd.read_fwf(z.open(name), widths=widths, names=cols, dtype=str)

def implied_dec_to_float(series:pd.Series, dec:int):
    factor = 10**dec
    return pd.to_numeric(series, errors="coerce")/factor

def main(zip_file:str, min_acres:float=10.0):
    z = Path(zip_file)
    if not z.exists():
        sys.exit(f"Zip not found: {z}")

    print("▶ reading LAND_DETAIL …")
    land = read_from_zip(z,"LAND_DETAIL",WIDTHS_LAND,COLS_LAND)
    land["acres"] = implied_dec_to_float(land["acres"],DECIMALS)
    land_big = land[land["acres"]>=min_acres]

    print(f"   kept {len(land_big):,} segments ≥{min_acres} acres")

    print("▶ reading APPRAISAL_INFO …")
    info = read_from_zip(z,"APPRAISAL_INFO",WIDTHS_INFO,COLS_INFO)

    print("▶ merging …")
    merged = land_big.merge(info,on=["acct","yr"],how="left")
    merged.sort_values(["acct","acres"],ascending=[True,False],inplace=True)
    merged = merged.drop_duplicates("acct",keep="first")

    out = merged[["own1","own2","mail1","mail2","zip","acres","acct"]]
    out = out.rename(columns={"own1":"Owner","own2":"CoOwner",
                              "mail1":"MailAddr1","mail2":"MailAddr2",
                              "zip":"Zip","acres":"Acreage","acct":"Account"})

    out_file = z.with_name("taylor_10plus_acres.csv")
    out.to_csv(out_file,index=False)
    print(f"✅ wrote {len(out):,} rows to {out_file}")

if __name__=="__main__":
    if len(sys.argv)<2:
        sys.exit("Usage: python taylor_parcel_parser.py <roll.zip>")
    main(sys.argv[1])
