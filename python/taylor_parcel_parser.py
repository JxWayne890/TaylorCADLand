import sys, zipfile, re
from pathlib import Path
import pandas as pd
from tqdm import tqdm

SIZE_RE   = re.compile(r"([A-Z])([0-9]{12})")   # F000000123456
ACRE_MIN  = 10.0
ALPHA_RE  = re.compile(r"[A-Z].*?[A-Z]")        # first long alpha run

def extract_large_parcels(zf: zipfile.ZipFile):
    land = next(n for n in zf.namelist() if "LAND_DETAIL" in n.upper())
    info = next(n for n in zf.namelist() if "APPRAISAL_INFO" in n.upper())

    # Build in-memory dict {acct: raw_info_line}
    info_dict = {}
    with zf.open(info) as fh:
        for line in fh:
            line = line.decode("ascii", "ignore")
            info_dict[line[:12]] = line.rstrip("\n")

    rows = []
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
            info_line = info_dict.get(acct, "")
            # Owner block: first alpha run  → owner1
            m_owner = ALPHA_RE.search(info_line[12:])
            owner1 = m_owner.group(0).strip() if m_owner else ""
            # Next alpha run → mailing line
            rest  = info_line[m_owner.end()+12:] if m_owner else ""
            m_mail1 = ALPHA_RE.search(rest)
            mail1 = m_mail1.group(0).strip() if m_mail1 else ""
            # ZIP: last 5 digits in line
            zip5 = info_line[-5:] if info_line[-5:].isdigit() else ""

            rows.append((owner1, mail1, zip5, acres, acct))

    return pd.DataFrame(rows,
        columns=["OwnerName","MailAddr","Zip","Acreage","Account"])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python taylor_parcel_parser.py roll.zip")
    z = Path(sys.argv[1]).resolve()
    with zipfile.ZipFile(z) as archive:
        df = extract_large_parcels(archive)
    out = z.with_name("taylor_10plus_acres.csv")
    df.to_csv(out, index=False)
    print(f"✅ Saved {len(df):,} rows → {out}")
