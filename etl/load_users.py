import pandas as pd
import psycopg2
import psycopg2.extras
import os, re
from pathlib import Path
from dotenv import load_dotenv
from mappings import COLUMN_ALIASES

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

DEFAULT_USER_TYPE = "0"
DEFAULT_CATEGORY_NAME = "Finance"
CHUNK_SIZE = 50_000

DB_COLS = [
    "email","name","age","city",
    "phone","countryCode",
    "bio","referralCode",
    "LeadSource","type",
    "categoryName","salary"
]

PHONE_RE = re.compile(r"\D+")

def norm(x):
    return "" if x is None else str(x).lower().strip()

def detect_columns(df):
    cols = {norm(c): c for c in df.columns}
    out = {}
    for k, aliases in COLUMN_ALIASES.items():
        for a in aliases:
            if norm(a) in cols:
                out[k] = cols[norm(a)]
                break
    return out

def normalize_salary(v):
    if v is None:
        return None
    t = str(v).lower().replace(",", "")
    try:
        return float(t)
    except:
        pass
    if "lac" in t or "lakh" in t:
        return float(re.findall(r"[\d.]+", t)[0]) * 100_000
    if "cr" in t:
        return float(re.findall(r"[\d.]+", t)[0]) * 10_000_000
    return None

def parse_phone_series(s):
    s = s.astype(str).str.replace(PHONE_RE, "", regex=True)
    phone = pd.Series(index=s.index, dtype=object)
    cc = pd.Series(index=s.index, dtype=object)

    m = s.str.len() == 10
    phone[m], cc[m] = s[m], "+91"

    m = s.str.startswith("91") & (s.str.len() == 12)
    phone[m], cc[m] = s[m].str[-10:], "+91"

    return phone, cc

def clean_df(df):
    col = detect_columns(df)
    if "phone" not in col:
        return pd.DataFrame(columns=DB_COLS)

    out = pd.DataFrame()
    for k in ["email","name","age","city","bio","referralCode"]:
        out[k] = df[col[k]] if k in col else None

    out["salary"] = df[col["salary"]].apply(normalize_salary) if "salary" in col else None

    phone, cc = parse_phone_series(df[col["phone"]])
    out["phone"] = phone
    out["countryCode"] = cc

    out = out.dropna(subset=["phone","countryCode"]).drop_duplicates()

    out["type"] = DEFAULT_USER_TYPE
    out["categoryName"] = DEFAULT_CATEGORY_NAME
    out["LeadSource"] = None

    return out[DB_COLS]

def read_csv(p):
    return pd.read_csv(p, chunksize=CHUNK_SIZE, encoding="latin1", on_bad_lines="skip")

def insert_batch(conn, df):
    if df.empty:
        return 0

    rows = [tuple(df[c] for c in DB_COLS) for _, df in df.iterrows()]

    sql = """
        INSERT INTO public.users (
            email,name,age,city,phone,"countryCode",
            bio,"referralCode","LeadSource",
            type,"categoryName",salary
        )
        VALUES %s
        ON CONFLICT (phone,"countryCode") DO NOTHING
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            sql,
            rows,
            page_size=10_000
        )
        return cur.rowcount

def process_file(path, base):
    lead = base.name.replace(" ","_")
    print(f"\n📂 {path}")
    print(f"🏷️ LeadSource = {lead}")

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_schema(), current_database()")
            print("🧠 Schema/DB:", cur.fetchone())

        for df in read_csv(path):
            print("📊 Raw rows:", len(df))
            clean = clean_df(df)
            print("🧹 Clean rows:", len(clean))
            if clean.empty:
                continue

            clean["LeadSource"] = lead
            inserted = insert_batch(conn, clean)
            conn.commit()

            print(f"➕ Inserted rows: {inserted}")

def process_data_folder(folder):
    base = Path(folder).resolve()
    for f in base.rglob("*.csv"):
        process_file(f, base)

if __name__ == "__main__":
    process_data_folder("/Users/sarthakchandra/Desktop/Tradewise/DataBase/data")