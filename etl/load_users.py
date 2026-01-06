import pandas as pd
import psycopg
import os
import re
import io
from pathlib import Path
from dotenv import load_dotenv
from mappings import COLUMN_ALIASES

# =====================
# CONFIG (YOU CONTROL)
# =====================

DEFAULT_COUNTRY_CODE = "+91"
DEFAULT_LEAD_SOURCE = "Manual Excel Import"
DEFAULT_USER_TYPE = "1"
DEFAULT_CATEGORY_NAME = "Finance"

CHUNK_SIZE = 50_000

DB_INSERT_COLUMNS = [
    "email",
    "name",
    "age",
    "city",
    "phone",
    "countryCode",
    "bio",
    "referralCode",
    "LeadSource",
    "type",
    "categoryName",
]

# =====================
# LOAD ENV
# =====================

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# =====================
# HELPERS
# =====================

def normalize(text: str) -> str:
    return text.lower().strip()


def detect_columns(df: pd.DataFrame) -> dict:
    detected = {}
    normalized_cols = {normalize(c): c for c in df.columns}

    for db_col, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            key = normalize(alias)
            if key in normalized_cols:
                detected[db_col] = normalized_cols[key]
                break

    return detected


def parse_phone(raw_phone: str):
    if not raw_phone or not isinstance(raw_phone, str):
        return None, None

    cleaned = re.sub(r"[^\d+]", "", raw_phone.strip())

    if cleaned.startswith("+91") and len(cleaned) == 13:
        return cleaned[3:], "+91"

    if cleaned.startswith("91") and len(cleaned) == 12:
        return cleaned[2:], "+91"

    if cleaned.startswith("0") and len(cleaned) == 11:
        return cleaned[-10:], "+91"

    digits = re.sub(r"\D", "", cleaned)
    if len(digits) == 10:
        return digits, "+91"

    return None, None


# =====================
# CLEAN + TRANSFORM
# =====================

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    col_map = detect_columns(df)
    clean = pd.DataFrame()

    for db_col, excel_col in col_map.items():
        clean[db_col] = df[excel_col]

    phones = clean.get("phone", pd.Series(dtype=str)).astype(str)
    parsed = phones.apply(parse_phone)

    clean["phone"] = parsed.apply(lambda x: x[0])
    clean["countryCode"] = parsed.apply(lambda x: x[1])

    clean = clean.dropna(subset=["phone", "countryCode"])

    if "email" in clean:
        clean["email"] = clean["email"].astype(str).str.lower().str.strip()

    clean["LeadSource"] = DEFAULT_LEAD_SOURCE
    clean["type"] = DEFAULT_USER_TYPE
    clean["categoryName"] = DEFAULT_CATEGORY_NAME

    for col in DB_INSERT_COLUMNS:
        if col not in clean.columns:
            clean[col] = None

    return clean[DB_INSERT_COLUMNS]


# =====================
# COPY INTO STAGING
# =====================

def copy_to_staging(conn, df: pd.DataFrame):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=True)
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy("""
            COPY users_staging (
                email,
                name,
                age,
                city,
                phone,
                "countryCode",
                bio,
                "referralCode",
                "LeadSource",
                type,
                "categoryName"
            )
            FROM STDIN WITH CSV HEADER
        """, buffer)
    conn.commit()


def merge_staging(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (
                email,
                name,
                age,
                city,
                phone,
                "countryCode",
                bio,
                "referralCode",
                "LeadSource",
                type,
                "categoryName"
            )
            SELECT
                email,
                name,
                age,
                city,
                phone,
                "countryCode",
                bio,
                "referralCode",
                "LeadSource",
                type,
                "categoryName"
            FROM users_staging
            ON CONFLICT (phone, "countryCode") DO NOTHING;
        """)
        cur.execute("TRUNCATE TABLE users_staging;")
    conn.commit()


# =====================
# FILE PROCESSING
# =====================

def process_csv(path: str):
    print(f"\n📂 Processing {path}")

    with psycopg.connect(**DB_CONFIG) as conn:
        for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE):
            clean = clean_df(chunk)
            if clean.empty:
                continue

            copy_to_staging(conn, clean)
            merge_staging(conn)

            print(f"✅ Inserted batch of {len(clean)} rows")


def process_data_folder(folder: str):
    for file in Path(folder).iterdir():
        if file.suffix.lower() == ".csv":
            process_csv(str(file))


# =====================
# ENTRY POINT
# =====================

if __name__ == "__main__":
    process_data_folder("data")