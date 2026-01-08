import pandas as pd
import hashlib
import datetime


#===============================
# MAP NAMA BULAN (WAJIB TARUH DI SINI)
# ==============================
MONTH_NAME_ID = {
    1: "Januari",
    2: "Februari",
    3: "Maret",
    4: "April",
    5: "Mei",
    6: "Juni",
    7: "Juli",
    8: "Agustus",
    9: "September",
    10: "Oktober",
    11: "November",
    12: "Desember",
}

# ==============================
# DATE CLEANER (WAJIB ADA)
# ==============================
ID_MONTH_MAP = {
    "januari": "January",
    "februari": "February",
    "maret": "March",
    "april": "April",
    "mei": "May",
    "juni": "June",
    "juli": "July",
    "agustus": "August",
    "september": "September",
    "oktober": "October",
    "november": "November",
    "desember": "December",
}

def clean_date(value):
    if pd.isna(value):
        return None

    s = str(value).strip()
    if s == "":
        return None

    s = s.lower()
    for id_m, en_m in ID_MONTH_MAP.items():
        s = s.replace(id_m, en_m)

    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    return dt.date() if pd.notna(dt) else None



# ==============================
# LEAD TYPE MAPPER
# ==============================
def normalize_text(v):
    if pd.isna(v):
        return ""
    return str(v).strip().upper()

LEAD_TYPE_MARKETING_MAP = {
    "CTWA-EVERPRO": "CTWA",
    "FORM-EVERPRO": "LP FORM",
    "FORM BIASA": "LP FORM",
    "GASS": "GASS",
    "LPWA-EVERPRO": "LP FORM",
    "FORM-GOOGLEADS": "GOOGLE",
    "FORM-NATIVEADS": "NATIVE",
    "FORM-SNACKVIDEO": "SNACK VIDEO",
    "FANSPAGE FACEBOOK": "LP FORM",
}

LEAD_TYPE_PRODUCT_MAP = {
    "FORM BIASA": "LP FORM",
    "EVERPRO": "LP EVERPRO",
    "EVERPRO - LPWA": "LP EVERPRO",
    "LOOPS FORM": "LP FORM",
    "NON LEAD": "INDIRECT LEAD",
    "FANSPAGE FACEBOOK": "INDIRECT LEAD",
    "ADCUAN": "LP ADCUAN",
}

# ==============================
# CANONICAL STRUCTURE
# ==============================
Q4_CANONICAL_COLS = [
    "No", "Tanggal Masuk", "Tanggal Input", "Nama", "Nomor",
    "Link Whatsapp", "Nomor Asli", "Jenis Lead", "Produk",
    "Landing Page", "Advertiser", "CSO", "NAMA KONTEN",
    "Column 26", "BULAN LEAD", "VALUE", "BOTOL",
    "KECAMATAN", "KABUPATEN", "PROVINSI",
    "Jenis Konten", "Isu Campaign", "Bulan Konten",
    "RESPON", "KETERANGAN", "UNEK UNEK",
    "Key WORD Funnel", "Keyword Nama Customer",
    "unit", "team","Jenis Lead","Jenis Lead_Origin"
]

# ==============================
# LEAD CATEGORY 
# ==============================

df["lead_category"] = None
df.loc[
    source_code == "Q4",
    "lead_category"
] = df["Jenis Lead"]

# ETA → dari Jenis Lead Origin
df.loc[
    source_code == "ETA",
    "lead_category"
] = df["Jenis Lead Origin"]

# fallback
df["lead_category"] = (
    df["lead_category"]
    .fillna("UNCLASSIFIED")
    .astype(str)
    .str.strip()
)
 

# ==============================
# ROW HASH
# ==============================
def generate_row_hash(row):
    values = []
    for col in Q4_CANONICAL_COLS + ["lead_type"]:
        v = row.get(col)
        if isinstance(v, (pd.Timestamp, datetime.date, datetime.datetime)):
            v = pd.to_datetime(v).strftime("%Y-%m-%d")
        elif pd.isna(v):
            v = ""
        else:
            v = str(v).strip().lower()
        values.append(v)
    return hashlib.sha256("|".join(values).encode()).hexdigest()

ETA_COLUMN_MAPPING = {
    "Nomor Input": "Nomor",
    "Nama Customer": "Nama",
    "Whatsapp": "Link Whatsapp"
}

# ==============================
# TRANSFORM CORE
# ==============================
def transform(df_raw, mapping, source_code):
    df = df_raw.rename(columns=mapping) if mapping else df_raw.copy()
    df.columns = df.columns.astype(str).str.strip()

    for col in Q4_CANONICAL_COLS:
        if col not in df.columns:
            df[col] = None

    df = df[Q4_CANONICAL_COLS].copy()
    
# ==============================
# NORMALISASI NOMOR 
# ==============================

    df["Nomor"] = (
        df["Nomor"]
        .astype(str)
        .str.replace(r"\D+", "", regex=True)
        .replace("nan", None)
        .replace("", None)  
    )
    
    
    # ==============================
    # DROP ROW KOSONG / SAMPAH 
    # ==============================
    df = df[
    (df["No"].notna()) &
    (df["No"].astype(str).str.strip() != "") &
    (
        (df["Nama"].notna() & (df["Nama"].astype(str).str.strip() != "")) |
        (df["Nomor"].notna() & (df["Nomor"].astype(str).str.strip() != ""))
    )
    ].copy()
    
    
    # ==============================
    # UNIT
    # ==============================
    if source_code == "Q4":
        df["unit"] = "SALES"
    elif source_code == "ETA":
        df["unit"] = "RESEARCH/DEVELOPMENT"
    else:
        df["unit"] = "UNCLASSIFIED"

    # ==============================
    # LEAD TYPE
    # ==============================
    funnel = df["Key WORD Funnel"].fillna("").apply(normalize_text)
    produk = df["Produk"].fillna("").apply(normalize_text)
    jenis_lead = df["Jenis Lead"].fillna("").apply(normalize_text)

    df["lead_type"] = funnel.map(LEAD_TYPE_MARKETING_MAP)

    df["lead_type"] = df["lead_type"].fillna(
    produk.map(LEAD_TYPE_PRODUCT_MAP)
)
    df["lead_type"] = df["lead_type"].fillna(jenis_lead)

    df["lead_type"] = df["lead_type"].replace("", "UNCLASSIFIED")
    df["lead_type"] = df["lead_type"].fillna("UNCLASSIFIED")
    
    # ==============================
    # NORMALISASI LEAD TYPE
    # ==============================
    df["lead_type"] = df["lead_type"].replace({
    "FORM- EVERPRO": "LP EVERPRO",
    "FORM EVERPRO": "LP EVERPRO",
    "LP FORM EVERPRO": "LP EVERPRO",
})
    
    # ==============================
    # TEAM
    # ==============================
    
    df["team"] = "UNCLASSIFIED"
    df.loc[
    (df["unit"] == "SALES") & 
    (df["lead_type"] == "CTWA"),
    "team"
] = "AKUISISI CTWA"
    
    df.loc[
    (df["unit"] == "SALES") &
    (df["lead_type"].isin([
        "LP FORM",
        "LP EVERPRO",
        "LPWA-EVERPRO",
        "GASS",
        "GOOGLE",
        "NATIVE",
        "SNACK VIDEO"
    ])),
    "team"
] = "AKUISISI"
    
    df.loc[
    (df["unit"] == "RESEARCH/DEVELOPMENT") &
    (df["lead_type"].isin(["LP FORM", "LP EVERPRO", "LP ADCUAN"])),
    "team"
] = "AKUISISI PRODUK"
    
    df.loc[
    (df["unit"] == "RESEARCH/DEVELOPMENT") &
    (df["lead_type"] == "INDIRECT LEAD"),
    "team"
] = "AKUISISI NON LEAD PRODUK"
    
    df["lead_id"] = source_code + "_" + df["No"].astype(str)
      
    # ==============================
    # DATE CLEANING
    # ==============================
    df["Tanggal Masuk"] = df["Tanggal Masuk"].apply(clean_date)
    df["Tanggal Input"] = df["Tanggal Input"].apply(clean_date)
    
    # ==============================
    # TIME DERIVED COLUMN (WAJIB)
    # ==============================
    df["entry_date"] = df["Tanggal Masuk"]
    df["input_date"] = df["Tanggal Input"]
    
    df["entry_year"] = pd.to_datetime(
        df["entry_date"], errors="coerce"       
    ).dt.year

    df["_entry_month_num"] = pd.to_datetime(
        df["entry_date"], errors="coerce"
    ).dt.month
    
    df["entry_month"] = df["_entry_month_num"].map(MONTH_NAME_ID)
    df.drop(columns=["_entry_month_num"], inplace=True)


    # ==============================
    # ROW HASH
    # ==============================
    df["row_hash"] = df.apply(generate_row_hash, axis=1)

    return df.reset_index(drop=True)

    # ==============================
    # FINAL DATASET
    # ==============================
def get_final_df():
    from step3_read_gsheet import extract_all

    df_q4, df_eta = extract_all()

    df_q4_t = transform(df_q4, None, "Q4")
    df_eta_t = transform(df_eta, ETA_COLUMN_MAPPING, "ETA")

    # buang No kosong
    for d in (df_q4_t, df_eta_t):
        d.dropna(subset=["No"], inplace=True)
        d.drop(d[d["No"].astype(str).str.strip() == ""].index, inplace=True)

    # samakan struktur
    df_eta_t = df_eta_t[df_q4_t.columns]

    final_df = pd.concat([df_q4_t, df_eta_t], ignore_index=True)
    final_df.drop_duplicates(subset=["lead_id"], inplace=True)

    
    return final_df

if __name__ == "__main__":
    from step3_read_gsheet import extract_all

    df_q4, df_eta = extract_all()

    df_q4_t = transform(df_q4, None, "Q4")
    df_eta_t = transform(df_eta, ETA_COLUMN_MAPPING, "ETA")

    print("=== Q4 SAMPLE ===")
    print(df_q4_t[["No", "Nama", "Nomor", "Link Whatsapp","entry_date","entry_year","entry_month"]].head(10))

    print("\n=== ETA SAMPLE ===")
    print(df_eta_t[["No", "Nama", "Nomor", "Link Whatsapp","entry_date","entry_year","entry_month"]].head(10))

    print("\nTOTAL Q4:", len(df_q4_t))
    print("TOTAL ETA:", len(df_eta_t))

    
    
    







    