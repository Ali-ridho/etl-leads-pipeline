import pandas as pd
import hashlib
import warnings
import re 
import sys
from datetime import datetime

# Matikan warning
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
pd.options.mode.chained_assignment = None 

# ==============================
# 1. KONFIGURASI & MAPPING
# ==============================

# Kolom System (Tidak boleh diutak-atik user & tidak masuk perhitungan Hash)
SYSTEM_COLUMNS = ["row_hash", "created_at", "updated_at", "sync_status"]

# Mapping Kolom V2 (Pembersihan nama kolom standar)
V2_COLUMN_MAP = {
    "no": "row",
    "nomor": "row",
    "whatsapp_link": "link_whatsapp", 
    "link_whatsapp": "link_whatsapp", 
    "whatsapp": "link_whatsapp",
    "lead_category": "lead_category",
    "nama": "lead_name",
    "jenis_lead": "lead_type",
    "sumber_lead": "lead_source",
    "produk": "product_name",
    "category": "lead_category",
    "consumption_category": "lead_category" 
}

# --- KONFIGURASI V1 (TETAP) ---
ID_MONTH_MAP = {
    "januari": "January", "februari": "February", "maret": "March",
    "april": "April", "mei": "May", "juni": "June",
    "juli": "July", "agustus": "August", "september": "September",
    "oktober": "October", "november": "November", "desember": "December",
    "agt": "August", "sep": "September", "okt": "October", "nov": "November", "des": "December"
}

ETA_MAP = {
    "No": "row_no", "Tanggal Masuk": "entry_date", "Tanggal Input": "input_date",
    "Nama": "lead_name", "Nomor Input": "phone_number", 
    "Link Whatsapp": "whatsapp_link",
    "Sumber Lead": "lead_source", "Jenis Lead": "lead_type",
    "Produk": "product_name", "Advertiser": "advertiser",
    "CSO": "customer_service", "Landing Page": "landing_page"
}

Q4_MAP = {
    "No": "row_no", "Tanggal Masuk": "entry_date", "Tanggal Input": "input_date",
    "Nama": "lead_name", "Nomor": "phone_number",
    "Link Whatsapp": "whatsapp_link", 
    "Jenis Lead": "lead_type", "Produk": "product_name",
    "Advertiser": "advertiser", "CSO": "customer_service",
    "Landing Page": "landing_page", "NAMA KONTEN": "content_name"
}

FINAL_DB_V1_COLUMNS = [
    "lead_id", "row_no", "entry_year", "entry_month", "entry_date", "input_date",
    "unit", "team", "lead_name", "phone_number", "whatsapp_link",
    "lead_category", "lead_source", "lead_type", "is_valid",
    "product_name", "advertiser", "customer_service", "landing_page",
    "content_name", "row_hash", "is_deleted"
]

# ==============================
# 2. HELPER FUNCTIONS
# ==============================
def clean_date_indo(value):
    if pd.isna(value): return None
    s = str(value).strip().lower()
    if s == "" or s == "nan" or s == "0": return None
    if isinstance(value, (pd.Timestamp, float, int)):
        try: return pd.to_datetime(value).date()
        except: pass
    for id_m, en_m in ID_MONTH_MAP.items():
        if id_m in s: s = s.replace(id_m, en_m)
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors='coerce')
        if pd.notna(dt): return dt.date()
    except: return None
    return None

def clean_phone_number(phone):
    if pd.isna(phone) or str(phone).strip() == "": return None
    phone = str(phone).strip()
    phone = re.sub(r'[^0-9*]', '', phone) 
    if len(phone) < 5: return None 
    if phone.startswith("6208"): phone = re.sub(r'^6208', '628', phone)
    elif phone.startswith("62008"): phone = re.sub(r'^62008', '628', phone)
    elif phone.startswith("625"): phone = re.sub(r'^625', '6285', phone)
    if phone.startswith("0"):
        if phone.startswith("08"): phone = "628" + phone[2:] 
        else: phone = "628" + phone[1:]
    elif phone.startswith("62") and not phone.startswith("628"):
        phone = "628" + phone[2:]
    elif not phone.startswith("62"):
        if phone.startswith("8"): phone = "62" + phone
        else: phone = "628" + phone
    return phone    

def clean_garbage_name(val):
    s = str(val).strip()
    if s == "" or s.lower() == "nan" or s.lower() == "none": return None
    if re.match(r'^\d+(\.\d+)?$', s): return None 
    return s

def generate_row_hash(row):
    # Hash V1
    raw_str = f"{row.get('lead_id','')}{row.get('phone_number','')}{row.get('lead_name','')}"
    return hashlib.md5(raw_str.encode()).hexdigest()

def generate_dynamic_row_hash(row, cols_to_hash):
    # Hash V2 (Dynamic)
    # Pastikan value none/nan jadi string kosong agar konsisten
    raw_str = "".join([str(row.get(col, '')).strip() for col in cols_to_hash])
    return hashlib.md5(raw_str.encode()).hexdigest()

# ==============================
# 3. TRANSFORM FUNCTION V1
# ==============================
def transform_v1(df, source_type):
    print(f"   ... Memproses {source_type} & Filter Baris Kosong...")
    df.columns = df.columns.astype(str).str.strip()
    df = df.loc[:, ~df.columns.duplicated()] 
    mapping = ETA_MAP if source_type == "ETA" else Q4_MAP
    unit_name = "RESEARCH/DEVELOPMENT" if source_type == "ETA" else "SALES"
    team_name = "AKUISISI"
    df_clean = df.rename(columns=mapping)
    if "row_no" in df_clean.columns:
        df_clean["row_no"] = pd.to_numeric(df_clean["row_no"], errors='coerce').fillna(0).astype(int)
        df_clean = df_clean[df_clean["row_no"] > 0].copy()
        is_valid_row = pd.Series(False, index=df_clean.index)
        if "lead_name" in df_clean.columns: is_valid_row |= df_clean["lead_name"].notna()
        if "entry_date" in df_clean.columns: is_valid_row |= df_clean["entry_date"].notna()
        df_clean = df_clean[is_valid_row].copy()
        df_clean["lead_id"] = source_type + "_" + df_clean["row_no"].astype(str)
    else:
        return pd.DataFrame()
    df_clean["unit"] = unit_name
    df_clean["team"] = team_name
    df_clean["is_deleted"] = 0
    if "entry_date" in df_clean.columns: df_clean["entry_date"] = df_clean["entry_date"].apply(clean_date_indo)
    if "input_date" in df_clean.columns: df_clean["input_date"] = df_clean["input_date"].apply(clean_date_indo)
    if "entry_date" in df_clean.columns:
        temp_date = pd.to_datetime(df_clean["entry_date"], errors='coerce')
        df_clean["entry_year"] = temp_date.dt.year.astype('Int64') 
        df_clean["entry_month"] = temp_date.dt.month_name()
    df_clean["row_hash"] = df_clean.apply(generate_row_hash, axis=1)
    available_cols = [c for c in FINAL_DB_V1_COLUMNS if c in df_clean.columns]
    return df_clean[available_cols]

# ==============================
# 4. TRANSFORM FUNCTION V2 (MODIFIED & STABILIZED)
# ==============================
def transform_v2_direct(df, source_label):
    print(f"   ... Memproses {source_label} (Awal: {len(df)} baris)...")
    
    # [POINT 4] WARNING SYSTEM (SEBELUM PERBAIKAN)
    # Memberi peringatan jika user memakai spasi/huruf besar
    raw_columns = df.columns.astype(str).tolist()
    for col in raw_columns:
        if " " in col or any(x.isupper() for x in col):
            print(f"   ⚠️ [PERINGATAN] Format kolom salah: '{col}'")
            print(f"      -> Mengandung spasi atau huruf besar.")
            print(f"      -> Sistem akan otomatis mengubahnya ke 'snake_case'.")

    # [POINT 4] OTOMASI PERBAIKAN NAMA KOLOM
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(")", "", regex=False)
        .str.replace("(", "", regex=False)
        .str.replace(" ", "_", regex=False) # Spasi jadi underscore
        .str.replace(r"[^a-z0-9_]", "", regex=True) # Hapus karakter aneh
    )

    # Cek Duplikat
    if df.columns.duplicated().any():
        dupes = df.columns[df.columns.duplicated()].tolist()
        print(f"❌ [FATAL ERROR] Ditemukan Kolom Duplikat di {source_label}: {dupes}")
        return pd.DataFrame() 

    # Koreksi Value
    df = df.replace("LP-PB Everpro", "LT Everpro", regex=False)
    df.rename(columns=V2_COLUMN_MAP, inplace=True)
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Basic Formatting
    if "row" not in df.columns: df["row"] = range(1, len(df)+ 1)
    if "part 1" in source_label.lower():
        if "lead_name" in df.columns:
            df["lead_name"] = df["lead_name"].apply(clean_garbage_name)
            df = df[df["lead_name"].notna()] 
        df = df[df["row"].astype(str).str.strip() != ""]

    df["row"] = pd.to_numeric(df["row"], errors='coerce').fillna(0).astype(int)
    df["source_id"] = source_label
    df["unique_id"] = df["source_id"] + "_" + df["row"].astype(str).str.zfill(5)

    for col in ["entry_date", "input_date"]:
        if col in df.columns: df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    if 'phone_number' in df.columns:
        df['phone_number'] = df['phone_number'].apply(clean_phone_number)
            
    # ==========================================================
    # [POINT 3 & FIX POINT 2] DYNAMIC ROW HASH YANG STABIL
    # ==========================================================
    
    # 1. Ambil SEMUA kolom data (kecuali system)
    current_df_cols = [c for c in df.columns.tolist() if 'unnamed' not in c]
    data_columns = [c for c in current_df_cols if c not in SYSTEM_COLUMNS]
    
    # 🔥 CRITICAL FIX: SORTIR KOLOM 🔥
    # Ini kuncinya! Kita urutkan kolom secara alfabet (A-Z).
    # Jadi walau di GSheet urutannya "Nama, Umur" dan besok "Umur, Nama",
    # Hash-nya akan tetap SAMA (karena diurutkan jadi "Nama, Umur" sebelum dihitung).
    data_columns.sort()
    
    # 2. Generate Hash dari data_columns yang sudah disortir
    if not df.empty:
        # Fungsi ini akan membaca seluruh kolom baru secara otomatis
        df["row_hash"] = df.apply(lambda row: generate_dynamic_row_hash(row, data_columns), axis=1)
    else:
        df["row_hash"] = None

    # Tambahkan Timestamp
    now = datetime.now()
    df["created_at"] = now
    df["updated_at"] = now
    df["sync_status"] = "synced"

    # Reordering: Data dulu, baru System
    final_column_order = data_columns + SYSTEM_COLUMNS
    for sys_col in SYSTEM_COLUMNS:
        if sys_col not in df.columns: df[sys_col] = None

    final_df = df.reindex(columns=final_column_order)
    return final_df

# ==============================
# 5. MAIN EXECUTION
# ==============================
def get_final_df(mode_selection="v2"):
    from step3_read_gsheet import extract_all
    
    df_q4, df_eta, dfs_v2_dict = extract_all(target_mode=mode_selection)
    final_dfs = []

    if mode_selection == "v2":
        if isinstance(dfs_v2_dict, dict):
            for source_name, df_raw in dfs_v2_dict.items():
                if not df_raw.empty:
                    processed_df = transform_v2_direct(df_raw, source_name)
                    if not processed_df.empty:
                        final_dfs.append(processed_df)
    
    elif mode_selection == "v1":
        if not df_q4.empty: final_dfs.append(transform_v1(df_q4, "Q4"))
        if not df_eta.empty: final_dfs.append(transform_v1(df_eta, "ETA"))

    if final_dfs:
        result = pd.concat(final_dfs, ignore_index=True)
        if mode_selection == "v2" and "unique_id" in result.columns:
            result.drop_duplicates(subset=["unique_id"], inplace=True)
        elif mode_selection == "v1" and "lead_id" in result.columns:
            result.drop_duplicates(subset=["lead_id"], inplace=True)

        if "entry_date" in result.columns:
            print(f"   ...🔄 Mengurutkan data berdasarkan entry_date...")
            result["_sort_temp"] = pd.to_datetime(result["entry_date"], errors='coerce')
            result = result.sort_values(by="_sort_temp", ascending=True, na_position='last')
            result.drop(columns=["_sort_temp"], inplace=True)
            result = result.reset_index(drop=True)

        return result
    else:
        return pd.DataFrame()