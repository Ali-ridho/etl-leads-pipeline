import gspread
import pandas as pd
from google.oauth2.service_account import Credentials # Update import
import os

# =========================
# 1. KONFIGURASI MASTER
# =========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ID Sheet "INSERT_CONFIG"
MASTER_CONFIG_ID = "1uEsOg6UgKfLhbULyvgKYS0pVX22SEChUPdUu7FTrZPQ" 
MASTER_TAB_NAME = "Sheet1" 

# --- CONFIG V1 (Data Lama - Tetap) ---
SHEET_ID_Q4 = "1SqTKPTf6sYVOPjDSrCq-QQMxd2gf-WAoM3fpk33smG8"
WORKSHEET_Q4 = 0
SHEET_ID_ETA = "1vzokc_TlXdPga3QospR9C8a4YfTp4P39v2L_8Z99I7g"
WORKSHEET_ETA = 0

# =========================
# 2. AUTHENTICATION (SUDAH DIPERBAIKI UTK ROBOT)
# =========================
def get_gspread_client():
    # Cek apakah kunci robot ada di folder yang sama
    if os.path.exists("service_account.json"):
        print("🤖 Menggunakan Service Account!")
        return gspread.service_account(filename="service_account.json")
    else:
        print("❌ Error: File 'service_account.json' tidak ditemukan!")
        print("   Pastikan file JSON dari Google Cloud sudah direname dan ditaruh di sini.")
        exit()

# =========================
# 3. HELPER: BACA CONFIG DINAMIS (+ DEBUG MODE)
# =========================
def get_source_files_from_master():
    """Membaca daftar file DAN STATUS dari Google Sheet 'INSERT_CONFIG'."""
    print(f"📡 Mengambil daftar file dari Master Config...")
    try:
        client = get_gspread_client()
        sh = client.open_by_key(MASTER_CONFIG_ID)
        ws = sh.worksheet(MASTER_TAB_NAME)
        
        # Ambil semua data
        all_rows = ws.get_all_values()
        
        if len(all_rows) < 2: return [] 
        
        clean_list = []
        
        print(f"   🔎 DEBUG MODE ON: Memeriksa Config...")

        # Loop mulai dari baris ke-2
        for i, row in enumerate(all_rows[1:]):
            # Jika baris kosong total, skip
            if not any(row): continue
            
            # --- FIX ANTI RABUN (Padding Row) ---
            while len(row) < 4:
                row.append("")
            
            label = str(row[0]).strip()
            s_id = str(row[1]).strip()
            tab_name = str(row[2]).strip()
            
            # Baca Status (Kolom Indeks 3 / Kolom D)
            status_raw = str(row[3]).strip().upper()
            
            # LOGIKA DETEKSI FALSE
            if status_raw == "FALSE" or status_raw == "OFF":
                is_active = False
                print(f"      🔴 [BARIS {i+2}] {label} -> STATUS: LOCKED (Skip)")
            else:
                is_active = True
                print(f"      🟢 [BARIS {i+2}] {label} -> STATUS: ACTIVE (Baca)")

            if s_id != "":
                clean_list.append({
                    "label": label,
                    "id": s_id,
                    "tab_name": tab_name,
                    "is_active": is_active 
                })
        
        return clean_list

    except Exception as e:
        print(f"❌ Gagal membaca Master Config: {e}")
        return []

# =========================
# 4. SAFE READER (BACA DATA)
# =========================
def read_google_sheet(sheet_id, worksheet_ident=0):
    try:
        client = get_gspread_client()
        sh = client.open_by_key(sheet_id)
        
        if isinstance(worksheet_ident, str):
            ws = sh.worksheet(worksheet_ident)
        else:
            ws = sh.get_worksheet(worksheet_ident)

        values = ws.get_all_values()
        if not values: return pd.DataFrame()

        header_row = None
        for i, row in enumerate(values):
            if not row: continue
            first_col = str(row[0]).strip().lower()
            if first_col == "no" or first_col == "row" or first_col == "nomor":
                header_row = i
                break
        
        if header_row is None: header_row = 0
        headers = [str(h).strip() for h in values[header_row]]
        rows = values[header_row + 1 :]
        df = pd.DataFrame(rows, columns=headers)
        df.columns = df.columns.str.strip()
        
        if "No" in df.columns: df.rename(columns={"No": "row"}, inplace=True)
        if "no" in df.columns: df.rename(columns={"no": "row"}, inplace=True)

        return df
    except Exception as e:
        print(f"❌ Error reading sheet {sheet_id}: {e}")
        return pd.DataFrame()

# =========================
# 5. EXTRACT ALL (AUTO-SKIP INACTIVE)
# =========================
def extract_all(target_mode="v2"):
    df_q4 = pd.DataFrame()
    df_eta = pd.DataFrame()
    dfs_v2 = {} 

    print(f"=== STEP 3: READ GOOGLE SHEETS (MODE: {target_mode.upper()}) ===")

    try:
        if target_mode == "v1":
            df_q4 = read_google_sheet(SHEET_ID_Q4, WORKSHEET_Q4)
            df_eta = read_google_sheet(SHEET_ID_ETA, WORKSHEET_ETA)

        elif target_mode == "v2":
            source_files = get_source_files_from_master()
            
            if not source_files:
                print("⚠️ Tidak ada file yang ditemukan di Config!")
                return df_q4, df_eta, dfs_v2

            for file_info in source_files:
                label = file_info['label']
                
                # FITUR OPTIMASI:
                if not file_info['is_active']:
                    # print(f"   ⛔ Skip Baca (Status FALSE di Config): {label}")
                    continue # Silent skip biar terminal bersih

                sheet_id = file_info['id']
                tab_name = file_info['tab_name']
                print(f"   📄 Membaca Data: {label}...")
                df = read_google_sheet(sheet_id, tab_name)
                
                if not df.empty:
                    dfs_v2[label] = df
                    print(f"      Rows {label}: {len(df)}")

    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        return df_q4, df_eta, dfs_v2

    return df_q4, df_eta, dfs_v2

# ==========================================
# 6. FUNGSI JEMBATAN UNTUK STEP 5
# ==========================================
def load_config():
    """
    Mengembalikan Config lengkap + Status Map untuk Step 5.
    """
    raw_files = get_source_files_from_master()
    
    v2_config = {}
    status_map = {} 
    
    for item in raw_files:
        v2_config[item['label']] = item['id']
        status_map[item['label']] = item['is_active']
        
    return {
        "v2": v2_config,
        "status_map": status_map, 
        "v1": {} 
    }

# === TESTING BLOCK (Biar bisa langsung di-run untuk cek koneksi) ===
if __name__ == "__main__":
    print("🚀 MENJALANKAN TEST KONEKSI ROBOT...")
    df_q4, df_eta, dfs_v2 = extract_all(target_mode="v2")
    print("✅ TEST SELESAI.")