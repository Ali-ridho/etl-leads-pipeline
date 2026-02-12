import gspread
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os
import re

# =========================
# 1. KONFIGURASI
# =========================
SPREADSHEET_ID = "13BPO4j9ormRwpnDMAq_APv49hXIzKpdlR0GyE4Ne0TM"  # ID Part 1

SOURCE_TABS = [
    "Q3 FORM 25", 
    "Q3 CTWA 25", 
    "Q4 FORM 25", 
    "Q4 CTWA 25", 
    "Q1 FORM 26"
]

TARGET_TAB = "INSERT" 

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# =========================
# 2. FUNGSI HITUNG & CARI MAX ID
# =========================
def get_gspread_client():
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES) 
            creds = flow.run_local_server(port=0)
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)
    return gspread.authorize(creds)

def analyze_rows(ws_name, client, spreadsheet_id):
    """
    Menghitung jumlah baris nyata DAN mencari angka terbesar di kolom pertama.
    Return: (real_count, max_id_number)
    """
    try:
        sh = client.open_by_key(spreadsheet_id)
        ws = sh.worksheet(ws_name)
        
        all_values = ws.get_all_values()
        if not all_values: return 0, 0
        
        # Cari Header
        start_row_index = 0
        for i, row in enumerate(all_values):
            if row and str(row[0]).strip() != "":
                # Cek apakah ini header (bukan angka)
                if not str(row[0]).strip().isdigit(): 
                    start_row_index = i
                    break
        
        data_rows = all_values[start_row_index + 1:]
        
        real_count = 0
        max_id_number = 0

        for row in data_rows:
            # Ambil nilai kolom pertama (Nomor/Row ID)
            if row:
                col_val = str(row[0]).strip()
                if col_val != "":
                    real_count += 1
                    
                    # Coba ambil angka terbesar (untuk cek gap)
                    # Hapus titik/koma jika formatnya 19.000
                    clean_num = re.sub(r'[^\d]', '', col_val) 
                    if clean_num.isdigit():
                        val = int(clean_num)
                        if val > max_id_number:
                            max_id_number = val
                
        return real_count, max_id_number
        
    except Exception as e:
        print(f"⚠️ Gagal membaca tab '{ws_name}': {e}")
        return 0, 0

# =========================
# 3. EKSEKUSI UTAMA
# =========================
def main():
    client = get_gspread_client()
    print(f"🕵️‍♂️ MEMULAI AUDIT DATA & ANALISA GAP...\n")
    
    total_expected = 0
    
    # 1. Hitung Sumber Data
    print("--- 1. CEK TAB SUMBER ---")
    for tab in SOURCE_TABS:
        count, _ = analyze_rows(tab, client, SPREADSHEET_ID) # Kita cuma butuh count di sini
        print(f"   📄 {tab.ljust(15)} : {count} baris")
        total_expected += count
        
    print(f"\n   ✅ TOTAL SEHARUSNYA : {total_expected} baris\n")
    
    # 2. Hitung Tab Gabungan & Analisa Gap
    print("--- 2. CEK TAB HASIL GABUNGAN (INSERT LEAD) ---")
    actual_count, max_label = analyze_rows(TARGET_TAB, client, SPREADSHEET_ID)
    
    print(f"   📊 {TARGET_TAB.ljust(15)} : {actual_count} baris (Count Real)")
    print(f"   🏷️  Label Terakhir  : {max_label} (Nomor di Excel)")
    
    # 3. Analisa GAP (Rumus Matematika)
    print("\n--- 3. ANALISA GAP PENOMORAN ---")
    gap = max_label - actual_count
    
    print(f"   $$ {max_label} \\text{{ (Label Terakhir)}} - {actual_count} \\text{{ (Jumlah Real)}} = {gap} \\text{{ Baris Bolong (Gap)}} $$")
    
    if gap > 0:
        print(f"\n   ℹ️  INFO: Ada {gap} nomor urut yang loncat/hilang di Excel sumber.")
        print("       Ini normal jika admin pernah menghapus baris di tengah-tengah.")
    elif gap == 0:
        print("\n   ✨ INFO: Penomoran sempurna! Tidak ada nomor yang loncat.")
    else:
        print("\n   ❓ INFO: Jumlah baris lebih banyak dari nomor terakhir (Duplikat/Tanpa Nomor?).")

    # 4. Kesimpulan Akhir
    print("\n--- 4. KESIMPULAN AUDIT ---")
    diff = actual_count - total_expected
    if diff == 0:
        print("🎉 SEMPURNA! Data 'INSERT' Balance dengan Total Sumber.")
    elif diff > 0:
        print(f"⚠️ PERINGATAN: Ada kelebihan {diff} baris. Cek baris kosong di formula.")
    else:
        print(f"❌ ERROR: Data kurang {abs(diff)} baris.")

if __name__ == "__main__":
    main()