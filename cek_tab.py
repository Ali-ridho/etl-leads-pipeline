import step3_read_gsheet

# ID File Part 1 & Part 2
FILES = {
    "FILE PART 1 (Lama)": "15fyf3KEIdErPx3BCAva9-ZGqH_cUuxDsUICYnqlwCYw",
    "FILE PART 2 (Baru)": "1QWVBy6enIrN-1-iEPq6wrKecWuLh8zj8ldkiWLpLdoQ"
}

print("=== 🔍 INSPEKSI NAMA TAB ===")
try:
    client = step3_read_gsheet.get_gspread_client()
    for label, file_id in FILES.items():
        print(f"\n📂 {label}")
        try:
            sh = client.open_by_key(file_id)
            print(f"   Judul File: {sh.title}")
            
            # Print semua nama tab
            for i, ws in enumerate(sh.worksheets()):
                print(f"   👉 Tab [{i}]: '{ws.title}'")
        except Exception as e:
            print(f"   ❌ Gagal buka file: {e}")

except Exception as e:
    print(f"❌ Error Login: {e}")