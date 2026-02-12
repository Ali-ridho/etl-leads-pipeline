import step3_read_gsheet
import pandas as pd

print("=== 🔍 CEK HEADER PART 1 ===")
# Ambil data
df_q4, df_eta, dfs_v2 = step3_read_gsheet.extract_all("v2")
df = dfs_v2.get("Part1", pd.DataFrame())

if not df.empty:
    print(f"Jumlah Baris: {len(df)}")
    print("\n--- DAFTAR SEMUA KOLOM ---")
    # Tampilkan semua nama kolom (header)
    cols = list(df.columns)
    for i, col in enumerate(cols):
        print(f"Col [{i}]: {col}")

    print("\n--- SAMPEL DATA BARIS PERTAMA ---")
    # Tampilkan isi baris pertama biar ketahuan isinya apa
    print(df.iloc[0].to_dict())
else:
    print("❌ Data Part 1 Kosong!")