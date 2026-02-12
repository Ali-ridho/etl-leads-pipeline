import step3_read_gsheet
import pandas as pd

print("=== 🔍 DIAGNOSA DATA MENTAH PART 2 ===")

# Ambil data mentah (tanpa filter, tanpa rename)
df_q4, df_eta, dfs_v2 = step3_read_gsheet.extract_all("v2")
df_part2 = dfs_v2.get("Part2", pd.DataFrame())

if df_part2.empty:
    print("❌ Data Part 2 benar-benar kosong dari Google Sheet!")
else:
    print(f"✅ Data Part 2 ditemukan: {len(df_part2)} baris.")
    print("\n--- 1. DAFTAR NAMA KOLOM ASLI ---")
    print(list(df_part2.columns))
    
    print("\n--- 2. SAMPEL 5 BARIS PERTAMA ---")
    print(df_part2.head(5).to_string())

print("\n==================================")