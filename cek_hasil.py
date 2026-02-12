import pandas as pd
import os
from step4_transform import get_final_df

print("🚀 SEDANG MENJALANKAN ETL (EXTRACT & TRANSFORM SAJA)...")
print("⏳ Tunggu sebentar, sedang membaca Google Sheet...")

# Ambil data hasil olahan logic terbaru
df = get_final_df("v2")

if not df.empty:
    # Simpan ke CSV biar bisa dibuka di Excel
    filename = "HASIL_VALIDASI_MENTOR.csv"
    
    # Gunakan pemisah titik koma (;) biar rapi di Excel Indonesia
    df.to_csv(filename, index=False, sep=';') 
    
    print("\n" + "="*50)
    print(f"✅ SUKSES! File Validasi tersimpan: {filename}")
    print("="*50)
    print("👉 TUGAS MAS SEKARANG:")
    print("1. Buka file CSV itu di Excel.")
    print("2. Cek kolom 'lead_category': Isinya benar kategori atau Nama Orang?")
    print("3. Cek kolom 'phone_number': Apakah format 625 sudah berubah jadi 6285?")
    print("4. Cek kolom paling kanan: Apakah 'row_hash' ada di ujung?")
else:
    print("❌ GAGAL. Tidak ada data yang terbaca.")