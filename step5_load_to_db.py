import pandas as pd
import sys
import datetime
import os
import time
from sqlalchemy import create_engine, text, inspect
from step4_transform import get_final_df
from step3_read_gsheet import load_config  

# ==============================
# 📝 SISTEM LOGGING
# ==============================
class DualLogger(object):
    def __init__(self):
        self.terminal = sys.stdout
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        self.log = open(f"etl_log_{today_str}.txt", "a", encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)  

    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = DualLogger()
print(f"\n======== [LOG DIMULAI: {datetime.datetime.now()}] ========")

# ==============================
# 1. CONFIG DATABASE
# ==============================
DB_USER = 'root'
DB_PASSWORD = '' 
DB_HOST = 'localhost'
DB_NAME = 'db_mapping' 
TABLE_NAME = 'leads_master_v2'

connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(connection_string)

# ==============================
# 2. HELPER FUNCTIONS
# ==============================

def smart_sync_columns(engine, df, table_name):
    """
    [POINT 1] AUTO ADD COLUMN
    """
    print("   🔍 [Auto Column] Mengecek struktur kolom database...")
    inspector = inspect(engine)
    
    if not inspector.has_table(table_name):
        return 

    existing_columns_list = [col['name'] for col in inspector.get_columns(table_name)]
    
    anchor_column = "" 
    if "row_hash" in existing_columns_list:
        idx = existing_columns_list.index("row_hash")
        if idx > 0: anchor_column = existing_columns_list[idx - 1]
    if not anchor_column and len(existing_columns_list) > 0:
        anchor_column = existing_columns_list[-1]

    new_columns_candidate = df.columns.tolist()

    with engine.connect() as conn:
        for col in new_columns_candidate:
            if col not in existing_columns_list:
                print(f"      🚀 KOLOM BARU TERDETEKSI: '{col}' belum ada di DB.")
                col_type = "TEXT" 
                if "date" in col or "_at" in col: col_type = "DATETIME"
                elif "price" in col or "amount" in col or "quantity" in col or "age" in col: col_type = "DOUBLE"
                elif "is_" in col: col_type = "BOOLEAN"
                
                try:
                    position_sql = ""
                    if anchor_column: position_sql = f"AFTER `{anchor_column}`"
                    alter_query = text(f"ALTER TABLE {table_name} ADD COLUMN `{col}` {col_type} {position_sql}")
                    conn.execute(alter_query)
                    print(f"      ✅ Sukses menambahkan kolom: {col}")
                    anchor_column = col 
                except Exception as e:
                    print(f"      ❌ Gagal menambahkan kolom {col}: {e}")

def generate_upsert_query(table_name, columns):
    """
    [CRITICAL FIX] URUTAN UPDATE DIPERBAIKI
    updated_at harus diproses PERTAMA sebelum row_hash diubah oleh MySQL.
    """
    cols_str = ", ".join([f"`{c}`" for c in columns])
    vals_str = ", ".join([f":{c}" for c in columns])
    
    # Pisahkan logika updated_at dan kolom lain
    update_parts = []
    updated_at_logic = ""

    for col in columns:
        # Skip PK & Created At
        if col in ['unique_id', 'created_at']:
            continue
            
        # Simpan logika updated_at secara terpisah
        if col == 'updated_at':
            # 🔥 INI BARIS YANG DIUBAH: Tambahan OR untuk mengecek perubahan status 🔥
            updated_at_logic = f"`updated_at` = IF(VALUES(`row_hash`) != `row_hash` OR VALUES(`sync_status`) != `sync_status`, NOW(), `updated_at`)"
        
        # Kolom biasa (termasuk row_hash)
        else:
            update_parts.append(f"`{col}` = VALUES(`{col}`)")
            
    # 🔥 GABUNGKAN KEMBALI: updated_at HARUS PALING DEPAN 🔥
    # Ini memaksa MySQL cek hash lama VS baru SEBELUM hash-nya ditimpa.
    if updated_at_logic:
        final_update_list = [updated_at_logic] + update_parts
    else:
        final_update_list = update_parts

    update_str = ", ".join(final_update_list)
    
    sql = f"""
    INSERT INTO {table_name} ({cols_str})
    VALUES ({vals_str})
    ON DUPLICATE KEY UPDATE
    {update_str};
    """
    return text(sql)

def sync_deletions(engine, df, table_name):
    if df.empty: return
    print("   🧹 Memulai Sinkronisasi Penghapusan (Sync Delete)...")
    with engine.connect() as conn:
        processed_sources = df['source_id'].unique().tolist()
        for source in processed_sources:
            current_ids = df[df['source_id'] == source]['unique_id'].tolist()
            if not current_ids: continue
            ids_formatted = "', '".join([str(x).replace("'", "''") for x in current_ids])
            delete_sql = text(f"""
                DELETE FROM {table_name} 
                WHERE source_id = :src 
                AND unique_id NOT IN ('{ids_formatted}')
            """)
            result = conn.execute(delete_sql, {"src": source})
            conn.commit()
            if result.rowcount > 0:
                print(f"      🗑️  Ditemukan {result.rowcount} data sampah di '{source}'. DIHAPUS.")

def update_inactive_status(engine, inactive_files, table_name):
    if not inactive_files: return
    print(f"\n   🔒 Mengunci (LOCK) {len(inactive_files)} file non-aktif...")
    with engine.connect() as conn:
        for source in inactive_files:
            sql = text(f"""
                UPDATE {table_name}
                SET sync_status = 'LOCKED',
                    updated_at = NOW()
                WHERE source_id = :src
                AND (sync_status != 'LOCKED' OR sync_status IS NULL)
            """)
            conn.execute(sql, {"src": source})
            conn.commit()

# ==============================
# 3. MAIN ETL PROCESS
# ==============================
def load_data_upsert(mode="v2"):
    print(f"\n=== ETL START: MODE {mode.upper()} (FIX: PRIORITY UPDATE TIME) ===")
    
    try:
        config_data = load_config() 
    except Exception as e:
        print(f"❌ Gagal load config: {e}")
        return

    all_files = config_data.get(mode, {})
    status_map = config_data.get("status_map", {}) 
    sorted_keys = sorted(all_files.keys())
    active_keys = [k for k in sorted_keys if status_map.get(k, True)]
    inactive_keys = [k for k in sorted_keys if not status_map.get(k, True)]

    print(f"   📂 Total File: {len(sorted_keys)} | ✅ Active: {len(active_keys)}")
    
    inspector = inspect(engine)
    if inspector.has_table(TABLE_NAME) and inactive_keys:
        update_inactive_status(engine, inactive_keys, TABLE_NAME)

    df = get_final_df(mode_selection=mode)
    
    if df.empty:
        print("⚠️ Tidak ada data ACTIVE yang terbaca.")
        return

    df_active = df[df['source_id'].isin(active_keys)].copy()
    
    if df_active.empty:
        print("⚠️ Data ditemukan, tapi tidak ada yang statusnya Active.")
        return

    print(f"   🚀 Memproses {len(df_active)} baris data AKTIF...")
    df_active['sync_status'] = 'ACTIVE'
    
    smart_sync_columns(engine, df_active, TABLE_NAME)

    df_final = df_active.where(pd.notnull(df_active), None)

    try:
        if inspector.has_table(TABLE_NAME):
            sync_deletions(engine, df_final, TABLE_NAME)
    except Exception as e:
        print(f"⚠️ Gagal Sync Delete: {e}")

    data_to_insert = df_final.to_dict(orient='records')
    if not data_to_insert: return

    query = generate_upsert_query(TABLE_NAME, df_final.columns.tolist())
    
    batch_size = 1000
    total_inserted = 0
    
    with engine.connect() as conn:
        print("   🔌 Terhubung ke Database. Memulai Batch Processing...")
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i : i + batch_size]
            try:
                conn.execute(query, batch)
                conn.commit()
                print(f"      📦 Batch {i} - {i+len(batch)} sukses diproses...")
                total_inserted += len(batch)
            except Exception as e:
                print(f"      ❌ Error batch {i}: {e}")

    print(f"\n🎉 SUKSES! Total Data Aktif Terproses: {total_inserted}")

if __name__ == "__main__":
    load_data_upsert("v2")