from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import json
import pendulum

# Konfigurasi Dasar
default_args = {
    'owner': 'ridho_intern',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='gsheet_to_mysql_etl',
    default_args=default_args,
    schedule='0 8-17 * * *',  # Akan jalan otomatis setiap hari
    catchup=False,
    tags=['magang', 'etl']
) as dag:

    @task
    def extract_from_gsheet():
        import gspread
        import pandas as pd
        from airflow.models import Variable
        
        # ==========================================
        # 1. AUTHENTICATION & KONFIGURASI
        # ==========================================
        PROJECT_NAME = "insert_lead" # <-- Filter Project untuk DAG ini
        
        print("🤖 Menggunakan Kredensial dari Airflow Variables!")
        creds_json = Variable.get("google_sheets_config", deserialize_json=True)
        client = gspread.service_account_from_dict(creds_json)

        # ==========================================
        # 2. BACA MASTER CONFIG (VERSI ANTI-BADAI)
        # ==========================================
        MASTER_CONFIG_ID = "1uEsOg6UgKfLhbULyvgKYS0pVX22SEChUPdUu7FTrZPQ" 
        MASTER_TAB_NAME = "insert_lead" 
        
        print(f"📡 Mengambil daftar file dari Master Config...")
        sh = client.open_by_key(MASTER_CONFIG_ID)
        ws = sh.worksheet(MASTER_TAB_NAME)
        
        # Menggunakan get_all_records() agar kebal pergeseran kolom!
        all_configs = ws.get_all_records() 
        
        clean_list = []
        for i, row in enumerate(all_configs):
            project_col = str(row.get('Project', '')).strip().lower()
            label = str(row.get('LABEL', '')).strip()
            s_id = str(row.get('Spreadsheet_ID', '')).strip()
            tab_name = str(row.get('Tab_Name', '')).strip()
            status_raw = str(row.get('Status', '')).strip().upper()
            
            # 1. FILTER: Abaikan jika project-nya bukan "insert_lead"
            if project_col != PROJECT_NAME.lower():
                continue
                
            if not s_id or not label:
                continue
                
            # 2. TENTUKAN STATUS AKTIF ATAU TERKUNCI
            if status_raw in ["FALSE", "OFF"]:
                print(f" 🔴 [BARIS {i+2}] {label} -> STATUS: LOCKED (Skip!)")
                continue
            else:
                print(f" 🟢 [BARIS {i+2}] {label} -> STATUS: ACTIVE")
                clean_list.append({
                    "label": label, 
                    "id": s_id, 
                    "tab_name": tab_name, 
                    "status_dari_config": "ACTIVE"
                })
                    
        # ==========================================
        # 3. EXTRACT DATA BERSALURAN
        # ==========================================
        extracted_data = {} 
        
        if not clean_list:
            print(f"⚠️ Tidak ada data GSheet yang aktif untuk project {PROJECT_NAME}")
            return extracted_data
            
        for file_info in clean_list:
            label = file_info['label']
            sheet_id = file_info['id']
            tab_name = file_info['tab_name']
            status_sheet_ini = file_info['status_dari_config']
            
            print(f"📄 Membaca Data: {label} (Status: {status_sheet_ini})...")
            target_sh = client.open_by_key(sheet_id)
            target_ws = target_sh.worksheet(tab_name)
            values = target_ws.get_all_values()
            
            if values:
                header_row = 0
                for i, row in enumerate(values):
                    if not row: continue
                    first_col = str(row[0]).strip().lower()
                    if first_col in ["no", "row", "nomor"]:
                        header_row = i
                        break
                        
                headers = [str(h).strip() for h in values[header_row]]
                rows = values[header_row + 1 :]
                df = pd.DataFrame(rows, columns=headers)
                
                # --- JURUS PEMBERSIH NULL (Sudah aktif!) ---
                df = df.replace(r'^\s*$', None, regex=True)
                df = df.replace("", None)
                
                # Normalisasi nama kolom
                df.columns = df.columns.str.strip()
                if "No" in df.columns: df.rename(columns={"No": "row"}, inplace=True)
                if "no" in df.columns: df.rename(columns={"no": "row"}, inplace=True)
                
                # SUNTIKKAN STATUS DARI CONFIG KE DALAM DATAFRAME
                df["sync_status"] = status_sheet_ini
                
                extracted_data[label] = df.to_dict(orient='records')
                print(f" ✅ Rows {label}: {len(df)}")
            
        return extracted_data

    @task
    def transform_data(extracted_data):
        import pandas as pd
        import numpy as np
        import hashlib
        import re
        import pendulum

        print("⚙️ Memulai proses Transformasi Data...")
        
        # 1. KONFIGURASI & MAPPING
        SYSTEM_COLUMNS = ["row_hash", "created_at", "updated_at", "sync_status"]
        V2_COLUMN_MAP = {
            "no": "row", "nomor": "row", "whatsapp_link": "link_whatsapp", 
            "link_whatsapp": "link_whatsapp", "whatsapp": "link_whatsapp",
            "lead_category": "lead_category", "nama": "lead_name",
            "jenis_lead": "lead_type", "sumber_lead": "lead_source",
            "produk": "product_name", "category": "lead_category",
            "consumption_category": "lead_category" 
        }

        # 2. HELPER FUNCTIONS
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

        def generate_dynamic_row_hash(row, cols_to_hash):
            raw_str = "".join([str(row.get(col, '')).strip() for col in cols_to_hash])
            return hashlib.md5(raw_str.encode()).hexdigest()

        # 3. PROSES TRANSFORMASI UTAMA
        final_dfs = []
        for source_label, records in extracted_data.items():
            if not records: continue
            
            df = pd.DataFrame(records)
            print(f" 🔄 Memproses Sheet: {source_label} ({len(df)} baris)")
            
            df.columns = (df.columns.astype(str).str.strip().str.lower()
                          .str.replace(")", "", regex=False).str.replace("(", "", regex=False)
                          .str.replace(" ", "_", regex=False).str.replace(r"[^a-z0-9_]", "", regex=True))

            df = df.replace("LP-PB Everpro", "LT Everpro", regex=False)
            df.rename(columns=V2_COLUMN_MAP, inplace=True)
            df = df.loc[:, ~df.columns.duplicated()]

            if "part 1" in source_label.lower() and "lead_name" in df.columns:
                df["lead_name"] = df["lead_name"].apply(clean_garbage_name)

            df = df.reset_index(drop=True)
            df["row"] = df.index + 1 
            df["row"] = pd.to_numeric(df["row"], errors='coerce').fillna(0).astype(int)
            df["source_id"] = source_label
            df["unique_id"] = df["source_id"] + "_" + df["row"].astype(str).str.zfill(5)
            
            for col in ["entry_date", "input_date"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

            if 'phone_number' in df.columns:
                df['phone_number'] = df['phone_number'].apply(clean_phone_number)
                    
            current_df_cols = [c for c in df.columns.tolist() if 'unnamed' not in c]
            data_columns = [c for c in current_df_cols if c not in SYSTEM_COLUMNS]
            data_columns.sort()
            
            if not df.empty:
                df["row_hash"] = df.apply(lambda row: generate_dynamic_row_hash(row, data_columns), axis=1)
            else:
                df["row_hash"] = None

            now_wib = pendulum.now('Asia/Jakarta')
            now_str = now_wib.strftime('%Y-%m-%d %H:%M:%S')
            
            df["created_at"] = now_str
            df["updated_at"] = now_str

            for sys_col in SYSTEM_COLUMNS:
                if sys_col not in df.columns: df[sys_col] = None

            final_dfs.append(df)

        # 4. GABUNGKAN SEMUA HASIL
        if final_dfs:
            result = pd.concat(final_dfs, ignore_index=True)
            if "unique_id" in result.columns:
                result.drop_duplicates(subset=["unique_id"], inplace=True)

            if "entry_date" in result.columns:
                result["_sort_temp"] = pd.to_datetime(result["entry_date"], errors='coerce')
                result = result.sort_values(by="_sort_temp", ascending=True, na_position='last')
                result.drop(columns=["_sort_temp"], inplace=True)
                result = result.reset_index(drop=True)

            result = result.replace({np.nan: None})
            final_data = result.to_dict(orient='records')
            print(f"✅ Mantap! Transformasi Selesai. Total data siap Load: {len(final_data)} baris.")
            return final_data
        else:
            print("⚠️ Tidak ada data yang berhasil ditransformasi.")
            return []

    @task
    def load_to_mysql(final_data):
        import pandas as pd
        from sqlalchemy import text, inspect, create_engine

        # ==========================================
        # 1. KONEKSI DATABASE & CEK ALL-LOCKED
        # ==========================================
        engine = create_engine("mysql+pymysql://root:@host.docker.internal:3306/db_mapping")
        TABLE_NAME = 'leads_master_v2'

        if not final_data:
            print("⚠️ Tidak ada data yang diterima (Semua Sheet FALSE). Mengunci semua data di DB...")
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"UPDATE {TABLE_NAME} SET sync_status = 'LOCKED'"))
                    print("🔒 KUNCI SUKSES: Seluruh data di database telah di-LOCKED.")
            except Exception as e:
                print(f" ⚠️ Gagal mengunci semua data: {e}")
            return "No Data - All Locked"

        print(f"📦 Menerima {len(final_data)} baris data untuk dikirim ke MariaDB/MySQL...")
        
        df_final = pd.DataFrame(final_data) 

        # ==========================================
        # 2. HELPER FUNCTIONS
        # ==========================================
        def smart_sync_columns(engine, df, table_name):
            print(" 🔍 [Auto Column] Mengecek struktur kolom database...")
            inspector = inspect(engine)
            
            if not inspector.has_table(table_name):
                print(f" ⚠️ Tabel {table_name} belum ada, akan dibuat otomatis oleh sistem.")
                return 

            existing_columns_list = [col['name'] for col in inspector.get_columns(table_name)]
            anchor_column = existing_columns_list[-1] if existing_columns_list else ""
            new_columns_candidate = df.columns.tolist()

            with engine.begin() as conn:
                for col in new_columns_candidate:
                    if col not in existing_columns_list:
                        print(f" 🚀 KOLOM BARU TERDETEKSI: '{col}' belum ada di DB.")
                        col_type = "TEXT" 
                        if "date" in col or "_at" in col: col_type = "DATETIME"
                        elif "price" in col or "amount" in col or "quantity" in col or "age" in col: col_type = "DOUBLE"
                        elif "is_" in col: col_type = "BOOLEAN"
                        
                        try:
                            position_sql = f"AFTER `{anchor_column}`" if anchor_column else ""
                            alter_query = text(f"ALTER TABLE {table_name} ADD COLUMN `{col}` {col_type} {position_sql}")
                            conn.execute(alter_query)
                            print(f" ✅ Sukses menambahkan kolom: {col}")
                            anchor_column = col 
                        except Exception as e:
                            print(f" ❌ Gagal menambahkan kolom {col}: {e}")

        def generate_upsert_query(table_name, columns):
            cols_str = ", ".join([f"`{c}`" for c in columns])
            vals_str = ", ".join([f":{c}" for c in columns])
            
            update_parts = []
            updated_at_logic = ""

            for col in columns:
                if col in ['unique_id', 'created_at']: continue
                if col == 'updated_at':
                    updated_at_logic = f"`updated_at` = IF(VALUES(`row_hash`) != `row_hash` OR VALUES(`sync_status`) != `sync_status`, NOW(), `updated_at`)"
                else:
                    update_parts.append(f"`{col}` = VALUES(`{col}`)")
                    
            final_update_list = [updated_at_logic] + update_parts if updated_at_logic else update_parts
            update_str = ", ".join(final_update_list)
            
            sql = f"""
            INSERT INTO {table_name} ({cols_str})
            VALUES ({vals_str})
            ON DUPLICATE KEY UPDATE {update_str};
            """
            return text(sql)

        def sync_deletions(engine, df, table_name):
            if df.empty: return
            print(" 🧹 Memulai Sinkronisasi Penghapusan (Sync Delete)...")
            with engine.begin() as conn:
                processed_sources = df['source_id'].unique().tolist()
                for source in processed_sources:
                    current_ids = df[df['source_id'] == source]['unique_id'].tolist()
                    if not current_ids: continue
                    ids_formatted = "', '".join([str(x).replace("'", "''") for x in current_ids])
                    delete_sql = text(f"""
                        DELETE FROM {table_name} 
                        WHERE source_id = :src AND unique_id NOT IN ('{ids_formatted}')
                    """)
                    result = conn.execute(delete_sql, {"src": source})
                    if result.rowcount > 0:
                        print(f" 🗑️ Ditemukan {result.rowcount} data sampah di '{source}'. DIHAPUS.")

        # ==========================================
        # 3. PROSES EKSEKUSI UTAMA
        # ==========================================
        inspector = inspect(engine)
        if not inspector.has_table(TABLE_NAME):
            print(f" 🏗️ Membangun tabel awal {TABLE_NAME}...")
            df_final.head(0).to_sql(TABLE_NAME, con=engine, if_exists='replace', index=False)
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (`unique_id`);"))

        smart_sync_columns(engine, df_final, TABLE_NAME)

        try:
            sync_deletions(engine, df_final, TABLE_NAME)
        except Exception as e:
            print(f" ⚠️ Gagal Sync Delete: {e}")

        # EKSEKUSI INSERT / UPDATE (Hanya berjalan 1x!)
        data_to_insert = df_final.to_dict(orient='records')
        query = generate_upsert_query(TABLE_NAME, df_final.columns.tolist())
        batch_size = 1000
        total_inserted = 0

        with engine.begin() as conn:
            print(" 🔌 Terhubung ke MariaDB. Memulai Batch Processing...")
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i : i + batch_size]
                try:
                    conn.execute(query, batch)
                    print(f" 📦 Batch {i} - {i+len(batch)} sukses diproses...")
                    total_inserted += len(batch)
                except Exception as e:
                    print(f" ❌ Error batch {i}: {e}")

        # KUNCI DATA YANG TIDAK AKTIF DARI CONFIG (LOCKED)
        try:
            with engine.begin() as conn:
                active_sources = df_final['source_id'].unique().tolist()
                if active_sources:
                    sources_formatted = "', '".join([str(x).replace("'", "''") for x in active_sources])
                    lock_sql = text(f"""
                        UPDATE {TABLE_NAME} 
                        SET sync_status = 'LOCKED' 
                        WHERE source_id NOT IN ('{sources_formatted}')
                    """)
                    result_lock = conn.execute(lock_sql)
                    print(f" 🔒 KUNCI SUKSES: {result_lock.rowcount} baris data dari GSheet yang FALSE telah di-LOCKED.")
        except Exception as e:
            print(f" ⚠️ Gagal mengunci data: {e}")

        print(f"\n🎉 SUKSES BESAR! Total {total_inserted} Data Berhasil Masuk ke Database MariaDB/MySQL!")
        return "Selesai 100%"

    # Mengatur Urutan Kerja (Flow)
    data_raw = extract_from_gsheet()
    data_clean = transform_data(data_raw)
    load_to_mysql(data_clean)