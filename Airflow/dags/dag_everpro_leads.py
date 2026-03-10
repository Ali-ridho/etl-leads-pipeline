from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import json
import pendulum

# ==========================================
# 1. KONFIGURASI PROJECT UTAMA 
# ==========================================
PROJECT_NAME = "everpro" # Ganti dengan nama project di Master Config (kolom 'Project'). Contoh: "ctwa"
TARGET_TABLE = "everpro_leads_raw_data" # Ganti dengan nama tabel tujuan di MariaDB/DBeaver. Contoh: "ctwa_leads_raw_data"
CONFIG_TAB_NAME = "everpro_leads" # Ganti dengan nama tab tujuan di GSheet Master Config. Contoh: "ctwa_leads"

default_args = {
    'owner': 'ridho_intern',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=f'dag_{PROJECT_NAME}_leads_project', 
    default_args=default_args,
    schedule='0 8-17 * * *',
    catchup=False,
    tags=[PROJECT_NAME, 'etl', 'reusable']
) as dag:

    @task
    def extract_from_gsheet():
        import gspread
        import pandas as pd
        from airflow.models import Variable
        
        print("🤖 Menggunakan Kredensial dari Airflow Variables!")
        creds_json = Variable.get("google_sheets_config", deserialize_json=True)
        client = gspread.service_account_from_dict(creds_json)
        master_config = Variable.get("master_config_sheet", deserialize_json=True)

        sh = client.open_by_key(master_config['config_sheet_id'])
        ws = sh.worksheet(CONFIG_TAB_NAME)
        all_configs = ws.get_all_records() 
        
        clean_list = []
        inactive_list = []
        
        for i, row in enumerate(all_configs):
            project_col = str(row.get('Project', '')).strip().lower()
            label = str(row.get('LABEL', '')).strip()
            s_id = str(row.get('Spreadsheet_ID', '')).strip()
            tab_name = str(row.get('Tab_Name', '')).strip()
            status_raw = str(row.get('Status', '')).strip().upper()
            
            if project_col != PROJECT_NAME.lower(): continue
            if not s_id or not label: continue
            
            if status_raw in ["FALSE", "OFF"]: 
                inactive_list.append(label)
                continue
            
            clean_list.append({"label": label, "id": s_id, "tab_name": tab_name, "sync_status": "ACTIVE"})
                    
        extracted_data = {} 
        for file_info in clean_list:
            label = file_info['label']
            sheet_id = file_info['id']
            tab_name = file_info['tab_name']
            sync_val = file_info['sync_status']
            
            target_sh = client.open_by_key(sheet_id)
            target_ws = target_sh.worksheet(tab_name)
            values = target_ws.get_all_values()
            
            if len(values) > 1:
                headers = [str(h).strip() for h in values[0]]
                rows = values[1:]
                df = pd.DataFrame(rows, columns=headers)
                
                df = df.replace(r'^\s*$', None, regex=True)
                df = df.replace("", None)
                
                df.columns = df.columns.str.strip()
                df['sync'] = sync_val # PERBAIKAN TYPO
                extracted_data[label] = df.to_dict(orient='records')
            
        return {"active_data": extracted_data, "inactive_labels": inactive_list}

    @task
    def transform_data(payload):
        import pandas as pd
        import numpy as np
        import hashlib
        import re
        import pendulum

        # PERBAIKAN TYPO VARIABEL
        extracted_data = payload.get("active_data", {})
        inactive_labels = payload.get("inactive_labels", [])        
        
        print("⚙️ Memulai proses Transformasi Data...")
        
        SYSTEM_COLUMNS = ["source_sheet", "row_hash", "created_at", "updated_at", "sync"]

        def clean_phone_number(phone):
            if pd.isna(phone) or str(phone).strip() == "": return None
            phone = str(phone).strip()
            phone = re.sub(r'[^0-9*]', '', phone) 
            if len(phone) < 5: return None 
            if phone.startswith("08"): phone = "628" + phone[2:]
            elif not phone.startswith("62"): phone = "62" + phone
            return phone    

        def generate_dynamic_row_hash(row, cols_to_hash):
            raw_str = "".join([str(row.get(col, '')).strip() for col in cols_to_hash])
            return hashlib.md5(raw_str.encode()).hexdigest()

        final_dfs = []
        for source_label, records in extracted_data.items():
            if not records: continue
            
            df = pd.DataFrame(records)
            df.columns = (df.columns.astype(str).str.strip().str.lower()
                          .str.replace(")", "", regex=False).str.replace("(", "", regex=False)
                          .str.replace(" ", "_", regex=False).str.replace(r"[^a-z0-9_]", "", regex=True))

            df = df.loc[:, ~df.columns.duplicated()]

            df["source_sheet"] = source_label
            
            if 'phone_number' in df.columns:
                df.rename(columns={'phone_number': 'phone_numl'}, inplace=True)

            if 'phone_numl' in df.columns:
                df['phone_numl'] = df['phone_numl'].apply(clean_phone_number)
            elif 'whatsapp' in df.columns: 
                df.rename(columns={'whatsapp': 'phone_numl'}, inplace=True)
                df['phone_numl'] = df['phone_numl'].apply(clean_phone_number)
                    
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

            final_dfs.append(df)

        if final_dfs:
            result = pd.concat(final_dfs, ignore_index=True)

            if 'id' in result.columns:
                result['id_num'] = pd.to_numeric(result['id'], errors='coerce')
                result = result.sort_values(by=['source_sheet', 'id_num'], ascending=[True, True])
                result.drop(columns=['id_num'], inplace=True)

            result = result.replace({np.nan: None})
            final_data = result.to_dict(orient='records')
            return {"final_data": final_data, "inactive_labels": inactive_labels}
        else:
            return {"final_data": [], "inactive_labels": inactive_labels}

    @task
    def load_to_mysql(payload):
        import pandas as pd
        from sqlalchemy import text, inspect, create_engine
        
        final_data = payload.get("final_data", [])
        inactive_labels = payload.get("inactive_labels", [])
        
        engine = create_engine("mysql+pymysql://root:@host.docker.internal:3306/db_mapping")

        # PERBAIKAN LOGIKA: Eksekusi status FALSE harus paling awal!
        if inactive_labels:
            with engine.begin() as conn:
                inactive_str = "', '".join([str(x).replace("'", "''") for x in inactive_labels])
                update_inactive_sql = text(f"UPDATE {TARGET_TABLE} SET sync = 'FALSE' WHERE source_sheet IN ('{inactive_str}')")
                conn.execute(update_inactive_sql)
                print(f"🔴 Berhasil mematikan status (FALSE) untuk: {inactive_labels}")

        if not final_data:
            print("⚠️ Tidak ada data ACTIVE untuk di-insert/upsert.")
            return "Selesai (Hanya update status)"

        df_final = pd.DataFrame(final_data) 

        def smart_sync_columns(engine, df, table_name):
            inspector = inspect(engine)
            existing_columns_list = [col['name'] for col in inspector.get_columns(table_name)]
            anchor_column = existing_columns_list[-1] if existing_columns_list else ""
            new_columns_candidate = df.columns.tolist()

            with engine.begin() as conn:
                for col in new_columns_candidate:
                    if col not in existing_columns_list:
                        col_type = "TEXT" 
                        if "date" in col or "_at" in col: col_type = "DATETIME"
                        elif "price" in col or "amount" in col or "quantity" in col: col_type = "DOUBLE"
                        
                        try:
                            position_sql = f"AFTER `{anchor_column}`" if anchor_column else ""
                            alter_query = text(f"ALTER TABLE {table_name} ADD COLUMN `{col}` {col_type} {position_sql}")
                            conn.execute(alter_query)
                            anchor_column = col 
                        except Exception as e:
                            pass

        def generate_upsert_query(table_name, columns):
            cols_str = ", ".join([f"`{c}`" for c in columns])
            vals_str = ", ".join([f":{c}" for c in columns])
            
            update_parts = []
            updated_at_logic = ""

            for col in columns:
                if col in ['id', 'source_sheet', 'created_at']: continue
                if col == 'updated_at':
                    updated_at_logic = f"`updated_at` = IF(VALUES(`row_hash`) != `row_hash` OR VALUES(`sync`) != `sync`, NOW(), `updated_at`)"
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
            with engine.begin() as conn:
                processed_sources = df['source_sheet'].unique().tolist()
                for source in processed_sources:
                    current_ids = df[df['source_sheet'] == source]['id'].astype(str).tolist()
                    if not current_ids: continue
                    ids_formatted = "', '".join([str(x).replace("'", "''") for x in current_ids])
                    delete_sql = text(f"""
                        DELETE FROM {table_name} 
                        WHERE source_sheet = :src AND id NOT IN ('{ids_formatted}')
                    """)
                    conn.execute(delete_sql, {"src": source})

        smart_sync_columns(engine, df_final, TARGET_TABLE)
        sync_deletions(engine, df_final, TARGET_TABLE)

        data_to_insert = df_final.to_dict(orient='records')
        query = generate_upsert_query(TARGET_TABLE, df_final.columns.tolist())
        batch_size = 1000

        with engine.begin() as conn:
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i : i + batch_size]
                conn.execute(query, batch)
                
        print("🎉 SUKSES BESAR! Data UPSERT selesai.")
        return "Selesai 100%"

    data_raw = extract_from_gsheet()
    data_clean = transform_data(data_raw)
    load_to_mysql(data_clean)