from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import json
import pendulum

# =========================================================================
# TEMPLATE REUSABLE ETL: GOOGLE SHEETS TO MYSQL
# =========================================================================

PROJECT_NAME = "ctwa"                  # Nama Project di Master Config GSheet
TARGET_TABLE = "ctwa_leads_raw_data"   # Nama Tabel Tujuan di MySQL/MariaDB
SOURCE_PK_COLUMN = "id"


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
        tab_key_name = f"{PROJECT_NAME}_tab"
        master_tab = master_config.get(tab_key_name)
        
        ws = sh.worksheet(master_tab)
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
                df['sync'] = sync_val 
                extracted_data[label] = df.to_dict(orient='records')
            
        return {"active_data": extracted_data, "inactive_labels": inactive_list}

    @task
    def transform_data(payload):
        import pandas as pd
        import numpy as np
        import hashlib
        import re
        import pendulum
        import json # Wajib import json untuk The JSON Hack

        extracted_data = payload.get("active_data", {})
        inactive_labels = payload.get("inactive_labels", [])        
        
        print("⚙️ Memulai proses Transformasi Data...")
        SYSTEM_COLUMNS = ["source_sheet", "row_hash", "created_at", "updated_at", "sync", "unique_id"]

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

            # --- MESIN TRANSLASI PK DINAMIS ---
            pk_source = SOURCE_PK_COLUMN.lower().strip().replace(" ", "_")
            if pk_source in df.columns and pk_source != 'id':
                df.rename(columns={pk_source: 'id'}, inplace=True)
            elif 'id' not in df.columns and len(df.columns) > 0:
                df.rename(columns={df.columns[0]: 'id'}, inplace=True)
            # ----------------------------------

            df = df.loc[:, ~df.columns.duplicated()]
            df["source_sheet"] = source_label
            
            # --- PEMBUATAN UNIQUE ID ANTI-TABRAKAN ---
            if 'id' in df.columns:
                df["unique_id"] = df["source_sheet"] + "_" + df["id"].astype(str).str.zfill(5)
            else:
                df["unique_id"] = df["source_sheet"] + "_" + df.index.astype(str).str.zfill(5)
            
            # -----------------------------------------
            
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

            # --- DYNAMIC TYPE CASTING & AUTO-SORTING ---
            if 'id' in result.columns:
                result['id'] = pd.to_numeric(result['id'], downcast='integer', errors='ignore')
                result = result.sort_values(by=['source_sheet', 'id'], ascending=[True, True])
            # -------------------------------------------

            # --- JARING PENANGKAP NaN: THE JSON HACK ---
            # 1. Konversi ke JSON. Pandas TERPAKSA merubah NaN menjadi 'null' standar web
            json_str = result.to_json(orient='records', date_format='iso')
            
            # 2. Baca kembali JSON tersebut. Python akan merubah 'null' menjadi None murni!
            final_data = json.loads(json_str)
            # -------------------------------------------

            return {"final_data": final_data, "inactive_labels": inactive_labels}
        else:
            return {"final_data": [], "inactive_labels": inactive_labels}
    
        
    @task
    def load_to_mysql(payload):
        import pandas as pd
        import math
        from sqlalchemy import text, inspect, create_engine
        
        # 👇 TAMBAHAN IMPORT NULLPOOL 👇
        from sqlalchemy.pool import NullPool 
        
        final_data = payload.get("final_data", [])
        inactive_labels = payload.get("inactive_labels", [])
        
        # 👇 ENGINE ANTI DOCKER TIMEOUT 👇
        engine = create_engine(
            "mysql+pymysql://root:@host.docker.internal:3306/db_mapping",
            poolclass=NullPool
        )
        inspector = inspect(engine)

        if inactive_labels and inspector.has_table(TARGET_TABLE):
            try:
                with engine.connect() as conn:
                    inactive_str = "', '".join([str(x).replace("'", "''") for x in inactive_labels])
                    update_inactive_sql = text(f"UPDATE {TARGET_TABLE} SET sync = 'FALSE', updated_at = NOW() WHERE source_sheet IN ('{inactive_str}')")
                    conn.execute(update_inactive_sql)
                    conn.commit()
                    print(f"🔴 Berhasil mematikan status (FALSE) dan update waktu untuk: {inactive_labels}")
            except Exception as e:
                print(f"⚠️ Gagal update status inactive: {e}")

        if not final_data:
            print("⚠️ Tidak ada data ACTIVE untuk di-insert/upsert.")
            return "Selesai (Hanya update status)"

        df_schema = pd.DataFrame(final_data) 

        if not inspector.has_table(TARGET_TABLE):
            print(f" 🏗️ Membangun tabel awal {TARGET_TABLE}...")
            df_schema.head(0).to_sql(TARGET_TABLE, con=engine, if_exists='replace', index=False)
            try:
                with engine.connect() as conn:
                    conn.execute(text(f"ALTER TABLE {TARGET_TABLE} MODIFY `unique_id` VARCHAR(255) NOT NULL;"))
                    conn.execute(text(f"ALTER TABLE {TARGET_TABLE} ADD PRIMARY KEY (`unique_id`);"))
                    conn.commit()
            except Exception as e:
                pass
      
        def smart_sync_columns(engine, df_cols, table_name):
            _inspector = inspect(engine)
            existing_columns_list = [col['name'] for col in _inspector.get_columns(table_name)]
            anchor_column = existing_columns_list[-1] if existing_columns_list else ""
            new_columns_candidate = df_cols

            for col in new_columns_candidate:
                if col not in existing_columns_list:
                    col_type = "TEXT" 
                    if "date" in col or "_at" in col: col_type = "DATETIME"
                    elif "price" in col or "amount" in col or "quantity" in col: col_type = "DOUBLE"
                    
                    try:
                        position_sql = f"AFTER `{anchor_column}`" if anchor_column else ""
                        alter_query = text(f"ALTER TABLE {table_name} ADD COLUMN `{col}` {col_type} {position_sql}")
                        with engine.connect() as conn:
                            conn.execute(alter_query)
                            conn.commit()
                        anchor_column = col 
                    except Exception as e:
                        pass

        def generate_upsert_query(table_name, columns):
            cols_str = ", ".join([f"`{c}`" for c in columns])
            vals_str = ", ".join([f":{c}" for c in columns])
            
            update_parts = []
            updated_at_logic = ""

            for col in columns:
                if col in ['unique_id', 'id', 'source_sheet', 'created_at']: continue
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

        def sync_deletions(engine, data_list, table_name):
            if not data_list: return
            processed_sources = list(set([row.get('source_sheet') for row in data_list]))
            for source in processed_sources:
                current_ids = [str(row.get('unique_id')) for row in data_list if row.get('source_sheet') == source]
                if not current_ids: continue
                ids_formatted = "', '".join([str(x).replace("'", "''") for x in current_ids])
                delete_sql = text(f"""
                    DELETE FROM {table_name} 
                    WHERE source_sheet = :src AND unique_id NOT IN ('{ids_formatted}')
                """)
                
                try:
                    with engine.connect() as conn:
                        conn.execute(delete_sql, {"src": source})
                        conn.commit()
                except Exception as e:
                    print(f"⚠️ Gagal delete di source {source}: {e}")

        smart_sync_columns(engine, list(final_data[0].keys()), TARGET_TABLE)
        sync_deletions(engine, final_data, TARGET_TABLE)

        query = generate_upsert_query(TARGET_TABLE, list(final_data[0].keys()))
        batch_size = 1000

        clean_data_to_insert = []
        for row in final_data:
            clean_row = {}
            for k, v in row.items():
                if v is None:
                    clean_row[k] = None
                elif isinstance(v, float) and math.isnan(v):
                    clean_row[k] = None
                elif isinstance(v, float) and v.is_integer():
                    clean_row[k] = int(v)
                else:
                    clean_row[k] = v
            clean_data_to_insert.append(clean_row)

        for i in range(0, len(clean_data_to_insert), batch_size):
            batch = clean_data_to_insert[i : i + batch_size]
            try:
                with engine.connect() as conn:
                    conn.execute(query, batch)
                    conn.commit()
            except Exception as e:
                print(f"⚠️ Gagal insert batch data: {e}")
                
        print("🎉 SUKSES BESAR! Data UPSERT selesai.")
        return "Selesai 100%"
    
    data_raw = extract_from_gsheet()
    data_clean = transform_data(data_raw)
    load_to_mysql(data_clean)