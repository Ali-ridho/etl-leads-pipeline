import pandas as pd
from sqlalchemy import create_engine, text
from step4_transform import get_final_df
import time

# =========================
# CONFIG
# =========================
DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "db_mapping"
TABLE_NAME = "leads_master"

FORCE_DATE_MIGRATION = True  # SET False setelah 1x sukses

# =========================
# MAIN
# =========================
if __name__ == "__main__":

    batch_ts = int(time.time())
    df = get_final_df()

    # =========================
    # VALIDASI ID (WAJIB)
    # =========================
    df = df[df["lead_id"].notna()]
    df = df[df["lead_id"].astype(str).str.strip() != ""]
    df = df[df["lead_id"].str.match(r"^(Q4|ETA)_\d+$", na=False)]

    # =========================
    # CANONICAL DATAFRAME
    # =========================
    df_dw = pd.DataFrame({
        "row_no": df["No"],
        "lead_id": df["lead_id"],
        "entry_year": df["entry_year"],
        "entry_month": df["entry_month"],
        "entry_date": df["entry_date"],
        "input_date": df["input_date"],
        "lead_name": df["Nama"],
        "phone_number": df["Nomor"],
        "whatsapp_link": df["Link Whatsapp"],
        "lead_type": df["lead_type"],
        "product_name": df["Produk"],
        "response": df["RESPON"],
        "description": df["KETERANGAN"],
        "standard_price": df["VALUE"],
        "quantity": df["BOTOL"],
        "unit": df["unit"],
        "team": df["team"],
        "row_hash": df["row_hash"],
        "last_seen_batch": batch_ts,
    })

    # =========================
    # 🔥 FIX FINAL: NUMERIC NaN
    # =========================
    df_dw["entry_year"] = pd.to_numeric(df_dw["entry_year"], errors="coerce").fillna(0).astype(int)
    df_dw["row_no"] = pd.to_numeric(df_dw["row_no"], errors="coerce").fillna(0).astype(int)
    df_dw["quantity"] = pd.to_numeric(df_dw["quantity"], errors="coerce").fillna(0).astype(int)

    df_dw["standard_price"] = (
        df_dw["standard_price"]
        .astype(str)
        .str.replace("Rp", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", "", regex=False)
    )
    df_dw["standard_price"] = pd.to_numeric(df_dw["standard_price"], errors="coerce").fillna(0).astype(int)

    # non-numeric → NULL
    df_dw = df_dw.where(pd.notna(df_dw), None)

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        future=True
    )

    with engine.begin() as conn:

        # =========================
        # DATA EXISTING
        # =========================
        db_existing = pd.read_sql(
            f"SELECT lead_id, row_hash FROM {TABLE_NAME}",
            conn
        )

        # =========================
        # INSERT BARU
        # =========================
        df_new = df_dw[~df_dw["lead_id"].isin(db_existing["lead_id"])]

        if not df_new.empty:
            df_new.to_sql(TABLE_NAME, conn, if_exists="append", index=False)

        print(f"➕ Insert baru: {len(df_new)}")

        # =========================
        # UPDATE (CDC)
        # =========================
        df_compare = df_dw.merge(
            db_existing,
            on="lead_id",
            how="inner",
            suffixes=("", "_db")
        )

        if FORCE_DATE_MIGRATION:
            df_update = df_compare
        else:
            df_update = df_compare[df_compare["row_hash"] != df_compare["row_hash_db"]]

        update_sql = text(f"""
            UPDATE {TABLE_NAME}
            SET
                row_no          = :row_no,
                lead_name       = :lead_name,
                phone_number    = :phone_number,
                whatsapp_link   = :whatsapp_link,
                unit            = :unit,
                team            = :team,
                lead_type       = :lead_type,
                product_name    = :product_name,
                response        = :response,
                description     = :description,
                standard_price  = :standard_price,
                quantity        = :quantity,
                entry_date      = :entry_date,
                input_date      = :input_date,
                entry_year      = :entry_year,
                entry_month     = :entry_month,
                row_hash        = :row_hash,
                last_seen_batch = :batch
            WHERE lead_id = :lead_id
        """)

        updated = 0
        for _, r in df_update.iterrows():
            res = conn.execute(update_sql, {
                "row_no": r["row_no"],
                "lead_name": r["lead_name"],
                "phone_number": r["phone_number"],
                "whatsapp_link": r["whatsapp_link"],
                "unit": r["unit"],
                "team": r["team"],
                "lead_type": r["lead_type"],
                "product_name": r["product_name"],
                "response": r["response"],
                "description": r["description"],
                "standard_price": r["standard_price"],
                "quantity": r["quantity"],
                "entry_date": r["entry_date"],
                "input_date": r["input_date"],
                "entry_year": r["entry_year"],
                "entry_month": r["entry_month"],
                "row_hash": r["row_hash"],
                "batch": batch_ts,
                "lead_id": r["lead_id"],
            })
            updated += res.rowcount

        print(f"🔁 Rows updated: {updated}")

        # =========================
        # HARD DELETE
        # =========================
        conn.execute(
            text(f"DELETE FROM {TABLE_NAME} WHERE last_seen_batch < :b"),
            {"b": batch_ts}
        )

        print("🗑️ Hard delete selesai")

    print("✅ ETL SELESAI – DATABASE TERSINKRON")
