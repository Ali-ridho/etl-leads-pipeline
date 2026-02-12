import gspread
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

flow = InstalledAppFlow.from_client_secrets_file(
    "credentials/oauth_client.json",
    SCOPES
)
creds = flow.run_local_server(port=0)

client = gspread.authorize(creds)

# === TEST Q4 ===
sheet_q4 = client.open_by_key(
    "1SqTKPTf6sYVOPjDSrCq-QQMxd2gf-WAoM3fpk33smG8"
)
ws_q4 = sheet_q4.sheet1
print("Q4 Title:", sheet_q4.title)
print("Q4 Rows:", ws_q4.row_count)

# === TEST ETALLAGEN ===
sheet_eta = client.open_by_key(
    "1vzokc_TlXdPga3QospR9C8a4YfTp4P39v2L_8Z99I7g"
)
ws_eta = sheet_eta.sheet1
print("ETA Title:", sheet_eta.title)
print("ETA Rows:", ws_eta.row_count)



from sqlalchemy import create_engine

engine = create_engine(DB_CONNECTION)

df_final = get_final_df()

print("INSERTING:", len(df_final), "rows")

df_final.to_sql(
    "leads_master",
    engine,
    if_exists="append",
    index=False,
    chunksize=500
)

print("✅ INSERT SELESAI")
