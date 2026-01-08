import gspread
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os

# =========================
# CONFIG
# =========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

SHEET_ID_Q4 = "1SqTKPTf6sYVOPjDSrCq-QQMxd2gf-WAoM3fpk33smG8"
WORKSHEET_Q4 = 0

SHEET_ID_ETA = "1vzokc_TlXdPga3QospR9C8a4YfTp4P39v2L_8Z99I7g"
WORKSHEET_ETA = 0


# =========================
# AUTH
# =========================
def get_gspread_client():
    creds = None

    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials/client_secret.json",
                SCOPES,
            )
            creds = flow.run_local_server(port=0)

        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    return gspread.authorize(creds)


# =========================
# SAFE GOOGLE SHEET READER
# =========================
def read_google_sheet(sheet_id, worksheet_index=0):
    client = get_gspread_client()
    sh = client.open_by_key(sheet_id)
    ws = sh.get_worksheet(worksheet_index)

    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    # 🔍 Cari header "No"
    header_row = None
    for i, row in enumerate(values):
        if row and row[0].strip().lower() == "no":
            header_row = i
            break

    if header_row is None:
        raise Exception("Header dengan kolom 'No' tidak ditemukan")

    headers = [h.strip() for h in values[header_row]]
    rows = values[header_row + 1 :]

    df = pd.DataFrame(rows, columns=headers)

    # =========================
    # NORMALISASI AWAL (PENTING)
    # =========================
    df.columns = df.columns.astype(str).str.strip()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Hapus kolom kosong & duplikat
    df = df.loc[:, df.columns.notna()]
    df = df.loc[:, ~df.columns.duplicated()]

    return df


# =========================
# EXTRACT ALL
# =========================
def extract_all():
    df_q4 = pd.DataFrame()
    df_eta = pd.DataFrame()

    try:
        df_q4 = read_google_sheet(SHEET_ID_Q4, WORKSHEET_Q4)
        print("Q4 rows:", len(df_q4))
    except Exception as e:
        print("❌ Gagal membaca Q4 sheet:", e)

    try:
        df_eta = read_google_sheet(SHEET_ID_ETA, WORKSHEET_ETA)
        print("ETA rows:", len(df_eta))
    except Exception as e:
        print("⚠ ETA sheet tidak terbaca / kosong:", e)

    return df_q4, df_eta


# =========================
# MANUAL TEST
# =========================
if __name__ == "__main__":
    df_q4, df_eta = extract_all()

    print("\n=== Q4 ===")
    print("Rows:", len(df_q4))
    print("Columns:", df_q4.columns.tolist())
    print(df_q4[["No", "Tanggal Masuk", "Tanggal Input"]].head())

    print("\n=== ETALLAGEN ===")
    print("Rows:", len(df_eta))
    print("Columns:", df_eta.columns.tolist())
    print(df_eta[["No", "Tanggal Masuk", "Tanggal Input"]].head())

    print("DEBUG Q4 SHAPE:", df_q4.shape)
