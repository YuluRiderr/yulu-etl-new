"""
Yulu ETL v2 — Sweep_Raw / Bikes_WHS / To_Be_Moved
Fetches from Metabase (and one external form-response sheet), and writes
into a master Google Sheet — WITHOUT disturbing any manually-entered
columns that already live in those tabs.

KEY DESIGN DIFFERENCE vs. the original script:
  Instead of hardcoding a {column_name: "A"} map for every tab, this
  version reads row 1 (the header) of the destination tab itself and
  matches columns BY NAME. Only columns that exist in both (a) the
  fetched data and (b) the sheet's header row get cleared/written.
  Anything else in the tab — extra manual columns, formulas, notes,
  whatever — is left completely untouched, no matter where it sits.
  This is the same "contiguous-run" clear/write logic as the original
  script's update_named_columns(), just with the mapping auto-derived
  instead of hand-typed, so it works the same way for every new tab
  you add later without needing a new manual column map each time.

Everything below is a clean overwrite (clear + write), never an append.

  *** TODO before running ***
  - Set MASTER_SHEET_ID to the Google Sheet holding Sweep_Raw / Bikes_WHS
    / To_Be_Moved.
  - Confirm CARD_ID_SWEEP_RAW is the right Metabase card for the raw
    Sweep export (assumed same as the existing Sweep card, 654).
  - Confirm TO_BE_MOVED_SOURCE_GID is the right worksheet gid on the
    external form-response sheet.
"""

import io
import os

import pandas as pd
import requests
import gspread

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
METABASE_URL      = os.environ["METABASE_URL"].rstrip("/")
METABASE_EMAIL    = os.environ["METABASE_EMAIL"]
METABASE_PASSWORD = os.environ["METABASE_PASSWORD"]

# Master sheet holding Sweep_Raw / Bikes_WHS / To_Be_Moved
MASTER_SHEET_ID = "1fuCo3fSY0KoW6Y2UQSgtGOBiVIocD_iFEyLdAlOMAr0"

CITY = "BLR"

# TODO: confirm this is the right Metabase card for the raw sweep export.
# Assumed to be the same underlying question as the existing Sweep card
# (654), just read here without dropping any columns down to a curated
# subset — every column Metabase returns for it is a candidate for
# Sweep_Raw, matched by name against that tab's header row.
CARD_ID_SWEEP_RAW = 654
CARD_ID_WAREHOUSE = 6214

# External Google Form response sheet that feeds "To_Be_Moved"
# NOTE: you have VIEW-only access to this sheet, and the service account
# is a separate Google identity from you — it needs its own access. See
# fetch_to_be_moved_source() below for the two ways this can work.
TO_BE_MOVED_SHEET_ID = "1CVfx-42si7dOhQu3bmhcfc8U3djr1rTx0ESO7zGTZxA"
# TODO: confirm this gid is the tab you want (taken from the link you shared)
TO_BE_MOVED_SOURCE_GID = 1539435491

# Destination tab names in MASTER_SHEET_ID
TAB_SWEEP_RAW    = "Sweep_Raw"
TAB_WAREHOUSE    = "Bikes_WHS"
TAB_TO_BE_MOVED  = "To_Be_Moved"

# ─────────────────────────────────────────────────────────────
# Expected columns per source (used only to select/clean what we fetch —
# the actual write still only touches columns that match the destination
# tab's own header row, per column, so nothing you've typed in a tab that
# ISN'T in this list is ever touched).
# ─────────────────────────────────────────────────────────────
SWEEP_RAW_COLUMNS = [
    "city", "bike", "imei", "is_whs_hard_tag_and_rtd", "current_firmware_version",
    "current_hardware_version", "is_iot_mapped", "is_motor_mapped", "is_mcu_mapped",
    "mcu_pcb_id", "motor_controller_type_id", "mcu_model", "mcu_sw_version_from_device",
    "enable_mcu_sw_update", "bike_category", "bike_group", "ble_id", "current_latitude",
    "current_longitude", "current_location", "source", "gprs_timestamp", "at_warehouse",
    "nearest_yz", "nearest_yz_id", "yz_label", "yz_radius_in_mtrs", "mtrs_from_nearest_yz",
    "inside_yulu_zone", "geozone", "in_red_zone", "most_recent_journey_state", "last_rtd_dt",
    "last_location_ping", "last_battery_ping", "last_location_packet_ping",
    "flag_battery_mre_fixed", "flag_bike_mre_fix", "secondary_bat_v", "mins_since_pnr",
    "battery_percentage", "last_journey_time", "no_of_days_since_rnt", "lock_state",
    "flag_bike_fault", "flag_lock_fault", "flag_relocating", "oz_name", "inside_oz",
    "hrs_since_outside_oz", "fault_reported_by", "fault_type", "fault_reported_dt",
    "falut_marked_since_in_days", "operational_cluster", "nearest_operational_cluster",
    "is_in_oc", "screener_cluster", "is_in_sc", "last_swept_dt", "last_screened_dt",
    "reserved_bike", "sanitized_location", "last_sanitized_date", "corp_company_name",
    "battery_qr_code", "onboard_bat_v", "report_run_time", "last_journey_type",
    "last_swapped_date", "flag_battery_critical", "biker_cluster", "is_in_bkc",
    "version_no", "last_screened_by", "on_pilot_map", "pilot_map_time", "on_biker_map",
    "biker_map_time", "on_fleet_map", "fleet_map_time", "is_test_vehicle", "dte_in_kms",
    "flag_missing", "flag_stolen", "flag_unavailable", "flag_deployed", "flag_assembled",
    "bike_state_id", "is_repair_completed", "repair_completed_dt", "last_whs_in_time",
    "is_battery_connector_faulty", "is_neck_pipe_bracket_welded", "created_dt", "updated_dt",
    "motor_oil_seal_status", "ltr_attach_time", "ltr_detach_time", "bike_corp_name",
    "mtrs_from_nearest_yc", "inside_yulu_centre", "nearest_yc",
]

WAREHOUSE_COLUMNS = [
    "city", "cluster", "yc_name", "bike_name", "category", "version_no",
    "at_warehouse", "whs_in_epoch", "issues", "part_name", "updated_part_name",
]

# Exact header names on the external form-response sheet
TO_BE_MOVED_COLUMNS = [
    "Timestamp", "City", "Bike number", "Bike's chassis number", "Bike category",
    "Bike version", "Reasons for retiral",
    "Identification date (on which date it was identified that bike should be retired)",
    "Identification cluster (in which cluster it was identified that bike should be retired)",
    "Identification workshop (in which workshop it was identified that bike should be retired)",
    "Bike's state id (current)", "Small brief of what happend with the bike (RCA report)",
    "Damaged bike's picture with visible bike number and damaged area",
    "is bike already dismantled?",
    "if the bike is dismantled, pls attach dismantled chassis picture with visible bike number",
    "Column 1", "Remarks",
]


def letter_to_index(letter: str) -> int:
    idx = 0
    for ch in letter:
        idx = idx * 26 + (ord(ch.upper()) - ord("A") + 1)
    return idx


def col_letter(n: int) -> str:
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


# ─────────────────────────────────────────────────────────────
# GOOGLE SHEETS AUTH
# Uses a service account (a separate robot identity, not your personal
# login). It needs to be explicitly shared as a Viewer/Editor on any
# sheet it touches — see setup notes.
# ─────────────────────────────────────────────────────────────
def get_gspread_client() -> gspread.Client:
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(sa_json)
            tmp_path = f.name
        return gspread.service_account(filename=tmp_path)
    return gspread.service_account(filename="service_account.json")


# ─────────────────────────────────────────────────────────────
# METABASE FETCH
# ─────────────────────────────────────────────────────────────
def metabase_session() -> dict:
    resp = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": METABASE_EMAIL, "password": METABASE_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    return {"X-Metabase-Session": resp.json()["id"]}


def fetch_metabase_csv(card_id: int, city: str = None) -> pd.DataFrame:
    headers = metabase_session()

    parameters = []
    if city:
        parameters.append({
            "type":   "text",
            "target": ["variable", ["template-tag", "City"]],
            "value":  city,
        })

    csv_resp = requests.post(
        f"{METABASE_URL}/api/card/{card_id}/query/csv",
        json={"parameters": parameters},
        headers=headers,
        timeout=180,
    )
    csv_resp.raise_for_status()

    df = pd.read_csv(io.StringIO(csv_resp.text), low_memory=False)
    print(f"  [Card {card_id}] city={city or 'ALL'} | {len(df)} rows | cols: {df.columns.tolist()}")
    return df


# ─────────────────────────────────────────────────────────────
# BIKE ID NORMALISATION
# gspread / CSV exports sometimes return bike IDs as "5038508.0" (float)
# or int. This strips the .0 and forces a plain string for reliable joins.
# ─────────────────────────────────────────────────────────────
def normalise_bike_id(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
              .str.strip()
              .str.replace(r"\.0$", "", regex=True)
              .str.replace(r"\s+", "", regex=True)
    )


def clean_for_sheets(df: pd.DataFrame) -> list:
    df = df.copy()
    df = df.replace([float("inf"), float("-inf")], "")
    df = df.where(pd.notnull(df), "")
    str_df = df.astype(str).replace({"nan": "", "NaN": "", "NaT": "", "None": "", "<NA>": ""})
    return str_df.values.tolist()


# ─────────────────────────────────────────────────────────────
# THE GENERIC "DON'T DISTURB OTHER COLUMNS" WRITER
# Used for every tab: Sweep_Raw, Bikes_WHS, To_Be_Moved, and any future
# tab you add. It reads the tab's own header row, matches df columns to
# it BY NAME, and only clears/writes the matched columns — split into
# contiguous letter-runs so a gap (e.g. a manual/formula column sitting
# between two matched columns) is never touched.
# ─────────────────────────────────────────────────────────────
def get_header_map(ws) -> dict:
    header = ws.row_values(1)
    return {
        name.strip(): col_letter(i + 1)
        for i, name in enumerate(header)
        if name.strip()
    }


def update_named_columns_auto(gc: gspread.Client, sheet_id: str, tab: str, df: pd.DataFrame):
    ws = gc.open_by_key(sheet_id).worksheet(tab)
    header_map = get_header_map(ws)

    column_letters = {col: header_map[col] for col in df.columns if col in header_map}
    unmatched = [c for c in df.columns if c not in header_map]
    if unmatched:
        print(f"  '{tab}' → NOTE: these fetched columns have no matching header "
              f"in the sheet, so they are NOT written: {unmatched}")

    if not column_letters:
        print(f"  '{tab}' → no matching columns found in header row, skipping.")
        return

    last_row = max(ws.row_count, 2)

    ordered = sorted(column_letters.items(), key=lambda pair: letter_to_index(pair[1]))
    ordered_cols = [col for col, _ in ordered]
    values_all = clean_for_sheets(df[ordered_cols])
    n_rows = len(values_all)
    if n_rows == 0:
        print(f"  '{tab}' → no rows to write, skipping (nothing cleared).")
        return

    # Split into contiguous column runs so any untouched column in between
    # (manual notes, formulas, etc.) never falls inside a clear/write range.
    groups = [[ordered[0]]]
    for prev, curr in zip(ordered, ordered[1:]):
        if letter_to_index(curr[1]) == letter_to_index(prev[1]) + 1:
            groups[-1].append(curr)
        else:
            groups.append([curr])

    col_offset = 0
    for group in groups:
        width = len(group)
        start_letter = group[0][1]
        end_letter = group[-1][1]
        sub_values = [row[col_offset:col_offset + width] for row in values_all]
        col_offset += width

        clear_range = f"{start_letter}2:{end_letter}{last_row}"
        write_range = f"{start_letter}2:{end_letter}{n_rows + 1}"
        ws.batch_clear([clear_range])
        ws.update(sub_values, write_range, value_input_option="user_entered")
        print(f"  '{tab}' → cleared {clear_range}, wrote {n_rows} rows to {write_range} "
              f"({', '.join(c for c, _ in group)}).")


# ─────────────────────────────────────────────────────────────
# STEP A — SWEEP_RAW
# ─────────────────────────────────────────────────────────────
def process_sweep_raw(gc: gspread.Client):
    print("\n── STEP A: Sweep_Raw ──")
    df = fetch_metabase_csv(CARD_ID_SWEEP_RAW, city=CITY)

    if "bike" in df.columns:
        df["bike"] = normalise_bike_id(df["bike"])

    if "city" in df.columns:
        df = df[df["city"] == CITY]

    keep = [c for c in SWEEP_RAW_COLUMNS if c in df.columns]
    missing = [c for c in SWEEP_RAW_COLUMNS if c not in df.columns]
    if missing:
        print(f"  NOTE: card {CARD_ID_SWEEP_RAW} is missing expected columns: {missing}")
    df = df[keep]

    update_named_columns_auto(gc, MASTER_SHEET_ID, TAB_SWEEP_RAW, df)


# ─────────────────────────────────────────────────────────────
# STEP B — BIKES IN WAREHOUSE (Bikes_WHS)
# ─────────────────────────────────────────────────────────────
def process_warehouse(gc: gspread.Client):
    print("\n── STEP B: Bikes_WHS ──")
    df = fetch_metabase_csv(CARD_ID_WAREHOUSE, city=CITY)

    if "city" in df.columns:
        df = df[df["city"] == CITY]

    keep = [c for c in WAREHOUSE_COLUMNS if c in df.columns]
    missing = [c for c in WAREHOUSE_COLUMNS if c not in df.columns]
    if missing:
        print(f"  NOTE: card {CARD_ID_WAREHOUSE} is missing expected columns: {missing}")
    df = df[keep]

    update_named_columns_auto(gc, MASTER_SHEET_ID, TAB_WAREHOUSE, df)


# ─────────────────────────────────────────────────────────────
# STEP C — TO_BE_MOVED
# Reads the external form-response sheet (by gid, so it survives tab
# renames) and pushes a clean copy into the To_Be_Moved tab of the
# master sheet, matched by header name.
# ─────────────────────────────────────────────────────────────
def fetch_to_be_moved_source(gc: gspread.Client) -> pd.DataFrame:
    """
    Reads the external form-response sheet. You only have VIEW access to
    it, and the service account running this script is a *different*
    Google identity — so there are two ways this can succeed, tried in
    order:

    1. PUBLIC CSV EXPORT (no service-account permission needed at all).
       Works only if the sheet is shared as "Anyone with the link →
       Viewer". If it's restricted to specific people, this request
       comes back as an HTML login page instead of CSV, and we fall
       through to option 2.

    2. SHEETS API via the service account (gspread). Works only if
       someone with edit/owner rights on that sheet has added the
       service account's email (the "client_email" field inside your
       GOOGLE_SERVICE_ACCOUNT_JSON) as a Viewer. Just Viewer — writing
       is never needed for this source.

    If both fail, the error message below tells you exactly which one
    to fix.
    """
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{TO_BE_MOVED_SHEET_ID}"
        f"/export?format=csv&gid={TO_BE_MOVED_SOURCE_GID}"
    )
    try:
        resp = requests.get(export_url, timeout=60)
        resp.raise_for_status()
        # A restricted sheet redirects to an HTML login/permission page
        # instead of returning CSV — this is how we detect that case.
        if "text/csv" not in resp.headers.get("Content-Type", ""):
            raise ValueError("response was not CSV — sheet is likely not link-shared")
        df = pd.read_csv(io.StringIO(resp.text))
        print("  To_Be_Moved source: fetched via public CSV export.")
    except Exception as e:
        print(f"  Public CSV export failed ({e}); falling back to Sheets API "
              f"via the service account…")
        try:
            sh = gc.open_by_key(TO_BE_MOVED_SHEET_ID)
            ws = sh.get_worksheet_by_id(TO_BE_MOVED_SOURCE_GID)
            df = pd.DataFrame(ws.get_all_records())
            print("  To_Be_Moved source: fetched via Sheets API (service account).")
        except gspread.exceptions.APIError as api_err:
            raise RuntimeError(
                "Could not read the To_Be_Moved source sheet either way.\n"
                "  Fix ONE of the following:\n"
                "  (a) Ask the sheet owner to set sharing to "
                "'Anyone with the link -> Viewer', OR\n"
                "  (b) Ask the sheet owner to add this service account's "
                "email (the 'client_email' in your GOOGLE_SERVICE_ACCOUNT_JSON) "
                "as a Viewer on that sheet.\n"
                f"  Underlying error: {api_err}"
            ) from api_err

    if df.empty:
        print("  WARNING: To_Be_Moved source sheet returned no rows.")
        return pd.DataFrame(columns=TO_BE_MOVED_COLUMNS)

    missing = [c for c in TO_BE_MOVED_COLUMNS if c not in df.columns]
    if missing:
        print(f"  NOTE: source sheet is missing expected columns: {missing}")
    keep = [c for c in TO_BE_MOVED_COLUMNS if c in df.columns]
    df = df[keep]

    if "Bike number" in df.columns:
        df["Bike number"] = normalise_bike_id(df["Bike number"])

    # Drop fully-blank rows (e.g. trailing empty form rows)
    df = df[~(df.astype(str).apply(lambda r: r.str.strip()).eq("").all(axis=1))]

    print(f"  To_Be_Moved source: {len(df)} rows fetched.")
    return df


def process_to_be_moved(gc: gspread.Client):
    print("\n── STEP C: To_Be_Moved ──")
    df = fetch_to_be_moved_source(gc)
    update_named_columns_auto(gc, MASTER_SHEET_ID, TAB_TO_BE_MOVED, df)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("Authenticating with Google Sheets…")
    gc = get_gspread_client()

    process_sweep_raw(gc)
    process_warehouse(gc)
    process_to_be_moved(gc)

    print("\n✅ ETL complete.")


if __name__ == "__main__":
    main()
