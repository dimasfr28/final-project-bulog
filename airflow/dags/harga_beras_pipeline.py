from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
import requests
from io import BytesIO
from dotenv import load_dotenv
from supabase import create_client

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

load_dotenv()

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ============================================================
# KONSTANTA
# ============================================================
PROVINCE_ID = 15          # Jawa Timur
MAX_RETRY = 10
DELAY_BETWEEN_REQUEST = 2  # detik

KOTA_JATIM = [
    {"kode_kab_kota": 3501, "nama_kab_kota": "Kab. Pacitan"},
    {"kode_kab_kota": 3502, "nama_kab_kota": "Kab. Ponorogo"},
    {"kode_kab_kota": 3503, "nama_kab_kota": "Kab. Trenggalek"},
    {"kode_kab_kota": 3504, "nama_kab_kota": "Kab. Tulungagung"},
    {"kode_kab_kota": 3505, "nama_kab_kota": "Kab. Blitar"},
    {"kode_kab_kota": 3506, "nama_kab_kota": "Kab. Kediri"},
    {"kode_kab_kota": 3507, "nama_kab_kota": "Kab. Malang"},
    {"kode_kab_kota": 3508, "nama_kab_kota": "Kab. Lumajang"},
    {"kode_kab_kota": 3509, "nama_kab_kota": "Kab. Jember"},
    {"kode_kab_kota": 3510, "nama_kab_kota": "Kab. Banyuwangi"},
    {"kode_kab_kota": 3511, "nama_kab_kota": "Kab. Bondowoso"},
    {"kode_kab_kota": 3512, "nama_kab_kota": "Kab. Situbondo"},
    {"kode_kab_kota": 3513, "nama_kab_kota": "Kab. Probolinggo"},
    {"kode_kab_kota": 3514, "nama_kab_kota": "Kab. Pasuruan"},
    {"kode_kab_kota": 3515, "nama_kab_kota": "Kab. Sidoarjo"},
    {"kode_kab_kota": 3516, "nama_kab_kota": "Kab. Mojokerto"},
    {"kode_kab_kota": 3517, "nama_kab_kota": "Kab. Jombang"},
    {"kode_kab_kota": 3518, "nama_kab_kota": "Kab. Nganjuk"},
    {"kode_kab_kota": 3519, "nama_kab_kota": "Kab. Madiun"},
    {"kode_kab_kota": 3520, "nama_kab_kota": "Kab. Magetan"},
    {"kode_kab_kota": 3521, "nama_kab_kota": "Kab. Ngawi"},
    {"kode_kab_kota": 3522, "nama_kab_kota": "Kab. Bojonegoro"},
    {"kode_kab_kota": 3523, "nama_kab_kota": "Kab. Tuban"},
    {"kode_kab_kota": 3524, "nama_kab_kota": "Kab. Lamongan"},
    {"kode_kab_kota": 3525, "nama_kab_kota": "Kab. Gresik"},
    {"kode_kab_kota": 3526, "nama_kab_kota": "Kab. Bangkalan"},
    {"kode_kab_kota": 3527, "nama_kab_kota": "Kab. Sampang"},
    {"kode_kab_kota": 3528, "nama_kab_kota": "Kab. Pamekasan"},
    {"kode_kab_kota": 3529, "nama_kab_kota": "Kab. Sumenep"},
    {"kode_kab_kota": 3571, "nama_kab_kota": "Kota Kediri"},
    {"kode_kab_kota": 3572, "nama_kab_kota": "Kota Blitar"},
    {"kode_kab_kota": 3573, "nama_kab_kota": "Kota Malang"},
    {"kode_kab_kota": 3574, "nama_kab_kota": "Kota Probolinggo"},
    {"kode_kab_kota": 3575, "nama_kab_kota": "Kota Pasuruan"},
    {"kode_kab_kota": 3576, "nama_kab_kota": "Kota Mojokerto"},
    {"kode_kab_kota": 3577, "nama_kab_kota": "Kota Madiun"},
    {"kode_kab_kota": 3578, "nama_kab_kota": "Kota Surabaya"},
    {"kode_kab_kota": 3579, "nama_kab_kota": "Kota Batu"},
]

MAPPING_KOTA = {item["nama_kab_kota"]: item["kode_kab_kota"] for item in KOTA_JATIM}
MAPPING_KOTA["Jawa Timur"] = 1


# ============================================================
# FUNGSI ETL (TASK 2)
# ============================================================

def deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        mask = cols == dup
        cols[mask] = [
            f"{dup}_{i}" if i != 0 else dup
            for i, _ in enumerate(mask[mask].index)
        ]
    df.columns = cols
    return df


def remove_null(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[
        pd.to_numeric(df["harga"].replace("-", None), errors="coerce")
        .pipe(lambda s: s.notna() & (s != 0))
    ]
    return df.reset_index(drop=True)


def encoding(df: pd.DataFrame) -> pd.DataFrame:
    df["kode_kab_kota"] = df["kota"].map(MAPPING_KOTA)

    if df["kode_kab_kota"].isna().any():
        missing = df[df["kode_kab_kota"].isna()]["kota"].unique()
        raise ValueError(f"Kota tidak ter-mapping: {missing}")

    df["variant_clean"] = df["variant"].str.lower()
    conditions = [
        df["variant_clean"] == "beras_medium",
        df["variant_clean"] == "beras_premium",
        df["harga"].isna(),
    ]
    choices = [1, 2, np.nan]
    df["variant_id"] = np.select(conditions, choices, default=99)

    df = remove_null(df)
    return df[["kode_kab_kota", "tanggal", "variant_id", "harga", "tipe_harga_id"]]


def clean_bapanas(df: pd.DataFrame, tipe_harga: str, tanggal: str) -> pd.DataFrame:
    df = df[df["tipe"] == tipe_harga]
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["tanggal"] = tanggal

    if tipe_harga == "produsen":
        kolom_beras = ["Beras Medium Penggilingan", "Beras Premium Penggilingan"]
        df["tipe_harga_id"] = 3
    elif tipe_harga == "konsumen":
        kolom_beras = ["Beras Medium", "Beras Premium"]
        df["tipe_harga_id"] = 2
    else:
        raise ValueError(f"Tipe tidak dikenali: {tipe_harga}")

    kolom_pakai = kolom_beras + ["No", "Kota/Kabupaten", "tanggal", "tipe_harga_id"]
    df = df[kolom_pakai]

    df.columns = (
        df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    )
    df = df.rename(columns={"kota/kabupaten": "kota"})

    rename_map = {col: "beras_medium" for col in df.columns if "medium" in col}
    rename_map.update({col: "beras_premium" for col in df.columns if "premium" in col})
    df = df.rename(columns=rename_map)

    df["no"] = pd.to_numeric(df["no"], errors="coerce")
    df = df[df["no"].notna()].copy()
    df["no"] = df["no"].astype(int)

    kolom_komoditas = ["beras_medium", "beras_premium"]
    for col in kolom_komoditas:
        df[col] = pd.to_numeric(df[col], errors="coerce").replace(0, pd.NA)

    rerata = df[kolom_komoditas].mean()
    new_row = {
        "no": df["no"].max() + 1,
        "kota": "Jawa Timur",
        "tanggal": df["tanggal"].iloc[0],
        "tipe_harga_id": df["tipe_harga_id"].iloc[0],
    }
    for col in kolom_komoditas:
        new_row[col] = rerata[col]

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df = df[["kota", "tanggal", "tipe_harga_id"] + kolom_komoditas]

    df = df.melt(
        id_vars=["kota", "tanggal", "tipe_harga_id"],
        value_vars=kolom_komoditas,
        var_name="variant",
        value_name="harga",
    )

    df = encoding(df)
    df["tanggal"] = pd.to_datetime(df["tanggal"]).dt.strftime("%Y-%m-%d")
    df["harga"] = df["harga"].round().astype("Int64")
    return df


def clean_sp2kp(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["tipe_harga_id"] = 1
    df["variant"] = (
        df["variant"].str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    )
    df["tanggal"] = df["date"]
    df = encoding(df)
    df = remove_null(df)
    return df[["kode_kab_kota", "tanggal", "variant_id", "harga", "tipe_harga_id"]]


# ============================================================
# FUNGSI API BAPANAS
# ============================================================

def get_bapanas_dataframe(
    tanggal, level_harga_id: int = 3, province_id: int = PROVINCE_ID
) -> tuple[bool, pd.DataFrame, str]:
    if isinstance(tanggal, str):
        tanggal = datetime.strptime(tanggal, "%Y-%m-%d").date()

    formatted = tanggal.strftime("%d/%m/%Y")
    period_param = f"{formatted}%20-%20{formatted}"
    url = (
        f"https://api-panelhargav2.badanpangan.go.id/harga-pangan-table-province/export"
        f"?province_id={province_id}&period_date={period_param}&level_harga_id={level_harga_id}"
    )

    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            df = pd.read_excel(BytesIO(response.content))
            if df.empty:
                return False, df, "Data kosong"
            return True, df, f"Berhasil {len(df)} baris"
        return False, pd.DataFrame(), f"HTTP {response.status_code}"
    except requests.exceptions.Timeout:
        return False, pd.DataFrame(), "Timeout"
    except Exception as e:
        return False, pd.DataFrame(), str(e)


def get_bapanas_konsumen_produsen(
    tanggal,
) -> tuple[bool, pd.DataFrame, pd.DataFrame, str]:
    success_k, df_k, msg_k = get_bapanas_dataframe(tanggal, level_harga_id=3)
    success_p, df_p, msg_p = get_bapanas_dataframe(tanggal, level_harga_id=1)

    if success_k and success_p:
        return True, df_k, df_p, "OK"
    elif success_k:
        return True, df_k, pd.DataFrame(), f"Hanya konsumen. Produsen: {msg_p}"
    elif success_p:
        return True, pd.DataFrame(), df_p, f"Hanya produsen. Konsumen: {msg_k}"
    return False, pd.DataFrame(), pd.DataFrame(), f"Keduanya gagal. K:{msg_k} P:{msg_p}"


# ============================================================
# PIPELINE TASKS
# ============================================================

def task_get_last_date(**context) -> None:
    """Task 1: Cek tanggal terakhir per tipe_harga_id dan tentukan apa yang perlu dijalankan.

    tipe_harga_id:
        1 = SP2KP (pasar)
        2 = BAPANAS konsumen
        3 = BAPANAS produsen

    Push XCom:
        - schedule_info: dict berisi per tipe apakah perlu dijalankan dan rentang tanggalnya
        - run_sp2kp: bool
        - run_konsumen: bool
        - run_produsen: bool
    """
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    response = supabase.rpc("get_tanggal_terakhir_per_tipe", {}).execute()
    data = response.data

    today = date.today()
    ti = context["ti"]

    # Bangun mapping tipe_harga_id -> tanggal_terakhir dari response RPC
    # Response berupa list of dict: [{"tipe_harga_id": ..., "tanggal_terakhir": ...}, ...]
    tipe_map = {
        int(row["tipe_harga_id"]): row["tanggal_terakhir"]
        for row in data
        if row.get("tipe_harga_id") is not None and row.get("tanggal_terakhir") is not None
    }

    # SP2KP (tipe_harga_id=1) tidak tersedia di hari Sabtu (5) dan Minggu (6)
    hari_ini_is_weekend = today.weekday() in (5, 6)

    schedule_info = {}
    for tipe_id, label in [(1, "sp2kp"), (2, "konsumen"), (3, "produsen")]:
        if tipe_id not in tipe_map:
            logger.warning("tipe_harga_id=%d tidak ditemukan di response RPC", tipe_id)
            schedule_info[label] = {"run": False, "start_date": None, "end_date": None}
            continue

        tanggal_terakhir = pd.to_datetime(tipe_map[tipe_id]).date()
        next_date = tanggal_terakhir + timedelta(days=1)

        # SP2KP: skip jika hari ini weekend (data memang tidak tersedia)
        if label == "sp2kp" and hari_ini_is_weekend:
            logger.info(
                "tipe_harga_id=%d (sp2kp): hari ini %s (weekend) → SKIP (tidak ada data SP2KP)",
                tipe_id, today.strftime("%A"),
            )
            schedule_info[label] = {"run": False, "start_date": None, "end_date": None}
            continue

        if tanggal_terakhir >= today:
            logger.info(
                "tipe_harga_id=%d (%s): tanggal_terakhir=%s >= today=%s → SKIP",
                tipe_id, label, tanggal_terakhir, today,
            )
            schedule_info[label] = {"run": False, "start_date": None, "end_date": None}
        else:
            logger.info(
                "tipe_harga_id=%d (%s): tanggal_terakhir=%s → run %s s/d %s",
                tipe_id, label, tanggal_terakhir, next_date, today,
            )
            schedule_info[label] = {
                "run": True,
                "start_date": str(next_date),
                "end_date": str(today),
            }

    ada_data_baru = any(v["run"] for v in schedule_info.values())

    ti.xcom_push(key="schedule_info",  value=schedule_info)
    ti.xcom_push(key="run_sp2kp",      value=schedule_info["sp2kp"]["run"])
    ti.xcom_push(key="run_konsumen",   value=schedule_info["konsumen"]["run"])
    ti.xcom_push(key="run_produsen",   value=schedule_info["produsen"]["run"])
    ti.xcom_push(key="ada_data_baru",  value=ada_data_baru)

    logger.info("Schedule: %s | ada_data_baru=%s", schedule_info, ada_data_baru)


def task_fetch_bapanas(**context) -> None:
    """Task 3 & 4: Ambil data BAPANAS dan terapkan ETL per tanggal.

    Hanya menjalankan tipe yang belum up-to-date berdasarkan schedule_info dari task_get_last_date.
    tipe_harga_id=2 → konsumen, tipe_harga_id=3 → produsen.
    """
    ti = context["ti"]
    schedule_info = ti.xcom_pull(task_ids="get_last_date", key="schedule_info")

    info_konsumen = schedule_info.get("konsumen", {})
    info_produsen = schedule_info.get("produsen", {})

    all_konsumen = []
    all_produsen = []
    error_log = []

    # Kumpulkan semua tanggal yang perlu diproses (union dari konsumen & produsen)
    # agar satu API call bisa melayani keduanya sekaligus jika rentang tanggalnya sama
    dates_konsumen = set(
        pd.date_range(info_konsumen["start_date"], info_konsumen["end_date"]).strftime("%Y-%m-%d")
    ) if info_konsumen.get("run") else set()

    dates_produsen = set(
        pd.date_range(info_produsen["start_date"], info_produsen["end_date"]).strftime("%Y-%m-%d")
    ) if info_produsen.get("run") else set()

    all_dates = sorted(dates_konsumen | dates_produsen)

    if not all_dates:
        logger.info("BAPANAS: tidak ada tipe yang perlu dijalankan (semua sudah up-to-date)")
        ti.xcom_push(key="df_bapanas_konsumen", value="[]")
        ti.xcom_push(key="df_bapanas_produsen", value="[]")
        ti.xcom_push(key="bapanas_error_log", value=[])
        return

    for tanggal_str in all_dates:
        need_konsumen = tanggal_str in dates_konsumen
        need_produsen = tanggal_str in dates_produsen

        logger.info(
            "Proses BAPANAS: %s | konsumen=%s produsen=%s",
            tanggal_str, need_konsumen, need_produsen,
        )

        MIN_ROWS = 7  # minimum baris data yang dianggap valid per tipe per tanggal

        success = False
        df_konsumen = pd.DataFrame()
        df_produsen = pd.DataFrame()
        message = ""

        for attempt in range(1, MAX_RETRY + 1):
            try:
                # Panggil API sesuai kebutuhan
                if need_konsumen and need_produsen:
                    success, df_konsumen, df_produsen, message = get_bapanas_konsumen_produsen(tanggal_str)
                elif need_konsumen:
                    ok, df_konsumen, msg = get_bapanas_dataframe(tanggal_str, level_harga_id=3)
                    success, message = ok, msg
                elif need_produsen:
                    ok, df_produsen, msg = get_bapanas_dataframe(tanggal_str, level_harga_id=1)
                    success, message = ok, msg

                if not success:
                    logger.warning("Percobaan %d gagal (API error): %s", attempt, message)
                    time.sleep(3)
                    continue

                # Validasi jumlah baris — data dianggap tidak valid jika terlalu sedikit
                konsumen_ok = (not need_konsumen) or (df_konsumen is not None and len(df_konsumen) >= MIN_ROWS)
                produsen_ok = (not need_produsen) or (df_produsen is not None and len(df_produsen) >= MIN_ROWS)

                if konsumen_ok and produsen_ok:
                    break

                # Data terlalu sedikit → anggap belum siap, retry
                krows = len(df_konsumen) if df_konsumen is not None else 0
                prows = len(df_produsen) if df_produsen is not None else 0
                logger.warning(
                    "Percobaan %d: data terlalu sedikit (konsumen=%d, produsen=%d, min=%d) — retry",
                    attempt, krows, prows, MIN_ROWS,
                )
                success = False

            except Exception as e:
                message = str(e)
                logger.error("Error percobaan %d: %s", attempt, message)
            time.sleep(3)

        if not success:
            logger.error("Gagal total untuk %s: %s", tanggal_str, message)
            error_log.append({"tanggal": tanggal_str, "error": message})
            continue

        inserted = False

        if need_konsumen and df_konsumen is not None and not df_konsumen.empty:
            df_konsumen = deduplicate_columns(df_konsumen)
            df_konsumen["tipe"] = "konsumen"
            df_konsumen = clean_bapanas(df_konsumen, "konsumen", tanggal_str)
            if not df_konsumen.empty:
                all_konsumen.append(df_konsumen)
                inserted = True

        if need_produsen and df_produsen is not None and not df_produsen.empty:
            df_produsen = deduplicate_columns(df_produsen)
            df_produsen["tipe"] = "produsen"
            df_produsen = clean_bapanas(df_produsen, "produsen", tanggal_str)
            if not df_produsen.empty:
                all_produsen.append(df_produsen)
                inserted = True

        if not inserted:
            logger.warning("Data kosong setelah ETL: %s", tanggal_str)
            error_log.append({"tanggal": tanggal_str, "error": "Data kosong setelah ETL"})

        time.sleep(DELAY_BETWEEN_REQUEST)

    df_all_konsumen = pd.concat(all_konsumen, ignore_index=True) if all_konsumen else pd.DataFrame()
    df_all_produsen = pd.concat(all_produsen, ignore_index=True) if all_produsen else pd.DataFrame()

    ti.xcom_push(key="df_bapanas_konsumen", value=df_all_konsumen.to_json(orient="records"))
    ti.xcom_push(key="df_bapanas_produsen", value=df_all_produsen.to_json(orient="records"))
    ti.xcom_push(key="bapanas_error_log", value=error_log)

    logger.info(
        "BAPANAS selesai. Konsumen: %d baris | Produsen: %d baris | Error: %d tanggal",
        len(df_all_konsumen), len(df_all_produsen), len(error_log),
    )


def task_fetch_sp2kp(**context) -> None:
    """Task 5: Ambil data SP2KP dari API Kemendag (tipe_harga_id=1).

    Skip jika SP2KP sudah up-to-date (tanggal_terakhir >= hari ini).
    """
    ti = context["ti"]
    schedule_info = ti.xcom_pull(task_ids="get_last_date", key="schedule_info")
    info_sp2kp = schedule_info.get("sp2kp", {})

    if not info_sp2kp.get("run"):
        logger.info("SP2KP sudah up-to-date, skip fetch")
        ti.xcom_push(key="df_sp2kp_raw", value="[]")
        return

    start_date = info_sp2kp["start_date"]
    end_date = info_sp2kp["end_date"]

    url = "https://api-sp2kp.kemendag.go.id/report/api/average-price/export-area-daily-json"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://sp2kp.kemendag.go.id",
        "Referer": "https://sp2kp.kemendag.go.id/",
        "Accept": "application/json",
    }

    all_data = []

    # Level 2: Kab/Kota
    for kota in KOTA_JATIM:
        logger.info("SP2KP: %s", kota["nama_kab_kota"])
        payload = {
            "start_date": start_date,
            "end_date": end_date,
            "level": 2,
            "variant_ids": "52,51",
            "kode_provinsi": 35,
            "kode_kab_kota": kota["kode_kab_kota"],
            "pasar_id": "",
            "skip_sat_sun": "true",
            "tipe_komoditas": "",
        }
        try:
            resp = requests.post(url, headers=headers, data=payload, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                for entry in data.get("data") or []:
                    for harga_entry in entry.get("daftarHarga") or []:
                        row = pd.json_normalize(harga_entry)
                        row["level"] = 2
                        row["kota"] = kota["nama_kab_kota"]
                        row["kode_kab_kota"] = kota["kode_kab_kota"]
                        row["variant_id"] = entry["variant_id"]
                        row["variant"] = entry["variant"]
                        row["tanggal"] = row["date"]
                        all_data.append(row)
        except requests.exceptions.RequestException as e:
            logger.error("Error SP2KP %s: %s", kota["nama_kab_kota"], e)

        time.sleep(0.15)

    # Level 1: Provinsi Jawa Timur
    logger.info("SP2KP: Jawa Timur (provinsi)")
    payload_prov = {
        "start_date": start_date,
        "end_date": end_date,
        "level": 1,
        "variant_ids": "52,51",
        "kode_provinsi": 35,
        "kode_kab_kota": "",
        "pasar_id": "",
        "skip_sat_sun": "true",
        "tipe_komoditas": "",
    }
    try:
        resp = requests.post(url, headers=headers, data=payload_prov, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            for entry in data.get("data") or []:
                for harga_entry in entry.get("daftarHarga") or []:
                    row = pd.json_normalize(harga_entry)
                    row["level"] = 1
                    row["kota"] = "Jawa Timur"
                    row["kode_kab_kota"] = 1
                    row["variant_id"] = entry["variant_id"]
                    row["variant"] = entry["variant"]
                    row["tanggal"] = row["date"]
                    all_data.append(row)
    except requests.exceptions.RequestException as e:
        logger.error("Error SP2KP provinsi: %s", e)

    if all_data:
        sp2kp_df = pd.concat(all_data, ignore_index=True)
        sp2kp_df.drop_duplicates(inplace=True)
        logger.info("SP2KP raw: %d baris", len(sp2kp_df))
    else:
        sp2kp_df = pd.DataFrame()
        logger.warning("SP2KP: tidak ada data")

    ti.xcom_push(key="df_sp2kp_raw", value=sp2kp_df.to_json(orient="records"))


def task_etl_sp2kp(**context) -> None:
    """Task 6: ETL data SP2KP."""
    from io import StringIO
    ti = context["ti"]
    raw_json = ti.xcom_pull(task_ids="fetch_sp2kp", key="df_sp2kp_raw")
    sp2kp_df = pd.read_json(StringIO(raw_json), orient="records", convert_dates=False)

    if sp2kp_df.empty:
        logger.info("SP2KP raw kosong, skip ETL")
        ti.xcom_push(key="df_sp2kp_clean", value="[]")
        return

    sp2kp_clean = clean_sp2kp(sp2kp_df)
    logger.info("SP2KP setelah ETL: %d baris", len(sp2kp_clean))
    ti.xcom_push(key="df_sp2kp_clean", value=sp2kp_clean.to_json(orient="records"))


def task_concat_data(**context) -> None:
    """Task 7: Gabungkan data BAPANAS (konsumen+produsen) dan SP2KP.

    need_retry=True jika ada tipe yang seharusnya punya data tapi kosong.
    SP2KP dikecualikan dari retry jika hari ini Sabtu/Minggu.
    """
    from io import StringIO

    ti = context["ti"]
    schedule_info = ti.xcom_pull(task_ids="get_last_date", key="schedule_info")
    today = date.today()
    hari_ini_is_weekend = today.weekday() in (5, 6)

    konsumen_json = ti.xcom_pull(task_ids="fetch_bapanas", key="df_bapanas_konsumen")
    produsen_json = ti.xcom_pull(task_ids="fetch_bapanas", key="df_bapanas_produsen")
    sp2kp_json    = ti.xcom_pull(task_ids="etl_sp2kp",    key="df_sp2kp_clean")

    df_konsumen = pd.read_json(StringIO(konsumen_json), orient="records", convert_dates=False)
    df_produsen = pd.read_json(StringIO(produsen_json), orient="records", convert_dates=False)
    df_sp2kp    = pd.read_json(StringIO(sp2kp_json),   orient="records", convert_dates=False)

    # Deteksi tipe yang seharusnya ada data tapi kosong
    need_retry = False
    if schedule_info.get("konsumen", {}).get("run") and df_konsumen.empty:
        logger.warning("KONSUMEN: run=True tapi DataFrame kosong → perlu retry")
        need_retry = True
    if schedule_info.get("produsen", {}).get("run") and df_produsen.empty:
        logger.warning("PRODUSEN: run=True tapi DataFrame kosong → perlu retry")
        need_retry = True
    if schedule_info.get("sp2kp", {}).get("run") and df_sp2kp.empty and not hari_ini_is_weekend:
        logger.warning("SP2KP: run=True, bukan weekend, tapi DataFrame kosong → perlu retry")
        need_retry = True

    frames = []
    for name, df in [("konsumen", df_konsumen), ("produsen", df_produsen), ("sp2kp", df_sp2kp)]:
        if not df.empty:
            frames.append(df)
            logger.info("%s: %d baris", name, len(df))

    if not frames:
        logger.info("Semua DataFrame kosong. need_retry=%s", need_retry)
        ti.xcom_push(key="df_final",   value="[]")
        ti.xcom_push(key="need_retry", value=need_retry)
        return

    df_final = pd.concat(frames, ignore_index=True)

    # Normalisasi tipe kolom
    cols_int = ["kode_kab_kota", "variant_id", "tipe_harga_id"]
    for col in cols_int:
        df_final[col] = pd.array(df_final[col], dtype="Int64")

    # Kolom tanggal bisa berupa string "YYYY-MM-DD" atau integer ms (dari XCom lama)
    def parse_tanggal(val):
        if isinstance(val, (int, float)):
            return pd.Timestamp(val, unit="ms").strftime("%Y-%m-%d")
        return str(val)[:10]

    df_final["tanggal"] = df_final["tanggal"].apply(parse_tanggal)
    df_final["harga"] = df_final["harga"].apply(lambda x: int(x) if pd.notna(x) else None)

    for col in cols_int:
        df_final[col] = df_final[col].apply(lambda x: int(x) if pd.notna(x) else None)

    logger.info("Total final: %d baris | need_retry=%s", len(df_final), need_retry)
    ti.xcom_push(key="df_final",   value=df_final.to_json(orient="records"))
    ti.xcom_push(key="need_retry", value=need_retry)


def task_load_to_supabase(**context) -> None:
    """Task 8: Insert data final ke Supabase."""
    ti = context["ti"]
    from io import StringIO
    final_json = ti.xcom_pull(task_ids="concat_data", key="df_final")
    df_final = pd.read_json(StringIO(final_json), orient="records", convert_dates=False)

    if df_final.empty:
        logger.warning("Data final kosong, tidak ada yang diinsert")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    records = df_final.to_dict(orient="records")
    batch_size = 1000
    total_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        response = supabase.table("harga_beras").insert(batch).execute()
        if response.data is None:
            raise RuntimeError(f"Insert gagal pada batch {i}: {response}")
        total_inserted += len(batch)
        logger.info("Inserted %d / %d baris", total_inserted, len(records))

    logger.info("Load ke Supabase selesai. Total: %d baris", total_inserted)


RETRY_DELAY_MINUTES = 15  # jeda sebelum Airflow retry otomatis


def _cek_uptodate_dari_db(schedule_info: dict) -> bool:
    """Cek ulang ke Supabase apakah semua tipe yang dijadwalkan sudah up-to-date.

    Membandingkan tanggal_terakhir di DB terhadap end_date masing-masing tipe
    dari schedule_info (bukan date.today()), agar benar untuk catchup run.

    Returns:
        True  → semua tipe yang run=True sudah up-to-date
        False → masih ada tipe yang belum up-to-date
    """
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    response = supabase.rpc("get_tanggal_terakhir_per_tipe", {}).execute()
    data = response.data

    tipe_map = {
        int(row["tipe_harga_id"]): row["tanggal_terakhir"]
        for row in data
        if row.get("tipe_harga_id") is not None and row.get("tanggal_terakhir") is not None
    }

    for tipe_id, label in [(1, "sp2kp"), (2, "konsumen"), (3, "produsen")]:
        info = schedule_info.get(label, {})
        # Hanya cek tipe yang memang dijadwalkan untuk run
        if not info.get("run"):
            logger.info("_cek_uptodate: tipe=%s run=False → skip", label)
            continue

        target = pd.to_datetime(info["end_date"]).date()

        if tipe_id not in tipe_map:
            logger.warning("_cek_uptodate: tipe_harga_id=%d (%s) tidak ada di DB", tipe_id, label)
            return False

        tanggal_terakhir = pd.to_datetime(tipe_map[tipe_id]).date()
        if tanggal_terakhir < target:
            logger.info(
                "_cek_uptodate: tipe=%s masih kurang — tanggal_terakhir=%s < target=%s",
                label, tanggal_terakhir, target,
            )
            return False

    logger.info("_cek_uptodate: semua tipe yang dijadwalkan sudah up-to-date")
    return True


def task_wait_and_retry(**context) -> None:
    """Task 9: Cek ulang DB apakah data sudah masuk. Jika belum, tunggu lalu raise
    agar Airflow me-retry. Jika sudah up-to-date, selesai normal.

    Alur per attempt:
      1. Cek DB langsung (bukan dari XCom) → jika sudah up-to-date, SUCCESS
      2. Jika belum → tunggu RETRY_DELAY_MINUTES menit
      3. Raise RuntimeError → Airflow retry task ini dari awal (kembali ke step 1)
    """
    ti = context["ti"]
    need_retry = ti.xcom_pull(task_ids="concat_data", key="need_retry")
    schedule_info = ti.xcom_pull(task_ids="get_last_date", key="schedule_info")

    if not need_retry:
        logger.info("need_retry=False — pipeline selesai normal tanpa perlu retry.")
        return

    # Cek ulang DB: apakah data sudah masuk setelah insert (atau run sebelumnya)?
    logger.info("Mengecek status terkini dari database Supabase...")
    if _cek_uptodate_dari_db(schedule_info):
        logger.info("Semua tipe sudah up-to-date di DB. Pipeline selesai.")
        return

    logger.warning(
        "DB belum up-to-date. Menunggu %d menit sebelum Airflow retry task ini...",
        RETRY_DELAY_MINUTES,
    )
    time.sleep(RETRY_DELAY_MINUTES * 60)

    raise RuntimeError(
        f"DB masih belum up-to-date setelah menunggu {RETRY_DELAY_MINUTES} menit. "
        "Airflow akan me-retry task ini (cek ulang DB di awal setiap retry)."
    )


# ============================================================
# DAG DEFINITION
# ============================================================

default_args = {
    "owner": "bulog-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="harga_beras_pipeline",
    description="Pipeline ETL harian: BAPANAS + SP2KP → Supabase",
    default_args=default_args,
    schedule="0 14 * * *",   # 08:00 WIB, Senin-MInggu
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bulog", "etl", "beras", "bapanas", "sp2kp"],
) as dag:

    t1_get_last_date = PythonOperator(
        task_id="get_last_date",
        python_callable=task_get_last_date,
        doc_md="**Task 1** — Cari tanggal terakhir di Supabase via RPC `get_tanggal_terakhir_per_tipe`.",
    )

    t2_declare_etl = PythonOperator(
        task_id="declare_etl_functions",
        python_callable=lambda **_: logger.info(
            "Fungsi ETL (clean_bapanas, clean_sp2kp, encoding) sudah di-load pada import module."
        ),
        doc_md="**Task 2** — Deklarasi fungsi ETL (sudah di-load saat import, task ini sebagai checkpoint).",
    )

    t3_t4_fetch_bapanas = PythonOperator(
        task_id="fetch_bapanas",
        python_callable=task_fetch_bapanas,
        doc_md="**Task 3 & 4** — Ambil data BAPANAS per tanggal (konsumen & produsen) lalu terapkan ETL.",
        execution_timeout=timedelta(hours=2),
    )

    t5_fetch_sp2kp = PythonOperator(
        task_id="fetch_sp2kp",
        python_callable=task_fetch_sp2kp,
        doc_md="**Task 5** — Ambil data SP2KP dari API Kemendag untuk semua kab/kota Jawa Timur.",
        execution_timeout=timedelta(hours=1),
    )

    t6_etl_sp2kp = PythonOperator(
        task_id="etl_sp2kp",
        python_callable=task_etl_sp2kp,
        doc_md="**Task 6** — Bersihkan dan normalisasi data SP2KP (clean_sp2kp).",
    )

    t7_concat = PythonOperator(
        task_id="concat_data",
        python_callable=task_concat_data,
        doc_md="**Task 7** — Gabungkan DataFrame BAPANAS konsumen, produsen, dan SP2KP.",
    )

    t8_load = PythonOperator(
        task_id="load_to_supabase",
        python_callable=task_load_to_supabase,
        doc_md="**Task 8** — Insert data final ke tabel `harga_beras` di Supabase (batch 1000).",
    )

    t9_wait_retry = PythonOperator(
        task_id="wait_and_retry",
        python_callable=task_wait_and_retry,
        retries=20,
        retry_delay=timedelta(seconds=10),  # delay singkat — sleep sudah dilakukan di dalam fungsi
        doc_md=(
            f"**Task 9** — Jika data kosong padahal ada tipe yang belum up-to-date, "
            f"tunggu {RETRY_DELAY_MINUTES} menit lalu raise exception agar Airflow retry otomatis (maks 20x). "
            f"Jika semua sudah up-to-date, selesai normal."
        ),
    )

    t10_trigger_outlier = TriggerDagRunOperator(
        task_id="trigger_outlier_pipeline",
        trigger_dag_id="outlier_pipeline",
        wait_for_completion=False,   # tidak menunggu outlier_pipeline selesai
        doc_md="**Task 10** — Trigger `outlier_pipeline` setelah semua data harga beras berhasil diproses.",
    )

    t11_trigger_forecast = TriggerDagRunOperator(
        task_id="trigger_forecast_pipeline",
        trigger_dag_id="forecast_prediction_pipeline",
        wait_for_completion=False,   # tidak menunggu forecast_prediction_pipeline selesai
        doc_md="**Task 11** — Trigger `forecast_prediction_pipeline` setelah semua data harga beras berhasil diproses.",
    )

    # ============================================================
    # DEPENDENCY CHAIN
    #
    #  t1_get_last_date
    #       │
    #  t2_declare_etl
    #       │
    #  ┌────┴────────────────┐
    #  t3_t4_bapanas   t5_fetch_sp2kp
    #                       │
    #                  t6_etl_sp2kp
    #  └────────┬───────────┘
    #       t7_concat
    #           │
    #       t8_load
    #           │
    #  t9_wait_retry  ← retry otomatis jika API belum tersedia
    #       ┌───┴───────────────────────┐
    #  t10_trigger_outlier    t11_trigger_forecast
    # ============================================================

    t1_get_last_date >> t2_declare_etl >> [t3_t4_fetch_bapanas, t5_fetch_sp2kp]
    t5_fetch_sp2kp >> t6_etl_sp2kp
    [t3_t4_fetch_bapanas, t6_etl_sp2kp] >> t7_concat >> t8_load >> t9_wait_retry >> [t10_trigger_outlier, t11_trigger_forecast]
