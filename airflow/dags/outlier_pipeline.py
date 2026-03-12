from __future__ import annotations

import logging
import math
import os
import re
import time
import urllib.parse
from datetime import datetime, timedelta, date
from io import StringIO

import holidays
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

from airflow import DAG
from airflow.operators.python import PythonOperator

load_dotenv()

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CURRENT_YEAR = datetime.now().year


# ============================================================
# FUNGSI HELPER
# ============================================================

def _supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_data_with_pagination(table_name: str, year: int, limit: int = 1000) -> pd.DataFrame:
    supabase = _supabase()
    offset = 0
    all_data = []
    while True:
        resp = (
            supabase.table(table_name)
            .select("*")
            .filter("tanggal", "gte", f"{year}-01-01")
            .filter("tanggal", "lt",  f"{year + 1}-01-01")
            .range(offset, offset + limit - 1)
            .execute()
        )
        if not resp.data:
            break
        all_data.extend(resp.data)
        offset += limit
    return pd.DataFrame(all_data)


# ============================================================
# FUNGSI DETEKSI OUTLIER
# ============================================================

def detect_iqr_outlier(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["is_outlier"] = False
    group_cols = ["tahun", "variant_id", "tipe_harga_id", "kode_kab_kota"]
    for _, g in df.groupby(group_cols):
        q1 = g["harga"].quantile(0.25)
        q3 = g["harga"].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        idx = g[(g["harga"] < lower) | (g["harga"] > upper)].index
        df.loc[idx, "is_outlier"] = True
    return df


# ============================================================
# FUNGSI CLUSTERING OUTLIER
# ============================================================

def cluster_outlier(df: pd.DataFrame, max_hari: int = 14):
    df = df.copy()
    df["tanggal"] = pd.to_datetime(df["tanggal"])
    df["tahun"] = df["tanggal"].dt.year

    out = df[df["is_outlier"] == True].copy()
    if out.empty:
        df["cluster_id"] = pd.NA
        return pd.DataFrame(), df

    out = out.sort_values(["tahun", "variant_id", "tipe_harga_id", "tanggal"])

    cluster_list = []
    for _, group in out.groupby(["tahun", "variant_id", "tipe_harga_id"]):
        group = group.sort_values("tanggal").copy()
        cluster_local = 1
        start_date = group.iloc[0]["tanggal"]
        prev_date = start_date
        clusters = [cluster_local]

        for i in range(1, len(group)):
            current_date = group.iloc[i]["tanggal"]
            selisih = (current_date - prev_date).days
            durasi = (current_date - start_date).days + 1
            if selisih > 1 or durasi > max_hari:
                cluster_local += 1
                start_date = current_date
            clusters.append(cluster_local)
            prev_date = current_date

        group["cluster_local"] = clusters
        cluster_list.append(group)

    out = pd.concat(cluster_list)

    df_out = (
        out.groupby(["tahun", "variant_id", "tipe_harga_id", "cluster_local"])
        .agg(
            start_date=("tanggal", "min"),
            end_date=("tanggal", "max"),
            jumlah_data=("tanggal", "count"),
        )
        .reset_index()
    )
    df_out["durasi_hari"] = (df_out["end_date"] - df_out["start_date"]).dt.days + 1
    df_out = df_out.sort_values(
        ["tahun", "variant_id", "tipe_harga_id", "start_date"]
    ).reset_index(drop=True)
    df_out["cluster_id"] = df_out.index + 1

    out = out.merge(
        df_out[["tahun", "variant_id", "tipe_harga_id", "cluster_local", "cluster_id"]],
        on=["tahun", "variant_id", "tipe_harga_id", "cluster_local"],
        how="left",
    )

    df_full = df.merge(
        out[["kode_kab_kota", "tanggal", "variant_id", "tipe_harga_id", "cluster_id"]],
        on=["kode_kab_kota", "tanggal", "variant_id", "tipe_harga_id"],
        how="left",
    )
    df_full["cluster_id"] = df_full["cluster_id"].astype("Int64")

    return df_out, df_full


# ============================================================
# FUNGSI ANALISIS BERITA & HARI LIBUR
# ============================================================

def get_gnews_by_date(start_date, end_date, language="id", country="ID") -> list[str]:
    from gnews import GNews

    keyword_raw = "harga OR impor beras OR logistik OR pangan OR cuaca ekstrim jawa timur"

    if not isinstance(start_date, tuple):
        start_date = (start_date.year, start_date.month, start_date.day)
    if not isinstance(end_date, tuple):
        end_date = (end_date.year, end_date.month, end_date.day)

    gn = GNews(language=language, country=country)
    gn.start_date = start_date
    gn.end_date = end_date

    news = gn.get_news(urllib.parse.quote(keyword_raw))

    data = []
    for article in news:
        data.append({
            "tanggal": article.get("published date"),
            "judul": article.get("title"),
            "sumber": article.get("publisher", {}).get("title"),
            "url": article.get("url"),
        })
    df_news = pd.DataFrame(data)
    return _df_to_sentences(df_news)


def _df_to_sentences(df: pd.DataFrame) -> list[str]:
    sentences = []
    for _, row in df.iterrows():
        kalimat = (
            f"Pada {row['tanggal']}, media {row['sumber']} "
            f"memberitakan: \"{row['judul']}\"."
        )
        sentences.append(kalimat)
    return sentences


def _format_news_item(news_text: str) -> str | None:
    pattern = r"Pada .*?, (\d{2} \w{3} \d{4}).*?media (.*?) memberitakan: \"(.*?)\""
    match = re.search(pattern, news_text)
    if match:
        tanggal = match.group(1)
        media = match.group(2)
        judul = match.group(3).split(" - ")[0]
        return f"- Menurut media {media}, pada {tanggal}, {judul}"
    return None


def _get_holidays_in_range(start_date, end_date, id_holidays, buffer_days: int = 7) -> list[dict]:
    holiday_list = []
    extended_start = start_date - timedelta(days=buffer_days)
    extended_end = end_date + timedelta(days=buffer_days)
    current = extended_start
    while current <= extended_end:
        if current in id_holidays:
            holiday_list.append({"date": current, "name": id_holidays[current]})
        current += timedelta(days=1)
    return holiday_list


def ask_event_indonesia(start_date, end_date, id_holidays) -> str:
    holidays_found = _get_holidays_in_range(start_date, end_date, id_holidays)
    berita = get_gnews_by_date(start_date, end_date)

    output_lines = []

    if holidays_found:
        output_lines.append("Berdekatan dengan hari besar:")
        for h in holidays_found:
            output_lines.append(f"- {h['name']} ({h['date'].strftime('%d-%m-%Y')})")

    formatted_news = [_format_news_item(item) for item in berita if _format_news_item(item)]

    if formatted_news:
        if output_lines:
            output_lines.append("")
        output_lines.append("Terjadi peristiwa:")
        output_lines.extend(formatted_news)

    if not output_lines:
        return "Tidak ditemukan peristiwa dalam periode tersebut."

    return "\n".join(output_lines)


def analyze_df_events(
    df_group: pd.DataFrame,
    df_detail: pd.DataFrame,
    id_holidays,
    delay: int = 2,
    error_delay: int = 5,
):
    df = df_group.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])

    total = len(df)
    logger.info("Total cluster diproses: %d", total)

    for i, (idx, row) in enumerate(df.iterrows(), 1):
        logger.info("[%d/%d] %s → %s", i, total, row["start_date"].date(), row["end_date"].date())
        try:
            analisis = ask_event_indonesia(row["start_date"], row["end_date"], id_holidays)
            df.loc[idx, "deskripsi"] = analisis
            if i < total:
                time.sleep(delay)
        except Exception as e:
            df.loc[idx, "deskripsi"] = f"ERROR: {e}"
            logger.error("Error cluster %s: %s", idx, e)
            time.sleep(error_delay)

    df_outlier_detail = (
        df_detail[df_detail["is_outlier"] == True][["id", "cluster_id"]]
        .reset_index(drop=True)
    )
    df_outlier_detail.insert(0, "row_id", range(1, len(df_outlier_detail) + 1))

    df_outlier_group = df[["cluster_id", "deskripsi"]].reset_index(drop=True)
    df_outlier_group.insert(0, "row_id", range(1, len(df_outlier_group) + 1))

    return df_outlier_detail, df_outlier_group


def analyze_in_batches(
    df_group: pd.DataFrame,
    df_detail: pd.DataFrame,
    id_holidays,
    batch_size: int = 50,
    delay_between_batch: int = 10,
):
    total = len(df_group)
    results_detail = []
    results_group = []

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        logger.info("Batch %d - %d dari %d", start, end, total)
        df_batch = df_group.iloc[start:end]
        detail, group = analyze_df_events(df_batch, df_detail, id_holidays, delay=2, error_delay=5)
        results_detail.append(detail)
        results_group.append(group)
        if end < total:
            time.sleep(delay_between_batch)

    final_detail = pd.concat(results_detail, ignore_index=True)
    final_group = pd.concat(results_group, ignore_index=True)
    return final_detail, final_group


# ============================================================
# INSERT HELPER
# ============================================================

def get_valid_harga_beras_ids(ids: list) -> set:
    """Kembalikan set id yang benar-benar ada di tabel harga_beras."""
    supabase = _supabase()
    valid = set()
    chunk_size = 500
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        resp = supabase.table("harga_beras").select("id").in_("id", chunk).execute()
        if resp.data:
            valid.update(row["id"] for row in resp.data)
    return valid


def insert_large_data(table_name: str, data: list, batch_size: int = 900):
    supabase = _supabase()
    total = len(data)
    batches = math.ceil(total / batch_size)
    for i in range(batches):
        batch = data[i * batch_size : (i + 1) * batch_size]
        resp = supabase.table(table_name).insert(batch).execute()
        if resp.data is None:
            raise RuntimeError(f"Insert gagal batch {i+1}: {resp}")
        logger.info("Batch %d/%d berhasil (%d rows)", i + 1, batches, len(batch))


# ============================================================
# PIPELINE TASKS
# ============================================================

def task_delete_old_outlier(**context) -> None:
    """Task 1: Hapus data outlier tahun berjalan agar bisa di-recalculate."""
    supabase = _supabase()
    resp = supabase.rpc("delete_outlier_data_by_year", {"tahun_param": CURRENT_YEAR}).execute()
    logger.info("Delete outlier tahun %d selesai. Response: %s", CURRENT_YEAR, resp.data)
    context["ti"].xcom_push(key="year", value=CURRENT_YEAR)


def task_fetch_harga_beras(**context) -> None:
    """Task 2: Ambil semua data harga_beras tahun berjalan dari Supabase."""
    year = context["ti"].xcom_pull(task_ids="delete_old_outlier", key="year")
    df = fetch_data_with_pagination("harga_beras", year)

    if df.empty:
        raise ValueError(f"Tidak ada data harga_beras untuk tahun {year}")

    logger.info("Berhasil ambil %d baris untuk tahun %d", len(df), year)
    context["ti"].xcom_push(key="df_harga_beras", value=df.to_json(orient="records"))


def task_get_last_ids(**context) -> None:
    """Task 3: Ambil last_group_id dan last_detail_id dari tabel outlier."""
    supabase = _supabase()
    resp = supabase.rpc("last_outlier_ids", {}).execute()
    last_group_id = resp.data[0]["last_group_id"]
    last_detail_id = resp.data[0]["last_detail_id"]

    logger.info("last_group_id=%d | last_detail_id=%d", last_group_id, last_detail_id)
    context["ti"].xcom_push(key="last_group_id",  value=last_group_id)
    context["ti"].xcom_push(key="last_detail_id", value=last_detail_id)


def task_detect_outlier(**context) -> None:
    """Task 4: Deteksi outlier dengan metode IQR per grup (tahun, variant, tipe, kota)."""
    ti = context["ti"]
    raw = ti.xcom_pull(task_ids="fetch_harga_beras", key="df_harga_beras")
    df = pd.read_json(StringIO(raw), orient="records", convert_dates=False)

    df["tanggal"] = pd.to_datetime(df["tanggal"])
    df["tahun"] = df["tanggal"].dt.year

    df = detect_iqr_outlier(df)

    n_outlier = df["is_outlier"].sum()
    logger.info("Outlier terdeteksi: %d dari %d baris", n_outlier, len(df))

    ti.xcom_push(key="df_with_outlier", value=df.to_json(orient="records", date_format="iso"))


def task_cluster_outlier(**context) -> None:
    """Task 5: Kelompokkan outlier menjadi cluster berdasarkan tanggal berurutan."""
    ti = context["ti"]
    raw = ti.xcom_pull(task_ids="detect_outlier", key="df_with_outlier")
    df = pd.read_json(StringIO(raw), orient="records", convert_dates=False)
    df["tanggal"] = pd.to_datetime(df["tanggal"])

    df_out, df_full = cluster_outlier(df)

    if df_out.empty:
        logger.info("Tidak ada outlier untuk di-cluster.")
        ti.xcom_push(key="df_out",  value="[]")
        ti.xcom_push(key="df_full", value=df_full.to_json(orient="records", date_format="iso"))
        return

    logger.info("Total cluster: %d", len(df_out))
    ti.xcom_push(key="df_out",  value=df_out.to_json(orient="records",  date_format="iso"))
    ti.xcom_push(key="df_full", value=df_full.to_json(orient="records", date_format="iso"))


def task_analyze_events(**context) -> None:
    """Task 6: Cari berita dan hari libur untuk setiap cluster outlier (GNews + holidays)."""
    ti = context["ti"]
    raw_out  = ti.xcom_pull(task_ids="cluster_outlier", key="df_out")
    raw_full = ti.xcom_pull(task_ids="cluster_outlier", key="df_full")

    df_out  = pd.read_json(StringIO(raw_out),  orient="records", convert_dates=False)
    df_full = pd.read_json(StringIO(raw_full), orient="records", convert_dates=False)

    if df_out.empty:
        logger.info("Tidak ada cluster, skip analisis.")
        ti.xcom_push(key="outlier_detail", value="[]")
        ti.xcom_push(key="outlier_group",  value="[]")
        return

    df_out["start_date"] = pd.to_datetime(df_out["start_date"])
    df_out["end_date"]   = pd.to_datetime(df_out["end_date"])
    df_full["tanggal"]   = pd.to_datetime(df_full["tanggal"])

    min_year = df_out["start_date"].dt.year.min()
    max_year = df_out["end_date"].dt.year.max()
    id_hols = holidays.Indonesia(years=range(min_year, max_year + 1))

    outlier_detail, outlier_group = analyze_in_batches(
        df_out, df_full, id_hols, batch_size=50, delay_between_batch=15
    )

    logger.info(
        "Analisis selesai. detail=%d baris | group=%d baris",
        len(outlier_detail), len(outlier_group),
    )
    ti.xcom_push(key="outlier_detail", value=outlier_detail.to_json(orient="records"))
    ti.xcom_push(key="outlier_group",  value=outlier_group.to_json(orient="records"))


def task_prepare_columns(**context) -> None:
    """Task 7: Setup kolom id, id_group, id_harga_beras sesuai last_id dari DB."""
    ti = context["ti"]
    last_group_id  = ti.xcom_pull(task_ids="get_last_ids", key="last_group_id")
    last_detail_id = ti.xcom_pull(task_ids="get_last_ids", key="last_detail_id")

    raw_detail = ti.xcom_pull(task_ids="analyze_events", key="outlier_detail")
    raw_group  = ti.xcom_pull(task_ids="analyze_events", key="outlier_group")

    outlier_detail = pd.read_json(StringIO(raw_detail), orient="records", convert_dates=False)
    outlier_group  = pd.read_json(StringIO(raw_group),  orient="records", convert_dates=False)

    if outlier_detail.empty or outlier_group.empty:
        logger.info("Tidak ada data outlier untuk dipersiapkan.")
        ti.xcom_push(key="outlier_detail_final", value="[]")
        ti.xcom_push(key="outlier_group_final",  value="[]")
        return

    # Setup outlier_detail
    outlier_detail["id_harga_beras"] = outlier_detail["id"]
    outlier_detail["id"]             = outlier_detail["row_id"] + last_detail_id
    outlier_detail["id_group"]       = outlier_detail["cluster_id"] + last_group_id
    outlier_detail = outlier_detail[["id", "id_harga_beras", "id_group"]]
    outlier_detail = outlier_detail.drop_duplicates(
        subset=["id_harga_beras", "id_group"], keep="first"
    )

    # Setup outlier_group
    outlier_group["id"] = outlier_group["cluster_id"] + last_group_id
    outlier_group = outlier_group[["id", "deskripsi"]]

    logger.info(
        "Kolom siap. detail=%d baris | group=%d baris",
        len(outlier_detail), len(outlier_group),
    )
    ti.xcom_push(key="outlier_detail_final", value=outlier_detail.to_json(orient="records"))
    ti.xcom_push(key="outlier_group_final",  value=outlier_group.to_json(orient="records"))


def task_insert_outlier_group(**context) -> None:
    """Task 8a: Insert outlier_group ke Supabase."""
    ti = context["ti"]
    raw = ti.xcom_pull(task_ids="prepare_columns", key="outlier_group_final")
    df = pd.read_json(StringIO(raw), orient="records", convert_dates=False)

    if df.empty:
        logger.info("outlier_group kosong, skip insert.")
        return

    records = df.to_dict(orient="records")
    insert_large_data("outlier_group", records)
    logger.info("Insert outlier_group selesai. Total: %d baris", len(records))


def task_insert_outlier_detail(**context) -> None:
    """Task 8b: Insert outlier_detail ke Supabase."""
    ti = context["ti"]
    raw = ti.xcom_pull(task_ids="prepare_columns", key="outlier_detail_final")
    df = pd.read_json(StringIO(raw), orient="records", convert_dates=False)

    if df.empty:
        logger.info("outlier_detail kosong, skip insert.")
        return

    # Validasi foreign key: buang baris yang id_harga_beras-nya tidak ada di harga_beras
    all_ids = df["id_harga_beras"].dropna().astype(int).tolist()
    valid_ids = get_valid_harga_beras_ids(all_ids)
    invalid_count = len(df) - df["id_harga_beras"].isin(valid_ids).sum()
    if invalid_count > 0:
        logger.warning(
            "%d baris dibuang karena id_harga_beras tidak ada di tabel harga_beras", invalid_count
        )
        df = df[df["id_harga_beras"].isin(valid_ids)]

    if df.empty:
        logger.info("Semua baris tidak valid setelah filter FK, skip insert.")
        return

    records = df.to_dict(orient="records")
    insert_large_data("outlier_detail", records)
    logger.info("Insert outlier_detail selesai. Total: %d baris", len(records))


# ============================================================
# DAG DEFINITION
# ============================================================

default_args = {
    "owner": "bulog-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="outlier_pipeline",
    description="Pipeline deteksi dan analisis outlier harga beras — di-trigger oleh harga_beras_pipeline",
    default_args=default_args,
    schedule=None,              # tidak ada jadwal; hanya berjalan saat di-trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bulog", "outlier", "beras"],
) as dag:

    t1_delete = PythonOperator(
        task_id="delete_old_outlier",
        python_callable=task_delete_old_outlier,
        doc_md="**Task 1** — Hapus data outlier tahun berjalan via RPC `delete_outlier_data_by_year`.",
    )

    t2_fetch = PythonOperator(
        task_id="fetch_harga_beras",
        python_callable=task_fetch_harga_beras,
        doc_md="**Task 2** — Ambil semua data `harga_beras` tahun berjalan dengan paginasi.",
        execution_timeout=timedelta(minutes=10),
    )

    t3_last_ids = PythonOperator(
        task_id="get_last_ids",
        python_callable=task_get_last_ids,
        doc_md="**Task 3** — Ambil `last_group_id` dan `last_detail_id` via RPC `last_outlier_ids`.",
    )

    t4_detect = PythonOperator(
        task_id="detect_outlier",
        python_callable=task_detect_outlier,
        doc_md="**Task 4** — Deteksi outlier dengan metode IQR per (tahun, variant, tipe, kota).",
    )

    t5_cluster = PythonOperator(
        task_id="cluster_outlier",
        python_callable=task_cluster_outlier,
        doc_md="**Task 5** — Kelompokkan outlier berurutan menjadi cluster (maks 14 hari).",
    )

    t6_analyze = PythonOperator(
        task_id="analyze_events",
        python_callable=task_analyze_events,
        doc_md="**Task 6** — Cari berita (GNews) dan hari libur (holidays) per cluster outlier.",
        execution_timeout=timedelta(hours=3),
    )

    t7_prepare = PythonOperator(
        task_id="prepare_columns",
        python_callable=task_prepare_columns,
        doc_md="**Task 7** — Setup kolom id, id_group, id_harga_beras sesuai last_id dari DB.",
    )

    t8a_insert_group = PythonOperator(
        task_id="insert_outlier_group",
        python_callable=task_insert_outlier_group,
        doc_md="**Task 8a** — Insert hasil ke tabel `outlier_group` di Supabase.",
    )

    t8b_insert_detail = PythonOperator(
        task_id="insert_outlier_detail",
        python_callable=task_insert_outlier_detail,
        doc_md="**Task 8b** — Insert hasil ke tabel `outlier_detail` di Supabase.",
    )

    # ============================================================
    # DEPENDENCY CHAIN
    #
    #  t1_delete
    #     │
    #  ┌──┴──────────┐
    #  t2_fetch    t3_last_ids
    #     │
    #  t4_detect
    #     │
    #  t5_cluster
    #     │
    #  t6_analyze
    #     │        (tunggu t3 juga selesai)
    #  t7_prepare ←─────────────────────┘
    #     │
    #  t8a_insert_group
    #     │
    #  t8b_insert_detail  ← setelah group (foreign key constraint)
    # ============================================================

    t1_delete >> [t2_fetch, t3_last_ids]
    t2_fetch >> t4_detect >> t5_cluster >> t6_analyze
    [t6_analyze, t3_last_ids] >> t7_prepare
    # outlier_detail punya foreign key ke outlier_group → group harus masuk dulu
    t7_prepare >> t8a_insert_group >> t8b_insert_detail
