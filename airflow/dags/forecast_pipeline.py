"""
DAG: forecast_pipeline
Pipeline forecasting harga beras — kode identik dengan pipeline_forecast.ipynb,
hanya dibagi menjadi task per pipeline stage.

Struktur task:
  load_data
    -> preprocessing (per 6 dataset, paralel)
      -> uji_asumsi (per 6 dataset, paralel)
        -> resample_split
          -> evaluasi_model
            -> final_forecast
              -> simpan_database
"""
from __future__ import annotations

import json as _json
import logging
import os
import warnings
from datetime import datetime, timedelta
from io import StringIO

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

from airflow import DAG
from airflow.operators.python import PythonOperator

load_dotenv()

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://extlxiwpcbzqaalpopqn.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV4dGx4aXdwY2J6cWFhbHBvcHFuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjkzNjgyNjUsImV4cCI6MjA4NDk0NDI2NX0.yfnBiW_eo9q0rSM0t1lm6g-xg3jWe1LDYf_x0ZI3rSQ")

# ─────────────────────────────────────────────────────────────────────────────
# KONFIGURASI (dari Cell 14 notebook)
# ─────────────────────────────────────────────────────────────────────────────
KODE_KAB_KOTA    = 1
TEST_RATIO       = 0.2
FORECAST_HORIZON = 8
KOLOM_TARGET     = "harga"
KOLOM_TANGGAL    = "tanggal"

# ─────────────────────────────────────────────────────────────────────────────
# HELPER: XCom serialize / deserialize
# ─────────────────────────────────────────────────────────────────────────────

def series_to_xcom(s: pd.Series) -> str:
    """Serialize pandas Series (dengan DatetimeIndex) ke JSON string."""
    return s.to_json(date_format='iso', orient='split')


def xcom_to_series(json_str: str) -> pd.Series:
    """Deserialize JSON string kembali ke pandas Series dengan DatetimeIndex."""
    s = pd.read_json(StringIO(json_str), orient='split', typ='series')
    s.index = pd.to_datetime(s.index)
    return s


def df_to_xcom(df: pd.DataFrame) -> str:
    """Serialize pandas DataFrame ke JSON string."""
    return df.to_json(date_format='iso', orient='split')


def xcom_to_df(json_str: str) -> pd.DataFrame:
    """Deserialize JSON string kembali ke pandas DataFrame."""
    return pd.read_json(StringIO(json_str), orient='split')


# ─────────────────────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def task_load_data(**context):
    """
    Cell 2: Import Library & Load Data
    Load data harga beras dari Supabase dengan pagination.
    """
    warnings.filterwarnings("ignore")

    _sb     = create_client(SUPABASE_URL, SUPABASE_KEY)
    _table  = "harga_beras"
    _cols   = "id,kode_kab_kota,tanggal,variant_id,harga,tipe_harga_id"
    _batch  = 1000
    _offset = 0
    _rows   = []

    print("  Mengambil data dari Supabase...", end="")
    while True:
        res = (
            _sb.table(_table)
            .select(_cols)
            .gte("tanggal", "2024-01-01")
            .eq("kode_kab_kota", KODE_KAB_KOTA)
            .order("tanggal")
            .range(_offset, _offset + _batch - 1)
            .execute()
        )
        batch_data = res.data
        if not batch_data:
            break
        _rows.extend(batch_data)
        print(f"\r  Mengambil data... {len(_rows):,} baris", end="")
        # Lanjut sampai benar-benar tidak ada data lagi.
        # Tidak pakai len(batch_data) < _batch karena Supabase server bisa mengembalikan
        # kurang dari _batch meski masih ada data berikutnya.
        _offset += len(batch_data)

    df = pd.DataFrame(_rows)
    df['tanggal'] = pd.to_datetime(df['tanggal'])
    df = df.sort_values('tanggal').reset_index(drop=True)

    print(f"\n  Selesai: {len(df):,} baris | {df['tanggal'].min().date()} -> {df['tanggal'].max().date()}")

    # Push ke XCom sebagai JSON
    context['ti'].xcom_push(key='df_raw', value=df_to_xcom(df))


def task_split_dataset(**context):
    """
    Cell 16: Split Dataset per Kategori (6 dataset)
    """
    from utils.forecast_functions import get_start_last_date

    df_json = context['ti'].xcom_pull(key='df_raw', task_ids='load_data')
    df = xcom_to_df(df_json)
    df['tanggal'] = pd.to_datetime(df['tanggal'])

    mask = lambda vid, tid: (
        (df["kode_kab_kota"] == KODE_KAB_KOTA) &
        (df["variant_id"]    == vid) &
        (df["tipe_harga_id"] == tid)
    )

    datasets = {
        "df_M_pasar"   : df[mask(1, 1)].copy(),
        "df_P_pasar"   : df[mask(2, 1)].copy(),
        "df_M_konsumen": df[mask(1, 2)].copy(),
        "df_P_konsumen": df[mask(2, 2)].copy(),
        "df_M_produsen": df[mask(1, 3)].copy(),
        "df_P_produsen": df[mask(2, 3)].copy(),
    }

    for name, d in datasets.items():
        start, end = get_start_last_date(d)
        print(f"  {name:<20}: {len(d)} baris | {start.date()} -> {end.date()}")
        context['ti'].xcom_push(key=name, value=df_to_xcom(d))


def _task_preprocessing(dataset_name: str, **context):
    """
    Cells 19-29: Preprocessing per dataset (search_missing_value + evaluate_imputation)
    """
    from utils.forecast_functions import get_start_last_date, search_missing_value, evaluate_imputation

    df_json = context['ti'].xcom_pull(key=dataset_name, task_ids='split_dataset')
    df = xcom_to_df(df_json)
    df['tanggal'] = pd.to_datetime(df['tanggal'])

    print(f"\n=== Preprocessing {dataset_name} ===")
    start, end = get_start_last_date(df)
    print(f"  Rentang: {start.date()} -> {end.date()}")
    df = search_missing_value(df, week_end=False)
    df, _ = evaluate_imputation(df)

    # df.index is DatetimeIndex setelah evaluate_imputation → reset
    df = df.reset_index()

    context['ti'].xcom_push(key=f"{dataset_name}_prep", value=df_to_xcom(df))


def _task_uji_asumsi(dataset_name: str, **context):
    """
    Cells 32-42: Uji asumsi statistik per dataset
    """
    from utils.forecast_functions import jalankan_uji

    df_json = context['ti'].xcom_pull(key=f"{dataset_name}_prep", task_ids=f"preprocessing_{dataset_name}")
    df = xcom_to_df(df_json)
    df['tanggal'] = pd.to_datetime(df['tanggal'])

    print(f"\n=== Uji Asumsi {dataset_name} ===")
    series, p_kand, q_kand, adf_pval, arch_pval, lb_pval = jalankan_uji(df, label=dataset_name)
    print(f"  p kandidat : {p_kand}")
    print(f"  q kandidat : {q_kand}")
    print(f"  ADF p-value: {adf_pval:.4f} | ARCH p-value: {arch_pval:.4f} | LB p-value: {lb_pval:.4f}")

    context['ti'].xcom_push(key=f"{dataset_name}_adf",  value=float(adf_pval))
    context['ti'].xcom_push(key=f"{dataset_name}_arch", value=float(arch_pval))
    context['ti'].xcom_push(key=f"{dataset_name}_lb",   value=float(lb_pval))
    context['ti'].xcom_push(key=f"{dataset_name}_p_kand", value=p_kand)
    context['ti'].xcom_push(key=f"{dataset_name}_q_kand", value=q_kand)


def task_resample_split(**context):
    """
    Cells 44-46: Resample harian -> mingguan, lalu train-test split
    """
    from utils.forecast_functions import resample_mingguan

    ti = context['ti']
    datasets = ["df_M_pasar", "df_P_pasar", "df_M_konsumen",
                "df_P_konsumen", "df_M_produsen", "df_P_produsen"]

    prep_task_ids = {name: f"preprocessing_{name}" for name in datasets}

    # Load semua dataset yang sudah dipreprocessing
    dfs = {}
    for name in datasets:
        df_json = ti.xcom_pull(key=f"{name}_prep", task_ids=prep_task_ids[name])
        df = xcom_to_df(df_json)
        df['tanggal'] = pd.to_datetime(df['tanggal'])
        dfs[name] = df

    # Cell 44: Resample mingguan
    # df_M_pasar dan df_P_pasar: include_weekend=False (default), sama seperti notebook
    include_wknd = {"df_M_pasar": False, "df_P_pasar": False,
                    "df_M_konsumen": False, "df_P_konsumen": False,
                    "df_M_produsen": False, "df_P_produsen": False}

    sw = {}
    for name in datasets:
        sw[name] = resample_mingguan(dfs[name], include_weekend=include_wknd[name])
        s = sw[name]
        print(f"  {name:<20}: {len(s)} minggu | {s.index[0].date()} -> {s.index[-1].date()}")

    # Versi pasar tanpa weekend untuk align exog ke konsumen/produsen
    sw_M_pasar_w = resample_mingguan(dfs["df_M_pasar"], include_weekend=False)
    sw_P_pasar_w = resample_mingguan(dfs["df_P_pasar"], include_weekend=False)

    # Cell 46: Train-test split
    def split_tt(series, ratio=TEST_RATIO):
        n_test = max(1, int(len(series) * ratio))
        return series.iloc[:-n_test], series.iloc[-n_test:]

    splits = {}
    for name in datasets:
        train, test = split_tt(sw[name])
        splits[name] = (train, test)
        total = len(train) + len(test)
        print(f"  {name:<20}: total={total} | train={len(train)} | test={len(test)}")

    # Push ke XCom
    for name in datasets:
        train, test = splits[name]
        ti.xcom_push(key=f"sw_{name.replace('df_','')}", value=series_to_xcom(sw[name]))
        ti.xcom_push(key=f"train_{name.replace('df_','')}", value=series_to_xcom(train))
        ti.xcom_push(key=f"test_{name.replace('df_','')}", value=series_to_xcom(test))

    ti.xcom_push(key="sw_M_pasar_w", value=series_to_xcom(sw_M_pasar_w))
    ti.xcom_push(key="sw_P_pasar_w", value=series_to_xcom(sw_P_pasar_w))


def task_evaluasi_model(**context):
    """
    Cells 49-59: Evaluasi model fixed (train-test split)
    - df_M_pasar    : ARIMA(2,0,3)+GARCH(1,1)
    - df_P_pasar    : ARIMA(1,0,2)+GARCH(1,1)
    - df_M_konsumen : ARIMAX(6,0,4) tanpa GARCH, exog predicted M_pasar
    - df_P_konsumen : ARIMAX(1,0,4)+GARCH(1,1), exog predicted P_pasar
    - df_M_produsen : SARIMAX(1,1,4)(0,0,0,52), exog predicted M_pasar + M_konsumen
    - df_P_produsen : SARIMAX(1,1,7)(0,0,1,52), exog predicted P_pasar
    """
    from statsmodels.tsa.arima.model import ARIMA
    from utils.forecast_functions import (
        fit_arima_fixed, fit_sarimax_fixed, align_exog, align_exog_multi
    )

    ti = context['ti']

    def load_series(key, task_id='resample_split'):
        return xcom_to_series(ti.xcom_pull(key=key, task_ids=task_id))

    train_M_pasar    = load_series("train_M_pasar")
    test_M_pasar     = load_series("test_M_pasar")
    train_P_pasar    = load_series("train_P_pasar")
    test_P_pasar     = load_series("test_P_pasar")
    train_M_konsumen = load_series("train_M_konsumen")
    test_M_konsumen  = load_series("test_M_konsumen")
    train_P_konsumen = load_series("train_P_konsumen")
    test_P_konsumen  = load_series("test_P_konsumen")
    train_M_produsen = load_series("train_M_produsen")
    test_M_produsen  = load_series("test_M_produsen")
    train_P_produsen = load_series("train_P_produsen")
    test_P_produsen  = load_series("test_P_produsen")

    sw_M_pasar_w = load_series("sw_M_pasar_w")
    sw_P_pasar_w = load_series("sw_P_pasar_w")
    sw_M_konsumen = load_series("sw_M_konsumen")

    # Cell 49: df_M_pasar — ARIMA(2,0,3)+GARCH(1,1)
    print("\n=== df_M_pasar — ARIMA(2,0,3)+GARCH(1,1) ===")
    fc_M_pasar, metrik_M_pasar, lo_M_pasar, hi_M_pasar = fit_arima_fixed(
        train_M_pasar, test_M_pasar, order=(2, 0, 3)
    )

    # Cell 51: df_P_pasar — ARIMA(1,0,2)+GARCH(1,1)
    print("\n=== df_P_pasar — ARIMA(1,0,2)+GARCH(1,1) ===")
    fc_P_pasar, metrik_P_pasar, lo_P_pasar, hi_P_pasar = fit_arima_fixed(
        train_P_pasar, test_P_pasar, order=(1, 0, 2)
    )

    # Cell 53: df_M_konsumen — ARIMAX(6,0,4) tanpa GARCH
    print("\n=== df_M_konsumen — ARIMAX(6,0,4) ===")
    _m_pasar_for_M_kons  = ARIMA(train_M_pasar, order=(2, 0, 3)).fit()
    _fc_pasar_for_M_kons = _m_pasar_for_M_kons.forecast(steps=len(test_M_konsumen)).values

    exog_tr_M_kons = align_exog(sw_M_pasar_w, train_M_konsumen.index)
    exog_te_M_kons = _fc_pasar_for_M_kons.reshape(-1, 1)

    fc_M_konsumen, metrik_M_konsumen, lo_M_konsumen, hi_M_konsumen = fit_arima_fixed(
        train_M_konsumen, test_M_konsumen,
        order=(6, 0, 4),
        exog_train=exog_tr_M_kons,
        exog_test=exog_te_M_kons,
    )

    # Cell 55: df_P_konsumen — ARIMAX(1,0,4)+GARCH(1,1)
    print("\n=== df_P_konsumen — ARIMAX(1,0,4)+GARCH(1,1) ===")
    _m_pasar_for_P_kons  = ARIMA(train_P_pasar, order=(1, 0, 2)).fit()
    _fc_pasar_for_P_kons = _m_pasar_for_P_kons.forecast(steps=len(test_P_konsumen)).values

    exog_tr_P_kons = align_exog(sw_P_pasar_w, train_P_konsumen.index)
    exog_te_P_kons = _fc_pasar_for_P_kons.reshape(-1, 1)

    fc_P_konsumen, metrik_P_konsumen, lo_P_konsumen, hi_P_konsumen = fit_arima_fixed(
        train_P_konsumen, test_P_konsumen,
        order=(1, 0, 4),
        exog_train=exog_tr_P_kons,
        exog_test=exog_te_P_kons,
    )

    # Cell 57: df_M_produsen — SARIMAX(1,1,4)(0,0,0,52)
    print("\n=== df_M_produsen — SARIMAX(1,1,4)(0,0,0,52) ===")
    _fc_pasar_for_M_prod = _m_pasar_for_M_kons.forecast(steps=len(test_M_produsen)).values

    _m_kons_for_M_prod  = ARIMA(train_M_konsumen, order=(6, 0, 4),
                                 exog=align_exog(sw_M_pasar_w, train_M_konsumen.index)).fit()
    _fc_kons_for_M_prod = _m_kons_for_M_prod.forecast(
        steps=len(test_M_produsen), exog=_fc_pasar_for_M_prod.reshape(-1, 1)
    ).values

    exog_tr_M_prod = align_exog_multi([sw_M_pasar_w, sw_M_konsumen], train_M_produsen.index)
    exog_te_M_prod = np.column_stack([_fc_pasar_for_M_prod, _fc_kons_for_M_prod])

    fc_M_produsen, metrik_M_produsen, lo_M_produsen, hi_M_produsen = fit_sarimax_fixed(
        train_M_produsen, test_M_produsen,
        order=(1, 1, 4),
        seasonal_order=(0, 0, 0, 52),
        exog_train=exog_tr_M_prod,
        exog_test=exog_te_M_prod,
    )

    # Cell 59: df_P_produsen — SARIMAX(1,1,7)(0,0,1,52)
    print("\n=== df_P_produsen — SARIMAX(1,1,7)(0,0,1,52) ===")
    _fc_pasar_for_P_prod = _m_pasar_for_P_kons.forecast(steps=len(test_P_produsen)).values

    exog_tr_P_prod = align_exog(sw_P_pasar_w, train_P_produsen.index)
    exog_te_P_prod = _fc_pasar_for_P_prod.reshape(-1, 1)

    fc_P_produsen, metrik_P_produsen, lo_P_produsen, hi_P_produsen = fit_sarimax_fixed(
        train_P_produsen, test_P_produsen,
        order=(1, 1, 7),
        seasonal_order=(0, 0, 1, 52),
        exog_train=exog_tr_P_prod,
        exog_test=exog_te_P_prod,
    )

    # Cell 61: Ringkasan evaluasi
    df_eval = pd.DataFrame([
        metrik_M_pasar, metrik_P_pasar,
        metrik_M_konsumen, metrik_P_konsumen,
        metrik_M_produsen, metrik_P_produsen,
    ])
    df_eval.insert(0, "dataset", [
        "df_M_pasar", "df_P_pasar",
        "df_M_konsumen", "df_P_konsumen",
        "df_M_produsen", "df_P_produsen",
    ])
    print("\n=== Ringkasan Evaluasi ===")
    print(df_eval.to_string(index=False))

    # Push metrik ke XCom
    for name, metrik in [
        ("M_pasar", metrik_M_pasar), ("P_pasar", metrik_P_pasar),
        ("M_konsumen", metrik_M_konsumen), ("P_konsumen", metrik_P_konsumen),
        ("M_produsen", metrik_M_produsen), ("P_produsen", metrik_P_produsen),
    ]:
        ti.xcom_push(key=f"metrik_{name}", value=metrik)


def task_final_forecast(**context):
    """
    Cells 63-75: Final forecast 8 minggu ke depan menggunakan seluruh data
    """
    from statsmodels.tsa.arima.model import ARIMA
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    from utils.forecast_functions import align_exog, align_exog_multi

    ti = context['ti']

    def load_series(key, task_id='resample_split'):
        return xcom_to_series(ti.xcom_pull(key=key, task_ids=task_id))

    sw_M_pasar    = load_series("sw_M_pasar")
    sw_P_pasar    = load_series("sw_P_pasar")
    sw_M_konsumen = load_series("sw_M_konsumen")
    sw_P_konsumen = load_series("sw_P_konsumen")
    sw_M_produsen = load_series("sw_M_produsen")
    sw_P_produsen = load_series("sw_P_produsen")
    sw_M_pasar_w  = load_series("sw_M_pasar_w")
    sw_P_pasar_w  = load_series("sw_P_pasar_w")

    def make_fc_dates(series, n=FORECAST_HORIZON):
        last = series.index[-1]
        return [last + pd.Timedelta(weeks=i+1) for i in range(n)]

    # Cell 65: Final df_M_pasar
    print("\n=== Final Forecast df_M_pasar ===")
    m_final_M_pasar  = ARIMA(sw_M_pasar, order=(2, 0, 3)).fit()
    final_fc_M_pasar = m_final_M_pasar.forecast(steps=FORECAST_HORIZON).values
    fc_dates_M_pasar = make_fc_dates(sw_M_pasar)
    print("  df_M_pasar forecast:", final_fc_M_pasar.round(2))

    # Cell 67: Final df_P_pasar
    print("\n=== Final Forecast df_P_pasar ===")
    m_final_P_pasar  = ARIMA(sw_P_pasar, order=(1, 0, 2)).fit()
    final_fc_P_pasar = m_final_P_pasar.forecast(steps=FORECAST_HORIZON).values
    fc_dates_P_pasar = make_fc_dates(sw_P_pasar)
    print("  df_P_pasar forecast:", final_fc_P_pasar.round(2))

    # Cell 69: Final df_M_konsumen
    print("\n=== Final Forecast df_M_konsumen ===")
    exog_full_M_kons = align_exog(sw_M_pasar_w, sw_M_konsumen.index)
    exog_fc_M_kons   = np.array(final_fc_M_pasar).reshape(-1, 1)

    m_final_M_kons = ARIMA(sw_M_konsumen, order=(6, 0, 4), exog=exog_full_M_kons).fit()
    final_fc_M_konsumen = m_final_M_kons.forecast(steps=FORECAST_HORIZON, exog=exog_fc_M_kons).values
    fc_dates_M_konsumen = make_fc_dates(sw_M_konsumen)
    print("  df_M_konsumen forecast:", final_fc_M_konsumen.round(2))

    # Cell 71: Final df_P_konsumen
    print("\n=== Final Forecast df_P_konsumen ===")
    exog_full_P_kons = align_exog(sw_P_pasar_w, sw_P_konsumen.index)
    exog_fc_P_kons   = np.array(final_fc_P_pasar).reshape(-1, 1)

    m_final_P_kons = ARIMA(sw_P_konsumen, order=(1, 0, 4), exog=exog_full_P_kons).fit()
    final_fc_P_konsumen = m_final_P_kons.forecast(steps=FORECAST_HORIZON, exog=exog_fc_P_kons).values
    fc_dates_P_konsumen = make_fc_dates(sw_P_konsumen)
    print("  df_P_konsumen forecast:", final_fc_P_konsumen.round(2))

    # Cell 73: Final df_M_produsen
    print("\n=== Final Forecast df_M_produsen ===")
    exog_full_M_prod = align_exog_multi([sw_M_pasar_w, sw_M_konsumen], sw_M_produsen.index)

    exog_fc_M_kons_for_prod = np.array(final_fc_M_pasar).reshape(-1, 1)
    exog_fc_M_kons_arr      = m_final_M_kons.forecast(steps=FORECAST_HORIZON,
                                                       exog=exog_fc_M_kons_for_prod).values

    exog_fc_M_prod = np.column_stack([
        np.array(final_fc_M_pasar),
        exog_fc_M_kons_arr,
    ])

    m_final_M_prod = SARIMAX(sw_M_produsen, exog=exog_full_M_prod,
                              order=(1, 1, 4), seasonal_order=(0, 0, 0, 52),
                              enforce_stationarity=False,
                              enforce_invertibility=False).fit(disp=False)
    final_fc_M_produsen = m_final_M_prod.forecast(steps=FORECAST_HORIZON, exog=exog_fc_M_prod).values
    fc_dates_M_produsen = make_fc_dates(sw_M_produsen)
    print("  df_M_produsen forecast:", final_fc_M_produsen.round(2))

    # Cell 75: Final df_P_produsen
    print("\n=== Final Forecast df_P_produsen ===")
    exog_full_P_prod = align_exog(sw_P_pasar_w, sw_P_produsen.index)
    exog_fc_P_prod   = np.array(final_fc_P_pasar).reshape(-1, 1)

    m_final_P_prod = SARIMAX(sw_P_produsen, exog=exog_full_P_prod,
                              order=(1, 1, 7), seasonal_order=(0, 0, 1, 52),
                              enforce_stationarity=False,
                              enforce_invertibility=False).fit(disp=False)
    final_fc_P_produsen = m_final_P_prod.forecast(steps=FORECAST_HORIZON, exog=exog_fc_P_prod).values
    fc_dates_P_produsen = make_fc_dates(sw_P_produsen)
    print("  df_P_produsen forecast:", final_fc_P_produsen.round(2))

    # Push forecast ke XCom
    fc_data = {
        "M_pasar":    {"dates": [str(d.date()) for d in fc_dates_M_pasar],    "values": final_fc_M_pasar.tolist()},
        "P_pasar":    {"dates": [str(d.date()) for d in fc_dates_P_pasar],    "values": final_fc_P_pasar.tolist()},
        "M_konsumen": {"dates": [str(d.date()) for d in fc_dates_M_konsumen], "values": final_fc_M_konsumen.tolist()},
        "P_konsumen": {"dates": [str(d.date()) for d in fc_dates_P_konsumen], "values": final_fc_P_konsumen.tolist()},
        "M_produsen": {"dates": [str(d.date()) for d in fc_dates_M_produsen], "values": final_fc_M_produsen.tolist()},
        "P_produsen": {"dates": [str(d.date()) for d in fc_dates_P_produsen], "values": final_fc_P_produsen.tolist()},
    }
    ti.xcom_push(key="fc_data", value=fc_data)


def task_simpan_database(**context):
    """
    Cells 77-80: Susun df_detail & simpan ke Supabase
    - evaluasi_prediksi
    - hasil_prediksi_harga_beras
    """
    ti  = context['ti']
    _sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── Ambil data dari XCom ─────────────────────────────────────────────────
    datasets = ["df_M_pasar", "df_P_pasar", "df_M_konsumen",
                "df_P_konsumen", "df_M_produsen", "df_P_produsen"]

    metrik_keys = {
        "df_M_pasar":    "M_pasar",
        "df_P_pasar":    "P_pasar",
        "df_M_konsumen": "M_konsumen",
        "df_P_konsumen": "P_konsumen",
        "df_M_produsen": "M_produsen",
        "df_P_produsen": "P_produsen",
    }

    uji_task_id = {
        "df_M_pasar":    "uji_asumsi_df_M_pasar",
        "df_P_pasar":    "uji_asumsi_df_P_pasar",
        "df_M_konsumen": "uji_asumsi_df_M_konsumen",
        "df_P_konsumen": "uji_asumsi_df_P_konsumen",
        "df_M_produsen": "uji_asumsi_df_M_produsen",
        "df_P_produsen": "uji_asumsi_df_P_produsen",
    }

    # Cell 77: Ambil max kode_prediksi
    try:
        _res_max = _sb.table("evaluasi_prediksi").select("kode_prediksi").order("kode_prediksi", desc=True).limit(1).execute()
        _max_kode = _res_max.data[0]["kode_prediksi"] if _res_max.data else 0
    except Exception:
        _max_kode = 0

    kode_prediksi_map = {name: _max_kode + i + 1 for i, name in enumerate(datasets)}
    print("  kode_prediksi:", kode_prediksi_map)

    # Cell 77: Susun df_detail_rows
    df_detail_rows = []
    for ds_name in datasets:
        mk = metrik_keys[ds_name]
        metrik   = ti.xcom_pull(key=f"metrik_{mk}",       task_ids="evaluasi_model")
        adf_pval = ti.xcom_pull(key=f"{ds_name}_adf",     task_ids=uji_task_id[ds_name])
        arch_pval= ti.xcom_pull(key=f"{ds_name}_arch",    task_ids=uji_task_id[ds_name])
        lb_pval  = ti.xcom_pull(key=f"{ds_name}_lb",      task_ids=uji_task_id[ds_name])
        p_kand   = ti.xcom_pull(key=f"{ds_name}_p_kand",  task_ids=uji_task_id[ds_name])
        q_kand   = ti.xcom_pull(key=f"{ds_name}_q_kand",  task_ids=uji_task_id[ds_name])

        df_detail_rows.append({
            "kode_prediksi":      kode_prediksi_map[ds_name],
            "model":              metrik["model"],
            "mae":                metrik["MAE"],
            "mape":               metrik["MAPE"],
            "rmse":               metrik["RMSE"],
            "adf_pvalue":         adf_pval,
            "arch_pvalue":        arch_pval,
            "ljung_box_pvalue":   lb_pval,
            "acf_signifikan_lag": _json.dumps(q_kand),
            "pacf_signifikan_lag":_json.dumps(p_kand),
        })

    # Cell 78: Susun df_forecast_all
    fc_data = ti.xcom_pull(key="fc_data", task_ids="final_forecast")

    # mapping: (variant_id, tipe_harga_id)
    fc_meta = {
        "M_pasar":    (1, 1),
        "P_pasar":    (2, 1),
        "M_konsumen": (1, 2),
        "P_konsumen": (2, 2),
        "M_produsen": (1, 3),
        "P_produsen": (2, 3),
    }

    _fc_configs = [
        (fc_data[k]["dates"], fc_data[k]["values"], vid, tid, kode_prediksi_map[f"df_{k}"])
        for k, (vid, tid) in fc_meta.items()
    ]

    df_forecast_all = pd.concat([
        pd.DataFrame({
            "variant_id"    : vid,
            "tipe_harga"    : tid,
            "tanggal"       : pd.to_datetime(dates),
            "harga"         : vals,
            "kode_prediksi" : kode,
        })
        for dates, vals, vid, tid, kode in _fc_configs
    ], ignore_index=True)

    df_forecast_all["tanggal"] = df_forecast_all["tanggal"].dt.date.astype(str)
    df_forecast_all["harga"]   = df_forecast_all["harga"].round(0).astype(int)

    # Cell 80: Simpan evaluasi_prediksi
    print("Menyimpan evaluasi_prediksi...")
    _eval_records = df_detail_rows
    _BATCH = 100
    for _i in range(0, len(_eval_records), _BATCH):
        _chunk = _eval_records[_i:_i + _BATCH]
        try:
            _sb.table("evaluasi_prediksi").insert(_chunk).execute()
        except Exception as _e:
            print(f"  [ERROR] evaluasi_prediksi batch {_i}: {_e}")
            raise
    print(f"  evaluasi_prediksi: {len(_eval_records)} rows berhasil disimpan.")

    # Cell 80: Simpan hasil_prediksi_harga_beras
    print("Menyimpan hasil_prediksi_harga_beras...")
    _pred_df = df_forecast_all.copy()

    _tanggal_baru   = _pred_df["tanggal"].unique().tolist()
    _existing = []
    for _i in range(0, len(_tanggal_baru), _BATCH):
        _chunk_tgl = _tanggal_baru[_i:_i + _BATCH]
        try:
            _res = _sb.table("hasil_prediksi_harga_beras").select("tanggal").in_("tanggal", _chunk_tgl).execute()
            _existing.extend([r["tanggal"] for r in _res.data])
        except Exception as _e:
            print(f"  [WARNING] cek existing tanggal error: {_e}")

    _existing_set = set(_existing)
    if _existing_set:
        print(f"  Ditemukan {len(_existing_set)} tanggal yang sudah ada, menghapus dulu...")
        for _tgl in _existing_set:
            try:
                _sb.table("hasil_prediksi_harga_beras").delete().eq("tanggal", _tgl).execute()
            except Exception as _e:
                print(f"  [ERROR] delete tanggal {_tgl}: {_e}")
                raise
        print(f"  {len(_existing_set)} tanggal berhasil dihapus.")
    else:
        print("  Tidak ada tanggal duplikat, langsung insert.")

    _pred_records = _pred_df.to_dict(orient="records")
    for _i in range(0, len(_pred_records), _BATCH):
        _chunk = _pred_records[_i:_i + _BATCH]
        try:
            _sb.table("hasil_prediksi_harga_beras").insert(_chunk).execute()
        except Exception as _e:
            print(f"  [ERROR] hasil_prediksi_harga_beras batch {_i}: {_e}")
            raise
    print(f"  hasil_prediksi_harga_beras: {len(_pred_records)} rows berhasil disimpan.")
    print("\nSelesai. Semua data berhasil disimpan ke Supabase.")


# ─────────────────────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner"          : "airflow",
    "retries"        : 1,
    "retry_delay"    : timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

DATASETS = ["df_M_pasar", "df_P_pasar", "df_M_konsumen",
            "df_P_konsumen", "df_M_produsen", "df_P_produsen"]

with DAG(
    dag_id="forecast_prediction_pipeline",
    default_args=default_args,
    description="Pipeline forecasting harga beras — ARIMA/SARIMAX+GARCH",
    schedule_interval="0 1 * * 1",   # setiap Senin jam 01:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["forecast", "harga_beras"],
) as dag:

    # Task 1: Load data
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=task_load_data,
    )

    # Task 2: Split dataset
    split_dataset = PythonOperator(
        task_id="split_dataset",
        python_callable=task_split_dataset,
    )

    # Task 3-8: Preprocessing per dataset (paralel)
    preprocessing_tasks = {
        ds: PythonOperator(
            task_id=f"preprocessing_{ds}",
            python_callable=_task_preprocessing,
            op_kwargs={"dataset_name": ds},
        )
        for ds in DATASETS
    }

    # Task 9-14: Uji asumsi per dataset (paralel)
    uji_asumsi_tasks = {
        ds: PythonOperator(
            task_id=f"uji_asumsi_{ds}",
            python_callable=_task_uji_asumsi,
            op_kwargs={"dataset_name": ds},
        )
        for ds in DATASETS
    }

    # Task 15: Resample + split
    resample_split = PythonOperator(
        task_id="resample_split",
        python_callable=task_resample_split,
    )

    # Task 16: Evaluasi model
    evaluasi_model = PythonOperator(
        task_id="evaluasi_model",
        python_callable=task_evaluasi_model,
    )

    # Task 17: Final forecast
    final_forecast = PythonOperator(
        task_id="final_forecast",
        python_callable=task_final_forecast,
    )

    # Task 18: Simpan ke database
    simpan_database = PythonOperator(
        task_id="simpan_database",
        python_callable=task_simpan_database,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    # load_data -> split_dataset
    load_data >> split_dataset

    # split_dataset -> preprocessing (paralel per dataset)
    for ds in DATASETS:
        split_dataset >> preprocessing_tasks[ds]

    # preprocessing -> uji_asumsi (paralel per dataset)
    for ds in DATASETS:
        preprocessing_tasks[ds] >> uji_asumsi_tasks[ds]

    # semua preprocessing selesai -> resample_split
    for ds in DATASETS:
        preprocessing_tasks[ds] >> resample_split

    # resample_split -> evaluasi_model
    # semua uji_asumsi selesai -> evaluasi_model (untuk pastikan XCom tersedia)
    list(uji_asumsi_tasks.values()) >> evaluasi_model
    resample_split >> evaluasi_model

    # evaluasi_model -> final_forecast -> simpan_database
    evaluasi_model >> final_forecast >> simpan_database
