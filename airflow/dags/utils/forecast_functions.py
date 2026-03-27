"""
Fungsi-fungsi dari pipeline_forecast.ipynb.
Kode ini tidak diubah, hanya dipindahkan dari notebook ke module.
"""
# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS (dari Cell 2 notebook)
# ─────────────────────────────────────────────────────────────────────────────
import warnings

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error
import matplotlib
matplotlib.use("Agg")  # non-interactive backend untuk Airflow
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from arch import arch_model
import matplotlib.dates as mdates
from itertools import product
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.stats.diagnostic import het_arch
import sqlalchemy, uuid, json as _json

# ─────────────────────────────────────────────────────────────────────────────
# KONFIGURASI (dari Cell 6 notebook)
# ─────────────────────────────────────────────────────────────────────────────
KOLOM_TARGET  = "harga"
KOLOM_TANGGAL = "tanggal"
MAX_LAGS      = 24
ALPHA         = 0.05


# ─────────────────────────────────────────────────────────────────────────────
# FUNGSI PREPROCESSING (dari Cell 4 notebook)
# ─────────────────────────────────────────────────────────────────────────────
def get_start_last_date(df):
    df = df.copy()
    df['tanggal'] = pd.to_datetime(df['tanggal'])
    start_date = df['tanggal'].min()
    last_date = df['tanggal'].max()
    return start_date, last_date

def search_missing_value(df, week_end=False):
    df = df.copy()

    # pastikan datetime
    df['tanggal'] = pd.to_datetime(df['tanggal'])

    # hapus duplicate tanggal (ambil rata-rata harga jika ada duplikat)
    df = df.groupby('tanggal', as_index=False)['harga'].mean()

    # jika weekend tidak dipakai
    if not week_end:
        df = df[df['tanggal'].dt.weekday < 5]
        freq = 'B'   # business day
    else:
        freq = 'D'   # semua hari

    # ambil range tanggal
    start_date, last_date = get_start_last_date(df)

    all_dates = pd.date_range(
        start=start_date,
        end=last_date,
        freq=freq
    )

    # tanggal yang ada
    existing_dates = pd.to_datetime(df['tanggal'].dt.normalize().unique())

    # cari missing date
    missing_dates = sorted(set(all_dates) - set(existing_dates))

    percentage_missing = round(len(missing_dates) / len(all_dates) * 100, 1)

    print(f"Jumlah data baris = {len(df)}")
    print("Jumlah missing:", len(missing_dates))
    print(f"Persentase missing: {percentage_missing}%")
    print("Date missing:", missing_dates)

    # isi tanggal yang hilang
    df = df.set_index('tanggal')
    df = df.reindex(all_dates)

    # isi kolom konstan
    df['kode_kab_kota'] = 1
    df['variant_id'] = 1
    df['tipe_harga_id'] = 1

    # kembalikan tanggal ke kolom
    df = df.reset_index().rename(columns={'index': 'tanggal'})

    # hapus kolom id jika ada
    if 'id' in df.columns:
        df = df.drop(columns=['id'])

    return df


def evaluate_imputation(df, price_col="harga", date_col="tanggal", missing_ratio=0.1, seed=42):

    np.random.seed(seed)

    df_test = df.copy()

    # buat missing artificial
    mask = np.random.rand(len(df_test)) < missing_ratio
    true_values = df_test.loc[mask, price_col]

    df_test.loc[mask, price_col] = np.nan

    # imputasi
    ffill = df_test[price_col].ffill()
    linear = df_test[price_col].interpolate()

    # dataframe evaluasi
    eval_df = pd.DataFrame({
        "true": true_values,
        "ffill": ffill[mask],
        "linear": linear[mask]
    })

    # hapus NaN
    eval_ffill = eval_df.dropna(subset=["true", "ffill"])
    eval_linear = eval_df.dropna(subset=["true", "linear"])

    # MAE
    mae_ffill = mean_absolute_error(eval_ffill["true"], eval_ffill["ffill"])
    mae_linear = mean_absolute_error(eval_linear["true"], eval_linear["linear"])

    # MAPE
    mape_ffill = mean_absolute_percentage_error(eval_ffill["true"], eval_ffill["ffill"]) * 100
    mape_linear = mean_absolute_percentage_error(eval_linear["true"], eval_linear["linear"]) * 100

    print("Forward Fill")
    print("MAE :", mae_ffill)
    print("MAPE:", mape_ffill)

    print("\nLinear Interpolation")
    print("MAE :", mae_linear)
    print("MAPE:", mape_linear)

    # pilih metode terbaik
    method = "linear" if mae_linear < mae_ffill else "ffill"
    print(f"\nMetode terbaik berdasarkan MAE: {method}")

    # imputasi pada data asli
    df = df.sort_values(date_col)
    df = df.set_index(date_col)

    if method == "linear":
        df[price_col] = df[price_col].interpolate(method="linear")
    else:
        df[price_col] = df[price_col].ffill()

    missing = df[price_col].isna().sum()
    print(f"Jumlah missing setelah imputasi = {missing}")

    return df, {
        "mae_ffill": mae_ffill,
        "mape_ffill": mape_ffill,
        "mae_linear": mae_linear,
        "mape_linear": mape_linear,
        "best_method": method
    }


# ─────────────────────────────────────────────────────────────────────────────
# FUNGSI UJI ASUMSI (dari Cell 6 notebook)
# ─────────────────────────────────────────────────────────────────────────────
def uji_adf(series, label="Series"):
    print("\n" + "="*60)
    print(f"  1. ADF TEST — Stasioneritas: {label}")
    print("="*60)

    hasil = adfuller(series, autolag="AIC")
    stat, pval, lags_used, nobs = hasil[0], hasil[1], hasil[2], hasil[3]
    critical = hasil[4]

    print(f"  ADF Statistic : {stat:.4f}")
    print(f"  p-value       : {pval:.4f}")
    print(f"  Lags Used     : {lags_used}")
    print(f"  Observations  : {nobs}")
    print("  Critical Values:")
    for key, val in critical.items():
        print(f"    {key}: {val:.4f}")

    if pval < ALPHA:
        print(f"\n  Stasioner (p={pval:.4f} < {ALPHA}) -> Tidak perlu differencing")
    else:
        print(f"\n  TIDAK STASIONER (p={pval:.4f} >= {ALPHA}) -> Lakukan differencing (d>=1)")

    return pval < ALPHA, pval


def plot_acf_pacf(series, label="Series", lags=MAX_LAGS):
    print("\n" + "="*60)
    print(f"  2. ACF & PACF — Parameter ARIMA (p, d, q): {label}")
    print("="*60)

    acf_vals  = acf(series, nlags=lags, fft=True)
    pacf_vals = pacf(series, nlags=lags)
    ci        = 1.96 / np.sqrt(len(series))

    q_kandidat = [i for i in range(1, lags+1) if abs(acf_vals[i])  > ci]
    p_kandidat = [i for i in range(1, lags+1) if abs(pacf_vals[i]) > ci]

    print(f"\n  Batas Signifikansi (+-{ci:.4f})")
    print(f"  -> ACF signifikan pada lag  : {q_kandidat[:5]}  -> q kandidat")
    print(f"  -> PACF signifikan pada lag : {p_kandidat[:5]}  -> p kandidat")
    if q_kandidat: print(f"\n  Saran q (MA) : {q_kandidat[0]}")
    if p_kandidat: print(f"  Saran p (AR) : {p_kandidat[0]}")

    if not p_kandidat: print("  Tidak ada PACF signifikan -> p kandidat default [1]"); p_kandidat = [1]
    if not q_kandidat: print("  Tidak ada ACF signifikan -> q kandidat default [1]");  q_kandidat = [1]

    fig, axes = plt.subplots(2, 1, figsize=(12, 7))
    fig.suptitle(f"ACF & PACF — {label}", fontsize=14, fontweight="bold", y=1.01)

    plot_acf( series, lags=lags, ax=axes[0], alpha=ALPHA,
              title="ACF -> tentukan q")
    plot_pacf(series, lags=lags, ax=axes[1], alpha=ALPHA,
              title="PACF -> tentukan p", method="ywm")

    for ax in axes:
        ax.axhline(y= ci, linestyle="--", color="red", linewidth=0.8, alpha=0.7)
        ax.axhline(y=-ci, linestyle="--", color="red", linewidth=0.8, alpha=0.7)
        ax.set_xlabel("Lag")

    plt.tight_layout()
    plt.close(fig)

    return p_kandidat, q_kandidat


def uji_ljung_box(series, label="Series", lags=MAX_LAGS):
    print("\n" + "="*60)
    print(f"  3. LJUNG-BOX TEST — White Noise Residual: {label}")
    print("="*60)

    hasil_lb = acorr_ljungbox(series, lags=list(range(1, lags+1)), return_df=True)
    lb_stat  = hasil_lb["lb_stat"].iloc[-1]
    lb_pval  = hasil_lb["lb_pvalue"].iloc[-1]

    print(f"\n  Lag diuji  : {lags}")
    print(f"  LB Stat    : {lb_stat:.4f}")
    print(f"  LB p-value : {lb_pval:.4f}")

    if lb_pval > ALPHA:
        print(f"\n  White Noise (p={lb_pval:.4f} >= {ALPHA}) — Model adequate")
    else:
        print(f"\n  Autokorelasi (p={lb_pval:.4f} < {ALPHA}) — Perlu perbaikan model")

    return hasil_lb


def uji_arch(series, label="Series", lags=12):
    print("\n" + "="*60)
    print(f"  4. ARCH TEST — Heteroskedastisitas & Volatilitas: {label}")
    print("="*60)

    ret = series.pct_change().dropna()

    stat, pval, _, _ = het_arch(ret, nlags=lags)

    print(f"\n  ARCH LM Statistic : {stat:.4f}")
    print(f"  p-value           : {pval:.4f}")
    print(f"  Lags Diuji        : {lags}")

    if pval < ALPHA:
        print(f"\n  EFEK ARCH ADA (p={pval:.4f} < {ALPHA}) -> Gunakan GARCH")
    else:
        print(f"\n  TIDAK ADA efek ARCH (p={pval:.4f} >= {ALPHA}) -> ARIMA cukup")

    vol_fit  = arch_model(ret, vol="Garch", p=1, q=1, rescale=True).fit(disp="off")
    cond_vol = vol_fit.conditional_volatility

    fig, axes = plt.subplots(2, 1, figsize=(12, 6), sharex=True)
    fig.suptitle(f"ARCH Test — Return & Conditional Volatility: {label}",
                 fontsize=13, fontweight="bold")

    axes[0].plot(ret.index, ret.values, color="#2563eb", linewidth=0.8, alpha=0.85)
    axes[0].set_title(f"Return {label}")
    axes[0].set_ylabel("Return")
    axes[0].axhline(0, color="gray", linewidth=0.5)

    axes[1].plot(cond_vol.index, cond_vol.values, color="#dc2626", linewidth=1.2)
    axes[1].set_title(f"Conditional Volatility (GARCH(1,1)) — {label}")
    axes[1].set_ylabel("Volatilitas")
    axes[1].set_xlabel("Tanggal")

    plt.tight_layout()
    plt.close(fig)

    return stat, pval


def cetak_ringkasan(stasioner, lb_df, arch_pval, label="Series"):
    print("\n" + "="*60)
    print(f"  RINGKASAN HASIL UJI STATISTIK — {label}")
    print("="*60)

    n_lb_sig = (lb_df["lb_pvalue"] < ALPHA).sum()

    status = {
        "ADF (Stasioneritas)"    : "Stasioner" if stasioner else "Tidak Stasioner -> differencing",
        "ACF/PACF (ARIMA)"       : "Lihat plot untuk p & q",
        "Ljung-Box (White Noise)": f"{'White Noise' if n_lb_sig == 0 else f'{n_lb_sig} lag signifikan'}",
        "ARCH (Volatilitas)"     : f"{'Ada efek ARCH -> GARCH' if arch_pval < ALPHA else 'Tidak ada efek ARCH'}",
    }

    for uji, hasil in status.items():
        print(f"  {'['+uji+']':<30} {hasil}")

    print("\n  Rekomendasi:")
    if not stasioner:     print("  Lakukan first-differencing (d=1) sebelum ARIMA")
    if n_lb_sig > 0:      print("  Sesuaikan parameter p, q berdasarkan ACF/PACF")
    if arch_pval < ALPHA: print("  Pertimbangkan model GARCH(1,1) untuk volatilitas")
    print("="*60)


def jalankan_uji(df, label, kolom_target=KOLOM_TARGET, kolom_tanggal=KOLOM_TANGGAL):
    print("\n" + "█"*60)
    print(f"  ANALISIS STATISTIK — {label}")
    print("█"*60)

    # Siapkan series
    if kolom_tanggal and kolom_tanggal in df.columns:
        df = df.set_index(kolom_tanggal)
    series = df[kolom_target].dropna()

    stasioner, adf_pval     = uji_adf(series, label=label)
    p_kandidat, q_kandidat  = plot_acf_pacf(series, label=label)
    lb_df                   = uji_ljung_box(series, label=label)
    arch_stat, arch_pval    = uji_arch(series, label=label)
    lb_pval                 = lb_df["lb_pvalue"].iloc[-1]
    cetak_ringkasan(stasioner, lb_df, arch_pval, label=label)

    return series, p_kandidat, q_kandidat, adf_pval, arch_pval, lb_pval


# ─────────────────────────────────────────────────────────────────────────────
# FUNGSI HELPER (dari Cell 8 notebook)
# ─────────────────────────────────────────────────────────────────────────────
def hitung_metrik(aktual, prediksi, label="Model"):
    aktual   = np.array(aktual)
    prediksi = np.array(prediksi)
    rmse = np.sqrt(mean_squared_error(aktual, prediksi))
    mae  = mean_absolute_error(aktual, prediksi)
    mape = np.mean(np.abs((aktual - prediksi) / aktual)) * 100
    print(f"  {'─'*42}")
    print(f"  {label}")
    print(f"  RMSE : {rmse:.3f}  |  MAE : {mae:.3f}  |  MAPE : {mape:.4f}%")
    return {"model": label, "RMSE": rmse, "MAE": mae, "MAPE": mape}


def resample_mingguan(df, kolom_target="harga", kolom_tanggal="tanggal", include_weekend=False):
    """Ubah df harian menjadi series mingguan (W-MON mean).
    Mendukung tanggal sebagai kolom maupun sebagai index."""
    df = df.copy()
    # Jika tanggal sudah jadi index (setelah evaluate_imputation), reset dulu
    if kolom_tanggal in df.index.names:
        df = df.reset_index()
    if kolom_tanggal in df.columns:
        df[kolom_tanggal] = pd.to_datetime(df[kolom_tanggal])
        df = df.sort_values(kolom_tanggal).set_index(kolom_tanggal)
    series = df[kolom_target].dropna()
    if not include_weekend:
        series = series[series.index.dayofweek < 5]
    return series.resample("W-MON").mean().dropna()


def align_exog(exog_series, target_index):
    """Sejajarkan exog series dengan index target (reindex + interpolasi)."""
    return exog_series.reindex(target_index).interpolate().ffill().bfill().values.reshape(-1, 1)


def align_exog_multi(series_list, target_index):
    """Stack beberapa exog series menjadi array 2D (n_obs x n_exog)."""
    cols = [align_exog(s, target_index) for s in series_list]
    return np.hstack(cols)


# ─────────────────────────────────────────────────────────────────────────────
# FUNGSI FIT MODEL FIXED (dari Cell 10 notebook)
# ─────────────────────────────────────────────────────────────────────────────
def fit_dengan_garch(model_fit, test, exog_test, model_label, test_size, force_no_garch=False):
    """
    Setelah model ARIMA/SARIMAX di-fit:
    1. Uji ARCH pada residual
    2. Jika ada efek ARCH -> tambahkan GARCH(1,1) koreksi
    3. Hitung metrik evaluasi
    Returns: fc_values (np.array), metrik (dict), lo (array|None), hi (array|None)
    """
    sarimax_fc = model_fit.forecast(steps=test_size, exog=exog_test)
    residuals  = model_fit.resid.dropna()

    lo, hi = None, None
    if force_no_garch:
        print("  GARCH dilewati (force_no_garch=True)")
        fc_vals = sarimax_fc.values
    else:
        stat, pval, _, _ = het_arch(residuals, nlags=12)
        print(f"  ARCH Test: stat={stat:.4f}, p={pval:.4f}", end="")

        if pval >= 0.05:
            print(" -> tidak ada efek ARCH, GARCH dilewati")
            fc_vals = sarimax_fc.values
        else:
            print(" -> ada efek ARCH, fit GARCH(1,1)")
            garch     = arch_model(residuals, vol="Garch", p=1, q=1, dist="normal")
            garch_fit = garch.fit(disp="off")
            fc_garch  = garch_fit.forecast(horizon=test_size)
            garch_var = fc_garch.variance.values[-1]
            garch_std = np.sqrt(garch_var)
            fc_vals   = sarimax_fc.values
            lo        = fc_vals - 1.96 * garch_std
            hi        = fc_vals + 1.96 * garch_std

    metrik = hitung_metrik(test.values, fc_vals, label=model_label)
    return fc_vals, metrik, lo, hi


def fit_arima_fixed(train, test, order, exog_train=None, exog_test=None, force_no_garch=False):
    """Fit ARIMA/ARIMAX dengan order fixed, lalu uji ARCH."""
    p, d, q   = order
    has_exog  = exog_train is not None
    label     = f"{'ARIMAX' if has_exog else 'ARIMA'}({p},{d},{q})"
    print(f"\n  Fitting {label}...")
    m = ARIMA(train, order=order, exog=exog_train).fit()
    return fit_dengan_garch(m, test, exog_test, label, len(test), force_no_garch=force_no_garch)


def fit_sarimax_fixed(train, test, order, seasonal_order, exog_train=None, exog_test=None, force_no_garch=False):
    """Fit SARIMA/SARIMAX dengan order fixed, lalu uji ARCH."""
    p, d, q   = order
    P, D, Q, m_period = seasonal_order
    has_exog  = exog_train is not None
    label     = f"{'SARIMAX' if has_exog else 'SARIMA'}({p},{d},{q})({P},{D},{Q},{m_period})"
    print(f"\n  Fitting {label}...")
    m = SARIMAX(train, order=order, seasonal_order=seasonal_order,
                exog=exog_train,
                enforce_stationarity=False, enforce_invertibility=False).fit(disp=False)
    return fit_dengan_garch(m, test, exog_test, label, len(test), force_no_garch=force_no_garch)


# ─────────────────────────────────────────────────────────────────────────────
# FUNGSI SIMPAN DATABASE (dari Cell 12 notebook)
# ─────────────────────────────────────────────────────────────────────────────
def get_db_engine(host="localhost", port=5432, dbname="bulog",
                  user="postgres", password="postgres"):
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return sqlalchemy.create_engine(url)


def simpan_evaluasi(df_detail_rows, engine):
    df_out = pd.DataFrame(df_detail_rows)
    df_out.to_sql("df_detail", engine, if_exists="append", index=False)
    print(f"  {len(df_out)} baris evaluasi disimpan ke tabel df_detail")


def simpan_prediksi(kode_kab_kota, variant_id, tipe_harga,
                    fc_dates, fc_values, kode_prediksi, engine):
    rows = [{"kode_kab_kota": kode_kab_kota, "variant_id": variant_id,
             "tipe_harga": tipe_harga, "tanggal": dt,
             "harga": float(val), "kode_prediksi": kode_prediksi}
            for dt, val in zip(fc_dates, fc_values)]
    pd.DataFrame(rows).to_sql("prediksi_harga_beras", engine, if_exists="append", index=False)
    print(f"  {len(rows)} baris prediksi disimpan (kode_prediksi={kode_prediksi})")
