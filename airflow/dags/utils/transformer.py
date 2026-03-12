import logging
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)


def transform_beras_data(raw: dict) -> dict:
    """
    Normalisasi dan validasi data mentah dari scraper.

    Input:  dict dari scraper dengan keys: tanggal, beras_medium, beras_premium
    Output: dict bersih siap untuk Supabase upsert
    """
    # tanggal: normalisasi ke string YYYY-MM-DD
    tanggal_raw = raw.get("tanggal")
    if isinstance(tanggal_raw, str):
        tanggal = tanggal_raw.strip()
    elif isinstance(tanggal_raw, (date, datetime)):
        tanggal = tanggal_raw.strftime("%Y-%m-%d")
    else:
        tanggal = date.today().strftime("%Y-%m-%d")
        logger.warning("tanggal tidak ditemukan di raw data, menggunakan hari ini: %s", tanggal)

    # beras_medium dan beras_premium: cast ke float
    beras_medium = _to_float(raw.get("beras_medium"), field="beras_medium")
    beras_premium = _to_float(raw.get("beras_premium"), field="beras_premium")

    clean = {
        "tanggal": tanggal,
        "beras_medium": round(beras_medium, 2),
        "beras_premium": round(beras_premium, 2),
    }

    logger.info("Data setelah transformasi: %s", clean)
    return clean


def _to_float(value, field: str, default: Optional[float] = None) -> float:
    """
    Konversi nilai ke float dengan handling format angka Indonesia.
    Contoh: "12.500" (titik = pemisah ribuan) → 12500.0
    """
    if value is None:
        if default is not None:
            return default
        raise ValueError(f"Field '{field}' bernilai None dan tidak ada default")
    try:
        # Handle format angka Indonesia: titik sebagai pemisah ribuan
        cleaned = str(value).strip().replace(".", "").replace(",", ".")
        return float(cleaned)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Tidak bisa konversi field '{field}' nilai '{value}' ke float"
        ) from exc
