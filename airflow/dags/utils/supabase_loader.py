import logging
import os
from typing import Optional

from supabase import create_client, Client

logger = logging.getLogger(__name__)

_client: Optional[Client] = None


def _get_supabase() -> Client:
    """
    Singleton Supabase client.
    Lazy-init agar tidak crash saat DAG parsing jika env vars belum ada.
    Membaca SUPABASE_URL dan SUPABASE_KEY dari environment variables
    yang di-inject Docker Compose dari file .env.
    """
    global _client
    if _client is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise EnvironmentError(
                "SUPABASE_URL dan SUPABASE_KEY harus tersedia di environment. "
                "Periksa konfigurasi environment di docker-compose.yml."
            )
        _client = create_client(url, key)
        logger.info("Supabase client berhasil diinisialisasi.")
    return _client


def date_exists_in_beras(tanggal: str) -> bool:
    """
    Cek apakah data untuk tanggal tertentu sudah ada di tabel beras.

    Args:
        tanggal: string format YYYY-MM-DD

    Returns:
        True jika data untuk tanggal tersebut sudah ada
    """
    client = _get_supabase()
    result = (
        client
        .table("beras")
        .select("tanggal")
        .eq("tanggal", tanggal)
        .execute()
    )
    return len(result.data) > 0


def upsert_beras(data: dict) -> dict:
    """
    Upsert data harga beras ke tabel beras di Supabase.

    Menggunakan upsert dengan on_conflict="tanggal" sehingga idempotent:
    menjalankan DAG dua kali untuk tanggal yang sama akan overwrite
    dengan data terbaru, tidak menyebabkan duplikat.

    Hanya field beras_medium dan beras_premium yang diisi dari scraping.
    Field hpp dan serapan tidak di-overwrite (diisi manual terpisah).

    Args:
        data: dict dengan keys: tanggal, beras_medium, beras_premium

    Returns:
        Data row yang berhasil diupsert dari Supabase
    """
    client = _get_supabase()

    payload = {
        "tanggal": data["tanggal"],
        "beras_medium": data["beras_medium"],
        "beras_premium": data["beras_premium"],
    }

    logger.info("Melakukan upsert ke tabel beras: %s", payload)

    result = (
        client
        .table("beras")
        .upsert(payload, on_conflict="tanggal")
        .execute()
    )

    if result.data:
        logger.info("Upsert berhasil: %s", result.data)
        return result.data[0]
    else:
        raise RuntimeError(f"Supabase upsert tidak mengembalikan data: {result}")
