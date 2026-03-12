import logging
import time
from datetime import date
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; BulogDataBot/1.0; "
        "+https://bulog.co.id/databot)"
    ),
    "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BACKOFF = 2


def _get_with_retry(url: str, **kwargs) -> requests.Response:
    """GET dengan exponential backoff retry pada transient error."""
    session = requests.Session()
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            wait = RETRY_BACKOFF ** attempt
            logger.warning(
                "Attempt %d/%d gagal untuk %s: %s. Retry dalam %ds",
                attempt, MAX_RETRIES, url, exc, wait
            )
            time.sleep(wait)
    raise RuntimeError(f"Semua {MAX_RETRIES} attempt gagal untuk {url}") from last_exc


def scrape_panel_harga(target_date: Optional[date] = None) -> dict:
    """
    Scrape data harga beras (medium & premium) dari sumber publik.
    Returns dict: {tanggal, beras_medium, beras_premium}

    Strategi:
    1. Coba PIHPS JSON API (hargapangan.id) - paling reliable
    2. Fallback: HTML parse dari BI Panel Harga (bi.go.id/hargapangan)
    """
    if target_date is None:
        target_date = date.today()

    # Attempt 1: PIHPS JSON endpoint (Kemendag)
    try:
        data = _scrape_pihps_json(target_date)
        if data:
            logger.info("Berhasil scrape dari PIHPS JSON untuk tanggal %s", target_date)
            return data
    except Exception as exc:
        logger.warning("PIHPS JSON scrape gagal: %s. Mencoba fallback HTML.", exc)

    # Attempt 2: HTML parsing BI Panel Harga
    try:
        data = _scrape_bi_html(target_date)
        if data:
            logger.info("Berhasil scrape dari BI HTML fallback untuk tanggal %s", target_date)
            return data
    except Exception as exc:
        logger.error("HTML fallback scrape juga gagal: %s", exc)
        raise

    raise RuntimeError(f"Semua strategi scraping gagal untuk tanggal {target_date}")


def _scrape_pihps_json(target_date: date) -> Optional[dict]:
    """
    Scrape dari PIHPS (hargapangan.id) via JSON endpoint.
    Provinsi Jawa Timur, komoditas beras medium & premium.

    Catatan: URL dan parameter perlu diverifikasi di browser dengan inspect network.
    Kode provinsi Jawa Timur = 35, komoditas beras medium = 1, premium = 2.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    url = "https://hargapangan.id/tabel-harga/pasar-tradisional/daerah"
    params = {
        "tanggal": date_str,
        "id_provinsi": "35",
    }

    try:
        response = _get_with_retry(url, params=params)
    except RuntimeError:
        return None

    # Coba parse response sebagai JSON
    try:
        payload = response.json()
    except Exception:
        # Jika bukan JSON, coba parse HTML dari response
        return _parse_hargapangan_html(response.text, date_str)

    data = payload.get("data", [])
    if not data:
        return None

    beras_medium = None
    beras_premium = None

    for record in data:
        name = record.get("komoditas", "").lower()
        price = record.get("harga_rata") or record.get("harga")
        if price is None:
            continue
        try:
            price_float = float(str(price).replace(".", "").replace(",", "."))
        except (ValueError, TypeError):
            continue

        if "medium" in name:
            beras_medium = price_float
        elif "premium" in name:
            beras_premium = price_float

    if beras_medium is None or beras_premium is None:
        return None

    return {
        "tanggal": date_str,
        "beras_medium": beras_medium,
        "beras_premium": beras_premium,
    }


def _parse_hargapangan_html(html: str, date_str: str) -> Optional[dict]:
    """Parse HTML dari hargapangan.id untuk ekstrak harga beras."""
    soup = BeautifulSoup(html, "lxml")

    beras_medium = None
    beras_premium = None

    # Cari semua baris tabel yang mengandung data beras
    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        commodity_text = cells[0].get_text(strip=True).lower()
        if "medium" not in commodity_text and "premium" not in commodity_text:
            continue

        # Ambil kolom harga (biasanya kolom kedua atau ketiga)
        for cell in cells[1:]:
            text = cell.get_text(strip=True)
            # Bersihkan format angka Indonesia: "12.500" → 12500
            cleaned = text.replace(".", "").replace(",", ".").strip()
            try:
                price = float(cleaned)
                if price > 1000:  # Filter nilai yang masuk akal (IDR)
                    if "medium" in commodity_text:
                        beras_medium = price
                    elif "premium" in commodity_text:
                        beras_premium = price
                    break
            except ValueError:
                continue

    if beras_medium is None or beras_premium is None:
        return None

    return {
        "tanggal": date_str,
        "beras_medium": beras_medium,
        "beras_premium": beras_premium,
    }


def _scrape_bi_html(target_date: date) -> Optional[dict]:
    """
    Fallback: parse HTML dari BI Panel Harga Pangan.
    URL: https://www.bi.go.id/hargapangan
    """
    date_str = target_date.strftime("%Y-%m-%d")
    url = "https://www.bi.go.id/hargapangan/TabelHarga/PasarTradisional"

    try:
        response = _get_with_retry(url)
    except RuntimeError:
        return None

    soup = BeautifulSoup(response.text, "lxml")

    beras_medium = None
    beras_premium = None

    # Cari tabel harga
    tables = soup.find_all("table")
    for table in tables:
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            commodity_text = cells[0].get_text(strip=True).lower()
            if "medium" not in commodity_text and "premium" not in commodity_text:
                continue

            for cell in cells[1:]:
                text = cell.get_text(strip=True)
                cleaned = text.replace(".", "").replace(",", ".").strip()
                try:
                    price = float(cleaned)
                    if price > 1000:
                        if "medium" in commodity_text and beras_medium is None:
                            beras_medium = price
                        elif "premium" in commodity_text and beras_premium is None:
                            beras_premium = price
                        break
                except ValueError:
                    continue

    if beras_medium is None or beras_premium is None:
        return None

    return {
        "tanggal": date_str,
        "beras_medium": beras_medium,
        "beras_premium": beras_premium,
    }
