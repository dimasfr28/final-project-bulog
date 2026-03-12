from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
from app.security import verify_token
from app.database import get_supabase
from datetime import datetime

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])
security = HTTPBearer()

BATCH_SIZE = 1000


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token_data = verify_token(credentials.credentials)
        return token_data.username
    except:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


def fetch_all_via_rpc(supabase, rpc_name: str, params: dict) -> list:
    """
    Mengambil semua data dari RPC yang mendukung p_limit & p_offset
    dengan loop batch per BATCH_SIZE baris.
    Digunakan untuk tabel besar seperti harga_beras & outlier_detail.
    """
    all_data = []
    offset = 0
    while True:
        batch_params = {**params, "p_limit": BATCH_SIZE, "p_offset": offset}
        result = supabase.rpc(rpc_name, batch_params).execute()
        batch = result.data or []
        all_data.extend(batch)
        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE
    return all_data


# =====================================================
# Filter Options — tabel kecil, query langsung
# =====================================================

@router.get("/kota")
async def get_kota(username: str = Depends(get_current_user)):
    """Daftar kabupaten/kota dari tabel kota"""
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_kota', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tipe-harga")
async def get_tipe_harga(username: str = Depends(get_current_user)):
    """Daftar tipe harga dari tabel tipe_harga"""
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_tipe_harga', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# Cards 1 & 2 — agregasi, hasil kecil
# =====================================================

@router.get("/latest-harga")
async def get_latest_harga(
    kode_kab_kota: Optional[str] = Query(None),
    tipe_harga_id: Optional[int] = Query(None),
    username: str = Depends(get_current_user)
):
    """Harga terakhir Medium & Premium beserta delta H-1"""
    try:
        supabase = get_supabase()
        params = {}
        if kode_kab_kota:
            params['p_kode_kab_kota'] = kode_kab_kota
        if tipe_harga_id is not None:
            params['p_tipe_harga_id'] = tipe_harga_id
        result = supabase.rpc('get_latest_harga', params).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# Cards 3-6 — agregasi, hasil kecil
# =====================================================

@router.get("/minmax-harga-tahun-berjalan")
async def get_minmax_harga_tahun_berjalan(
    kode_kab_kota: Optional[str] = Query(None),
    tipe_harga_id: Optional[int] = Query(None),
    username: str = Depends(get_current_user)
):
    """Min/maks harga tahun berjalan beserta selisih terhadap harga terakhir"""
    try:
        supabase = get_supabase()
        params = {}
        if kode_kab_kota:
            params['p_kode_kab_kota'] = kode_kab_kota
        if tipe_harga_id is not None:
            params['p_tipe_harga_id'] = tipe_harga_id
        result = supabase.rpc('get_minmax_harga_tahun_berjalan', params).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# Choropleth Map — agregasi per kota, hasil kecil
# =====================================================

@router.get("/harga-peta")
async def get_harga_peta(
    tipe_harga_id: Optional[int] = Query(None),
    username: str = Depends(get_current_user)
):
    """Harga terakhir per kabupaten/kota untuk choropleth map"""
    try:
        supabase = get_supabase()
        params = {}
        if tipe_harga_id is not None:
            params['p_tipe_harga_id'] = tipe_harga_id
        result = supabase.rpc('get_harga_peta', params).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# Line Chart — data harian, bisa puluhan ribu baris
# Gunakan pagination loop 1000/batch via RPC
# =====================================================

@router.get("/tren-harga")
async def get_tren_harga(
    kode_kab_kota: Optional[str] = Query(None),
    tipe_harga_id: Optional[int] = Query(None),
    username: str = Depends(get_current_user)
):
    """
    Tren harga harian Medium & Premium dengan flag outlier.
    Data bisa puluhan ribu baris, diambil dengan pagination loop
    per BATCH_SIZE (1000) baris via RPC get_tren_harga.

    RPC harus mendukung parameter: p_kode_kab_kota, p_tipe_harga_id,
    p_limit, p_offset.
    """
    try:
        supabase = get_supabase()
        params = {}
        if kode_kab_kota:
            params['p_kode_kab_kota'] = kode_kab_kota
        if tipe_harga_id is not None:
            params['p_tipe_harga_id'] = tipe_harga_id

        all_data = fetch_all_via_rpc(supabase, 'get_tren_harga', params)
        return {"status": "success", "data": all_data}
    except Exception as e:
        import traceback
        print(f"[ERROR tren-harga] {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# Bar Chart — agregasi, hasil kecil
# =====================================================

@router.get("/bar-harga")
async def get_bar_harga(
    kode_kab_kota: Optional[str] = Query(None),
    tipe_harga_id: Optional[int] = Query(None),
    mode: Optional[str] = Query("top_highest_date"),
    top_n: Optional[int] = Query(12),
    username: str = Depends(get_current_user)
):
    """
    Data bar chart.
    mode: top_highest_date | top_lowest_date | top_highest_city | top_lowest_city
    """
    try:
        supabase = get_supabase()
        params = {'p_mode': mode, 'p_top_n': top_n}
        if kode_kab_kota:
            params['p_kode_kab_kota'] = kode_kab_kota
        if tipe_harga_id is not None:
            params['p_tipe_harga_id'] = tipe_harga_id
        result = supabase.rpc('get_bar_harga', params).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# Legacy Endpoints
# =====================================================

@router.get("/latest-beras-medium")
async def get_latest_beras_medium(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_latest_beras_medium', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/latest-beras-premium")
async def get_latest_beras_premium(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_latest_beras_premium', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/latest-hpp")
async def get_latest_hpp(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_latest_hpp', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/harga-harian-outlier")
async def get_harga_harian_dengan_outlier(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_harga_harian_dengan_outlier', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/harga-tahunan-outlier")
async def get_harga_tahunan_dengan_outlier(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_harga_tahunan_dengan_outlier', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/harga-serapan-harian")
async def get_harga_serapan_harian(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_harga_serapan_harian', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/harga-serapan-tahunan")
async def get_harga_serapan_tahunan(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_harga_serapan_tahunan', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/available-years")
async def get_available_years(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_available_years', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistik-ringkasan")
async def get_statistik_ringkasan(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_statistik_ringkasan', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/descriptive")
async def get_descriptive_analysis(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_statistik_ringkasan', {}).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        return {
            "status": "success",
            "data": {
                "total_records": 1000,
                "average_value": 50.5,
                "min_value": 10.0,
                "max_value": 100.0,
                "last_updated": datetime.now().isoformat()
            }
        }


@router.get("/predictive")
async def get_predictive_analysis(username: str = Depends(get_current_user)):
    return {
        "status": "success",
        "data": {
            "predictions": [
                {"month": "Feb 2026", "predicted_value": 55.5},
                {"month": "Mar 2026", "predicted_value": 58.2},
                {"month": "Apr 2026", "predicted_value": 60.1}
            ]
        }
    }


@router.get("/stats")
async def get_dashboard_stats(username: str = Depends(get_current_user)):
    try:
        supabase = get_supabase()
        result = supabase.rpc('get_statistik_ringkasan', {}).execute()
        data = result.data
        return {
            "status": "success",
            "data": {
                "total_records": data.get('total_data', 0) if data else 0,
                "last_updated": datetime.now().isoformat()
            }
        }
    except:
        return {
            "status": "success",
            "data": {
                "total_records": 1000,
                "last_updated": datetime.now().isoformat()
            }
        }
