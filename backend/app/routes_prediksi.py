from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
from app.security import verify_token
from app.database import get_supabase

router = APIRouter(prefix="/api/prediksi", tags=["prediksi"])
security = HTTPBearer()


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token_data = verify_token(credentials.credentials)
        return token_data.username
    except:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


@router.get("/evaluasi")
async def get_evaluasi(
    username: str = Depends(get_current_user),
    tipe_harga_id: Optional[int] = Query(None),
    variant_id: Optional[int] = Query(None),
):
    """
    Ambil evaluasi model prediksi (MAPE, MAE, RMSE, uji statistik, ACF/PACF lag)
    berdasarkan tipe harga dan variant.
    """
    try:
        supabase = get_supabase()

        # Cari kode_prediksi dari hasil_prediksi_harga_beras
        query = supabase.table("hasil_prediksi_harga_beras").select("kode_prediksi")
        if tipe_harga_id is not None:
            query = query.eq("tipe_harga", tipe_harga_id)
        if variant_id is not None:
            query = query.eq("variant_id", variant_id)

        result = query.limit(1).execute()
        rows = result.data or []

        if not rows or rows[0].get("kode_prediksi") is None:
            return {"data": None}

        kode_prediksi = rows[0]["kode_prediksi"]

        eval_result = supabase.table("evaluasi_prediksi") \
            .select("*") \
            .eq("kode_prediksi", kode_prediksi) \
            .single() \
            .execute()

        return {"data": eval_result.data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/chart")
async def get_chart(
    username: str = Depends(get_current_user),
    tipe_harga_id: Optional[int] = Query(None),
    variant_id: Optional[int] = Query(None),
):
    """
    Ambil data aktual (mingguan, dikonversi dari harian) dan prediksi untuk line chart.
    Aktual: AVG harga per minggu dari harga_beras.
    Prediksi: langsung dari hasil_prediksi_harga_beras.
    """
    try:
        supabase = get_supabase()

        # Data prediksi
        prediksi_query = supabase.table("hasil_prediksi_harga_beras") \
            .select("tanggal, harga") \
            .order("tanggal")
        if tipe_harga_id is not None:
            prediksi_query = prediksi_query.eq("tipe_harga", tipe_harga_id)
        if variant_id is not None:
            prediksi_query = prediksi_query.eq("variant_id", variant_id)

        prediksi_result = prediksi_query.execute()
        prediksi = prediksi_result.data or []

        # Data aktual: gunakan RPC untuk aggregate mingguan
        params = {}
        if tipe_harga_id is not None:
            params["p_tipe_harga_id"] = tipe_harga_id
        if variant_id is not None:
            params["p_variant_id"] = variant_id

        aktual_result = supabase.rpc("get_harga_beras_mingguan", params).execute()
        aktual = aktual_result.data or []

        return {
            "aktual": aktual,
            "prediksi": prediksi,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
