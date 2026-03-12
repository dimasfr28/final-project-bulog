from fastapi import APIRouter, HTTPException, status
from datetime import timedelta
from app.models import LoginRequest, LoginResponse
from app.security import create_access_token, verify_password, hash_password
from app.config import settings
from app.database import get_supabase

router = APIRouter(prefix="/api/auth", tags=["auth"])


def get_user_from_db(username: str):
    """Query user dari tabel Supabase"""
    supabase = get_supabase()
    result = supabase.table("user").select("*").eq("username", username).execute()
    if result.data and len(result.data) > 0:
        return result.data[0]
    return None


@router.post("/login", response_model=LoginResponse)
async def login(credentials: LoginRequest):
    """
    Login endpoint dengan enkripsi password (menggunakan Supabase)
    """
    username = credentials.username
    password = credentials.password

    # Query user dari Supabase
    user = get_user_from_db(username)

    # Validate user exists
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Verify password
    if not verify_password(password, user["password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Create token
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": username}, expires_delta=access_token_expires
    )

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "username": username
    }

@router.get("/verify")
async def verify_token(token: str):
    """
    Verify token validity
    """
    from app.security import verify_token
    try:
        token_data = verify_token(token)
        return {"valid": True, "username": token_data.username}
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )
