"""
Script untuk seed default user ke Supabase
Jalankan: python scripts/seed_user.py
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import get_supabase
from app.security import hash_password
from app.config import settings

def seed_default_user(force_update=False):
    """Insert atau update default user"""
    supabase = get_supabase()

    username = settings.DEFAULT_USERNAME
    password = settings.DEFAULT_PASSWORD

    if not username or not password:
        print("ERROR: DEFAULT_USERNAME atau DEFAULT_PASSWORD tidak diset di .env")
        return False

    # Hash password
    hashed_password = hash_password(password)

    # Cek apakah user sudah ada
    result = supabase.table("user").select("*").eq("username", username).execute()

    if result.data:
        if force_update:
            # Update password
            update_result = supabase.table("user").update({
                "password": hashed_password
            }).eq("username", username).execute()
            print(f"User '{username}' password updated")
            return True
        else:
            print(f"User '{username}' sudah ada di database (gunakan --force untuk update password)")
            return True

    # Insert new user
    insert_result = supabase.table("user").insert({
        "username": username,
        "password": hashed_password
    }).execute()

    if insert_result.data:
        print(f"User '{username}' berhasil ditambahkan ke database")
        return True
    else:
        print(f"Gagal menambahkan user: {insert_result}")
        return False

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Force update password")
    args = parser.parse_args()
    seed_default_user(force_update=args.force)
