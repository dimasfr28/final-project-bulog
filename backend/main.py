from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes_auth import router as auth_router
from app.routes_dashboard import router as dashboard_router
from app.routes_data import router as data_router
from app.routes_prediksi import router as prediksi_router
from app.config import settings

app = FastAPI(
    title="BULOG Dashboard API",
    description="API untuk BULOG Jatim Dashboard dengan Autentikasi",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost"],  # Sesuaikan dengan frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(data_router)
app.include_router(prediksi_router)

@app.get("/")
async def root():
    return {
        "message": "BULOG Dashboard API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=True
    )
