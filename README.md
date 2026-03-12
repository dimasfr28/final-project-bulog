# Dashboard Pemantauan & Prediksi Harga Beras — BULOG Jawa Timur

Aplikasi web full-stack untuk memantau harga beras di seluruh kabupaten/kota Jawa Timur secara real-time, mendeteksi anomali harga, serta melakukan prediksi harga berbasis model SARIMAX.

---

## Daftar Isi

- [Fitur Utama](#fitur-utama)
- [Arsitektur Sistem](#arsitektur-sistem)
- [Struktur Direktori](#struktur-direktori)
- [Tech Stack](#tech-stack)
- [Skema Database](#skema-database)
- [Prasyarat](#prasyarat)
- [Cara Menjalankan](#cara-menjalankan)
  - [Menggunakan Docker Compose](#menggunakan-docker-compose)
  - [Menjalankan Manual (Lokal)](#menjalankan-manual-lokal)
- [Konfigurasi Environment](#konfigurasi-environment)
- [API Endpoints](#api-endpoints)
- [Pipeline Airflow](#pipeline-airflow)
- [Halaman Aplikasi](#halaman-aplikasi)

---

## Fitur Utama

| Fitur | Deskripsi |
|---|---|
| **Autentikasi JWT** | Login dengan token JWT, berlaku 8 jam |
| **Dashboard Utama** | Kartu harga terkini (Medium & Premium), min/maks tahun berjalan, choropleth map Jawa Timur, tren harga harian dengan deteksi outlier, dan bar chart perbandingan kota |
| **Manajemen Data** | Tabel harga beras dengan filter tipe harga, variant, dan rentang tanggal; export ke CSV dan Excel (2 sheet: analisa & data mentah) |
| **Prediksi Harga** | Line chart aktual vs prediksi (mingguan, model SARIMAX), evaluasi model (MAPE, MAE, RMSE), uji stasioneritas (ADF, ARCH), uji residual (Ljung-Box), dan visualisasi lag signifikan ACF/PACF |
| **Pipeline Otomatis** | DAG Airflow untuk scraping harga harian dari Bapanas, deteksi outlier, dan forecasting |

---

## Arsitektur Sistem

```
┌─────────────┐     HTTP/REST      ┌──────────────────┐
│  React SPA  │ ─────────────────► │  FastAPI Backend  │
│  (port 3000)│ ◄───────────────── │  (port 8000)      │
└─────────────┘    JWT Auth        └────────┬─────────┘
                                            │ Supabase Client
                                   ┌────────▼─────────┐
                                   │   Supabase (PG)   │
                                   │   - harga_beras   │
                                   │   - prediksi      │
                                   │   - evaluasi      │
                                   └──────────────────┘
                                            ▲
                                            │ Upsert
                                   ┌────────┴─────────┐
                                   │  Apache Airflow   │
                                   │  (port 8080)      │
                                   │  - scraping       │
                                   │  - outlier        │
                                   │  - forecasting    │
                                   └──────────────────┘

           ┌──────────────────────────────────┐
           │  Nginx Reverse Proxy (port 80)    │
           └──────────────────────────────────┘
```

---

## Struktur Direktori

```
final_project/
├── backend/                    # FastAPI Backend
│   ├── app/
│   │   ├── config.py           # Konfigurasi env & settings
│   │   ├── database.py         # Koneksi Supabase
│   │   ├── models.py           # Pydantic models
│   │   ├── security.py         # JWT encode/decode
│   │   ├── routes_auth.py      # Endpoint autentikasi
│   │   ├── routes_dashboard.py # Endpoint data dashboard
│   │   ├── routes_data.py      # Endpoint manajemen data & export
│   │   └── routes_prediksi.py  # Endpoint prediksi harga
│   ├── main.py                 # Entry point FastAPI
│   ├── requirements.txt
│   ├── Dockerfile
│   └── .env.example
│
├── frontend/                   # React SPA
│   ├── src/
│   │   ├── pages/
│   │   │   ├── Login.jsx
│   │   │   ├── Dashboard.jsx   # Peta, tren, kartu harga
│   │   │   ├── ManageData.jsx  # Tabel data & export
│   │   │   └── PrediksiHarga.jsx # Forecast & evaluasi model
│   │   ├── components/
│   │   │   ├── Sidebar.jsx
│   │   │   └── Navigation.jsx
│   │   ├── services/
│   │   │   ├── authService.js
│   │   │   └── dashboardService.js
│   │   ├── App.jsx
│   │   └── index.jsx
│   ├── public/
│   └── .env.example
│
├── airflow/                    # Apache Airflow
│   ├── dags/
│   │   ├── harga_beras_pipeline.py  # Scraping harga dari Bapanas
│   │   ├── outlier_pipeline.py      # Deteksi outlier
│   │   └── forecast_pipeline.py    # Forecasting SARIMAX
│   ├── Dockerfile.airflow
│   └── requirements.txt
│
├── docker-compose.yml
├── nginx.conf
└── README.md
```

---

## Tech Stack

### Backend
| Komponen | Teknologi |
|---|---|
| Framework | FastAPI 0.104 |
| Server | Uvicorn |
| Autentikasi | JWT (PyJWT + python-jose) |
| Database Client | Supabase Python SDK |
| ORM | SQLAlchemy 2.0 |
| Password Hashing | bcrypt |
| Export | openpyxl (Excel), csv (CSV) |

### Frontend
| Komponen | Teknologi |
|---|---|
| Framework | React 18 |
| Styling | Tailwind CSS |
| Grafik | Chart.js + react-chartjs-2 |
| Peta | Leaflet + react-leaflet |
| Zoom Chart | chartjs-plugin-zoom |
| HTTP Client | Axios (via services) |

### Infrastructure
| Komponen | Teknologi |
|---|---|
| Database | Supabase (PostgreSQL) |
| Pipeline / Scheduler | Apache Airflow 2.x (LocalExecutor) |
| Airflow DB | PostgreSQL 15 (container terpisah) |
| Reverse Proxy | Nginx |
| Container | Docker & Docker Compose |

---

## Skema Database

### Tabel Utama

```sql
-- Data harga beras harian per kab/kota
CREATE TABLE harga_beras (
  id            SERIAL PRIMARY KEY,
  kode_kab_kota INTEGER NOT NULL REFERENCES kota(kode_kab_kota),
  tanggal       DATE NOT NULL,
  variant_id    INTEGER NOT NULL REFERENCES variant(variant_id),
  harga         NUMERIC(10,2),
  tipe_harga_id BIGINT REFERENCES tipe_harga(id)
);

-- Hasil prediksi harga beras (mingguan, tanggal = Senin)
CREATE TABLE hasil_prediksi_harga_beras (
  id            INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  variant_id    BIGINT REFERENCES variant(variant_id),
  tipe_harga    BIGINT REFERENCES tipe_harga(id),
  tanggal       DATE,
  harga         NUMERIC,
  kode_prediksi INTEGER REFERENCES evaluasi_prediksi(kode_prediksi)
);

-- Evaluasi model prediksi
CREATE TABLE evaluasi_prediksi (
  kode_prediksi    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  model            VARCHAR,
  adf_pvalue       NUMERIC,
  arch_pvalue      NUMERIC,
  ljung_box_pvalue NUMERIC,
  acf_signifikan_lag  JSONB,
  pacf_signifikan_lag JSONB,
  mae              REAL,
  mape             REAL,
  rmse             REAL
);
```

### Tabel Referensi
- **`kota`** — daftar kabupaten/kota Jawa Timur (38 kab/kota)
- **`variant`** — variant beras (Beras Medium, Beras Premium, dll.)
- **`tipe_harga`** — tipe harga (Harga Konsumen / Harga Produsen)

---

## Prasyarat

- Docker & Docker Compose v2+
- (Opsional, untuk dev lokal) Python 3.11+ dan Node.js 18+
- Akun Supabase dengan project yang sudah di-setup

---

## Cara Menjalankan

### Menggunakan Docker Compose

**1. Clone dan masuk ke direktori proyek**
```bash
git clone <repo-url>
cd final_project
```

**2. Buat file `.env` di root proyek**
```bash
cp backend/.env.example .env
```

Isi nilai yang diperlukan (lihat [Konfigurasi Environment](#konfigurasi-environment)).

**3. Jalankan semua service**
```bash
docker compose up -d
```

**4. Akses aplikasi**

| Service | URL |
|---|---|
| Frontend (React) | http://localhost:3000 |
| Backend API | http://localhost:8000 |
| API Docs (Swagger) | http://localhost:8000/docs |
| Airflow UI | http://localhost:8080 |
| Nginx (Proxy) | http://localhost |

---

### Menjalankan Manual (Lokal)

#### Backend
```bash
cd backend
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env       # isi nilai env
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

#### Frontend
```bash
cd frontend
npm install
cp .env.example .env       # isi REACT_APP_API_URL
npm start
```

---

## Konfigurasi Environment

Buat file `.env` di direktori `backend/` (salin dari `.env.example`):

```env
# Supabase
SUPABASE_URL=https://<project-id>.supabase.co
SUPABASE_KEY=<your-supabase-anon-key>

# JWT Authentication
SECRET_KEY=<random-string-panjang-minimal-32-karakter>
ALGORITHM=HS256

# Kredensial Login Default
DEFAULT_USERNAME=bulog-jatim
DEFAULT_PASSWORD=<password-aman>

# API Server
API_HOST=0.0.0.0
API_PORT=8000
```

Untuk Airflow, tambahkan di `.env` root:
```env
AIRFLOW_FERNET_KEY=<fernet-key>
AIRFLOW_SECRET_KEY=<secret-key>
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=airflow123
```

---

## API Endpoints

### Autentikasi
| Method | Endpoint | Deskripsi |
|---|---|---|
| `POST` | `/api/auth/login` | Login, mendapatkan JWT token |

### Dashboard (`/api/dashboard`)
> Semua endpoint membutuhkan header `Authorization: Bearer <token>`

| Method | Endpoint | Deskripsi |
|---|---|---|
| `GET` | `/kota` | Daftar kabupaten/kota |
| `GET` | `/tipe-harga` | Daftar tipe harga |
| `GET` | `/latest-harga` | Harga terakhir Medium & Premium + delta H-1 |
| `GET` | `/minmax-harga-tahun-berjalan` | Min/maks harga tahun berjalan |
| `GET` | `/harga-peta` | Harga per kota untuk choropleth map |
| `GET` | `/tren-harga` | Tren harga harian dengan flag outlier |
| `GET` | `/bar-harga` | Data bar chart (top highest/lowest per tanggal/kota) |

### Data (`/api/data`)
| Method | Endpoint | Deskripsi |
|---|---|---|
| `GET` | `/harga-beras` | Tabel data harga beras (dengan filter) |
| `GET` | `/harga-beras/export` | Export data ke CSV |
| `GET` | `/harga-beras/export-excel` | Export data ke Excel (2 sheet) |
| `GET` | `/variant` | Daftar variant beras |

### Prediksi (`/api/prediksi`)
| Method | Endpoint | Deskripsi |
|---|---|---|
| `GET` | `/evaluasi` | Evaluasi model (MAPE, MAE, RMSE, uji statistik, ACF/PACF lag) |
| `GET` | `/chart` | Data aktual (mingguan) + data prediksi untuk line chart |

---

## Pipeline Airflow

Terdapat 3 DAG yang berjalan secara otomatis:

### 1. `harga_beras_pipeline`
Scraping harga beras harian dari **Bapanas** (Badan Pangan Nasional) untuk seluruh 38 kabupaten/kota Jawa Timur, lalu upsert ke tabel `harga_beras` di Supabase. Setelah selesai, men-trigger DAG `outlier_pipeline`.

### 2. `outlier_pipeline`
Mendeteksi anomali harga menggunakan metode statistik dan menyimpan hasilnya ke database.

### 3. `forecast_pipeline`
Pipeline forecasting harga beras dengan tahapan:
```
load_data
  → preprocessing (per 6 dataset, paralel)
    → uji_asumsi (per 6 dataset, paralel)
      → resample_split
        → evaluasi_model
          → final_forecast
            → simpan_database
```
Model yang digunakan: **SARIMAX** — parameter terbaik dipilih berdasarkan evaluasi MAPE, MAE, dan RMSE. Hasil prediksi disimpan ke `hasil_prediksi_harga_beras` dan evaluasinya ke `evaluasi_prediksi`.

---

## Halaman Aplikasi

### Login
Autentikasi single-user dengan username dan password. Token JWT disimpan di `localStorage`.

### Dashboard
- **Kartu Harga Terkini** — Harga terakhir Beras Medium & Premium beserta delta terhadap hari sebelumnya
- **Kartu Min/Maks** — Harga minimum dan maksimum sepanjang tahun berjalan
- **Choropleth Map Jawa Timur** — Visualisasi distribusi harga per kabupaten/kota
- **Line Chart Tren Harga** — Tren harga harian dengan highlight titik outlier
- **Bar Chart** — Perbandingan harga tertinggi/terendah per tanggal atau per kota
- Filter: kabupaten/kota, tipe harga (konsumen/produsen)

### Manage Data
- Tabel harga beras dengan filter tipe harga, variant, dan rentang tanggal
- Perbandingan harga rata-rata vs rata-rata bulan lalu (%)
- Export ke **CSV** atau **Excel** (sheet `analisa` berisi pivot per tanggal, sheet `data` berisi data mentah)

### Prediksi Harga
- **Filter**: tipe harga & variant beras
- **Metrik Evaluasi**: MAPE, MAE, RMSE, nama model terpilih
- **Uji Stasioneritas**: ADF p-value, ARCH p-value
- **Uji Residual**: Ljung-Box p-value
- **Line Chart**: Aktual (mingguan) vs Prediksi
- **Bar Chart ACF & PACF**: Visualisasi lag signifikan
