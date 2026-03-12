-- =============================================================
-- RPC Functions untuk Dashboard BULOG Jawa Timur
-- Jalankan seluruh file ini di Supabase SQL Editor
--
-- Tabel yang digunakan:
--   harga_beras   (kode_kab_kota INT, tanggal DATE, variant_id, harga, tipe_harga_id INT)
--   outlier_group (id, deskripsi)
--   outlier_detail(id, id_harga_beras, id_group)
--   kota          (kode_kab_kota INT, nama_kab_kota TEXT)
--   tipe_harga    (id INT, nama_tipe TEXT)
--   variant       (variant_id, nama_variant TEXT)
--
-- Asumsi variant:
--   nama_variant ILIKE '%medium%'  → Beras Medium
--   nama_variant ILIKE '%premium%' → Beras Premium
-- =============================================================


-- =============================================================
-- DROP functions yang return type-nya berubah
-- (harus dijalankan sebelum CREATE OR REPLACE)
-- =============================================================
-- Drop semua kemungkinan signature lama
DROP FUNCTION IF EXISTS get_harga_peta(integer);
DROP FUNCTION IF EXISTS get_harga_peta(int);
DROP FUNCTION IF EXISTS get_tren_harga(text, integer, integer, integer);
DROP FUNCTION IF EXISTS get_tren_harga(text, int, int, int);
DROP FUNCTION IF EXISTS get_tren_harga(text, integer);
DROP FUNCTION IF EXISTS get_tren_harga();
DROP FUNCTION IF EXISTS get_bar_harga(text, integer, text, integer);
DROP FUNCTION IF EXISTS get_bar_harga(text, int, text, int);
DROP FUNCTION IF EXISTS get_bar_harga();


-- =============================================================
-- 1. get_kota — daftar kabupaten/kota (diurutkan nama)
--    kode_kab_kota dikembalikan sebagai TEXT agar mudah
--    dipakai sebagai value di frontend select
-- =============================================================
CREATE OR REPLACE FUNCTION get_kota()
RETURNS TABLE(kode_kab_kota TEXT, nama_kab_kota TEXT)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT kode_kab_kota::TEXT, nama_kab_kota
    FROM kota
    ORDER BY nama_kab_kota;
$$;


-- =============================================================
-- 2. get_tipe_harga — daftar tipe harga
-- =============================================================
CREATE OR REPLACE FUNCTION get_tipe_harga()
RETURNS TABLE(id INT, nama_tipe TEXT)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT id, nama_tipe
    FROM tipe_harga
    ORDER BY id;
$$;


-- =============================================================
-- 3. get_latest_harga
--    Cards 1 & 2: harga terakhir Medium & Premium beserta delta H-1
--    p_kode_kab_kota TEXT (akan di-cast ke INT) — NULL = Jawa Timur
-- =============================================================
CREATE OR REPLACE FUNCTION get_latest_harga(
    p_kode_kab_kota TEXT    DEFAULT NULL,
    p_tipe_harga_id INT     DEFAULT NULL
)
RETURNS TABLE(
    variant         TEXT,
    harga           NUMERIC,
    harga_kemarin   NUMERIC,
    delta           NUMERIC,
    tanggal         DATE
)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    WITH tanggal_terakhir AS (
        SELECT MAX(hb.tanggal) AS tgl
        FROM harga_beras hb
        WHERE (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
          AND (p_kode_kab_kota IS NULL OR hb.kode_kab_kota = p_kode_kab_kota::INT)
    ),
    hari_ini AS (
        SELECT
            v.nama_variant,
            AVG(hb.harga) AS harga,
            hb.tanggal
        FROM harga_beras hb
        JOIN variant v ON v.variant_id = hb.variant_id
        JOIN tanggal_terakhir tt ON hb.tanggal = tt.tgl
        WHERE (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
          AND (p_kode_kab_kota IS NULL OR hb.kode_kab_kota = p_kode_kab_kota::INT)
          AND (v.nama_variant ILIKE '%medium%' OR v.nama_variant ILIKE '%premium%')
        GROUP BY v.nama_variant, hb.tanggal
    ),
    tanggal_kemarin AS (
        SELECT MAX(hb.tanggal) AS tgl
        FROM harga_beras hb
        CROSS JOIN tanggal_terakhir tt
        WHERE hb.tanggal < tt.tgl
          AND (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
          AND (p_kode_kab_kota IS NULL OR hb.kode_kab_kota = p_kode_kab_kota::INT)
    ),
    kemarin AS (
        SELECT
            v.nama_variant,
            AVG(hb.harga) AS harga
        FROM harga_beras hb
        JOIN variant v ON v.variant_id = hb.variant_id
        JOIN tanggal_kemarin tk ON hb.tanggal = tk.tgl
        WHERE (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
          AND (p_kode_kab_kota IS NULL OR hb.kode_kab_kota = p_kode_kab_kota::INT)
          AND (v.nama_variant ILIKE '%medium%' OR v.nama_variant ILIKE '%premium%')
        GROUP BY v.nama_variant
    )
    SELECT
        hi.nama_variant                                             AS variant,
        ROUND(hi.harga, 0)                                         AS harga,
        ROUND(k.harga, 0)                                          AS harga_kemarin,
        CASE
            WHEN k.harga IS NULL OR k.harga = 0 THEN NULL
            ELSE ROUND((hi.harga - k.harga) / k.harga, 6)
        END                                                        AS delta,
        hi.tanggal
    FROM hari_ini hi
    LEFT JOIN kemarin k ON k.nama_variant = hi.nama_variant
    ORDER BY
        CASE WHEN hi.nama_variant ILIKE '%medium%' THEN 0 ELSE 1 END;
$$;


-- =============================================================
-- 4. get_minmax_harga_tahun_berjalan
--    Cards 3-6: min/maks Medium & Premium tahun berjalan
--    p_kode_kab_kota TEXT (di-cast ke INT) — NULL = Jawa Timur
-- =============================================================
CREATE OR REPLACE FUNCTION get_minmax_harga_tahun_berjalan(
    p_kode_kab_kota TEXT    DEFAULT NULL,
    p_tipe_harga_id INT     DEFAULT NULL
)
RETURNS TABLE(
    tipe            TEXT,
    harga           NUMERIC,
    nama_kab_kota   TEXT,
    tanggal         DATE,
    selisih         NUMERIC
)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    WITH tahun_ini AS (
        SELECT EXTRACT(YEAR FROM CURRENT_DATE)::INT AS tahun
    ),
    latest_harga AS (
        SELECT
            v.nama_variant,
            ROUND(AVG(hb.harga), 0) AS harga_terakhir
        FROM harga_beras hb
        JOIN variant v ON v.variant_id = hb.variant_id
        WHERE hb.tanggal = (
            SELECT MAX(h2.tanggal) FROM harga_beras h2
            WHERE (p_tipe_harga_id IS NULL OR h2.tipe_harga_id = p_tipe_harga_id)
              AND (p_kode_kab_kota IS NULL OR h2.kode_kab_kota = p_kode_kab_kota::INT)
        )
          AND (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
          AND (p_kode_kab_kota IS NULL OR hb.kode_kab_kota = p_kode_kab_kota::INT)
          AND (v.nama_variant ILIKE '%medium%' OR v.nama_variant ILIKE '%premium%')
        GROUP BY v.nama_variant
    ),
    kandidat AS (
        SELECT
            hb.kode_kab_kota,
            hb.tanggal,
            v.nama_variant,
            AVG(hb.harga) AS harga
        FROM harga_beras hb
        JOIN variant v ON v.variant_id = hb.variant_id
        JOIN tahun_ini ti ON EXTRACT(YEAR FROM hb.tanggal) = ti.tahun
        WHERE (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
          AND (p_kode_kab_kota IS NULL OR hb.kode_kab_kota = p_kode_kab_kota::INT)
          AND (v.nama_variant ILIKE '%medium%' OR v.nama_variant ILIKE '%premium%')
        GROUP BY hb.kode_kab_kota, hb.tanggal, v.nama_variant
    ),
    kandidat_dengan_window AS (
        SELECT
            c.kode_kab_kota,
            c.tanggal,
            c.nama_variant,
            ROUND(c.harga, 0)                            AS harga,
            MIN(c.harga) OVER (PARTITION BY c.nama_variant) AS harga_min,
            MAX(c.harga) OVER (PARTITION BY c.nama_variant) AS harga_max
        FROM kandidat c
    ),
    extremes AS (
        SELECT
            CASE
                WHEN nama_variant ILIKE '%medium%'  AND harga = ROUND(harga_min, 0) THEN 'min_medium'
                WHEN nama_variant ILIKE '%medium%'  AND harga = ROUND(harga_max, 0) THEN 'max_medium'
                WHEN nama_variant ILIKE '%premium%' AND harga = ROUND(harga_min, 0) THEN 'min_premium'
                WHEN nama_variant ILIKE '%premium%' AND harga = ROUND(harga_max, 0) THEN 'max_premium'
            END AS tipe,
            kode_kab_kota,
            tanggal,
            nama_variant,
            harga
        FROM kandidat_dengan_window
        WHERE harga = ROUND(harga_min, 0) OR harga = ROUND(harga_max, 0)
    )
    SELECT DISTINCT ON (e.tipe)
        e.tipe,
        e.harga,
        k.nama_kab_kota,
        e.tanggal,
        ROUND(e.harga - lh.harga_terakhir, 0) AS selisih
    FROM extremes e
    LEFT JOIN kota k ON k.kode_kab_kota = e.kode_kab_kota
    LEFT JOIN latest_harga lh ON lh.nama_variant = e.nama_variant
    WHERE e.tipe IS NOT NULL
    ORDER BY e.tipe, e.tanggal DESC;
$$;


-- =============================================================
-- 5. get_harga_peta
--    Choropleth: harga terakhir per kota, baris per variant
--    Output: satu baris per (kota x variant) sehingga frontend
--    bisa menampilkan warna berdasarkan variant yang dipilih
-- =============================================================
CREATE OR REPLACE FUNCTION get_harga_peta(
    p_tipe_harga_id INT DEFAULT NULL
)
RETURNS TABLE(
    kode_kab_kota   TEXT,
    nama_kab_kota   TEXT,
    nama_variant    TEXT,
    harga           NUMERIC,
    tanggal         DATE
)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    WITH tanggal_terakhir AS (
        SELECT kode_kab_kota, MAX(tanggal) AS tgl
        FROM harga_beras
        WHERE (p_tipe_harga_id IS NULL OR tipe_harga_id = p_tipe_harga_id)
        GROUP BY kode_kab_kota
    ),
    harga_per_kota AS (
        SELECT
            hb.kode_kab_kota,
            hb.tanggal,
            v.nama_variant,
            ROUND(AVG(hb.harga), 0) AS harga
        FROM harga_beras hb
        JOIN variant v ON v.variant_id = hb.variant_id
        JOIN tanggal_terakhir tt
          ON tt.kode_kab_kota = hb.kode_kab_kota AND tt.tgl = hb.tanggal
        WHERE (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
          AND (v.nama_variant ILIKE '%medium%' OR v.nama_variant ILIKE '%premium%')
        GROUP BY hb.kode_kab_kota, hb.tanggal, v.nama_variant
    )
    SELECT
        hp.kode_kab_kota::TEXT,
        k.nama_kab_kota,
        hp.nama_variant,
        hp.harga,
        hp.tanggal
    FROM harga_per_kota hp
    LEFT JOIN kota k ON k.kode_kab_kota = hp.kode_kab_kota
    ORDER BY k.nama_kab_kota, hp.nama_variant;
$$;


-- =============================================================
-- 6. get_tren_harga
--    Line Chart: tren harian per variant + flag outlier
--    Output: satu baris per (tanggal x variant) sehingga
--    frontend bisa filter/render dataset sesuai variant
--    Mendukung p_limit & p_offset untuk pagination (1000/batch)
--    p_kode_kab_kota TEXT (di-cast ke INT) — NULL = Jawa Timur
-- =============================================================
CREATE OR REPLACE FUNCTION get_tren_harga(
    p_kode_kab_kota  TEXT    DEFAULT NULL,
    p_tipe_harga_id  BIGINT  DEFAULT NULL,
    p_limit          INT     DEFAULT 1000,
    p_offset         INT     DEFAULT 0
)
RETURNS TABLE(
    tanggal           DATE,
    nama_variant      VARCHAR,
    harga             NUMERIC,
    is_outlier        BOOLEAN,
    deskripsi_outlier TEXT
)
LANGUAGE plpgsql STABLE SECURITY DEFINER AS $$
BEGIN
    RETURN QUERY
    WITH harga_per_hari AS (
        SELECT
            hb.tanggal,
            v.nama_variant,
            ROUND(AVG(hb.harga),0) AS harga,
            MAX(hb.id)             AS id
        FROM harga_beras hb
        JOIN variant v ON v.variant_id = hb.variant_id
        WHERE (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
          AND (p_kode_kab_kota IS NULL OR hb.kode_kab_kota = p_kode_kab_kota::INT)
          AND (v.nama_variant ILIKE '%medium%' OR v.nama_variant ILIKE '%premium%')
        GROUP BY hb.tanggal, v.nama_variant
    )
    SELECT
        hph.tanggal,
        hph.nama_variant,
        hph.harga,
        (od.id_harga_beras IS NOT NULL) AS is_outlier,
        og.deskripsi                    AS deskripsi_outlier
    FROM harga_per_hari hph
    LEFT JOIN outlier_detail od ON od.id_harga_beras = hph.id
    LEFT JOIN outlier_group og ON og.id = od.id_group
    ORDER BY hph.tanggal, hph.nama_variant
    LIMIT p_limit OFFSET p_offset;
END;
$$;

-- =============================================================
-- 7. get_bar_harga
--    Bar Chart: 4 mode
--      top_highest_date → Top-N tanggal harga tertinggi tahun berjalan
--      top_lowest_date  → Top-N tanggal harga terendah tahun berjalan
--      top_highest_city → Top-N kab/kota harga tertinggi (Jatim only)
--      top_lowest_city  → Top-N kab/kota harga terendah (Jatim only)
--    Output: satu baris per (label x variant) sehingga frontend
--    bisa render dataset dinamis sesuai variant yang ada
--    p_kode_kab_kota TEXT (di-cast ke INT) — NULL = Jawa Timur
-- =============================================================
CREATE OR REPLACE FUNCTION get_bar_harga(
    p_kode_kab_kota TEXT    DEFAULT NULL,
    p_tipe_harga_id INT     DEFAULT NULL,
    p_mode          TEXT    DEFAULT 'top_highest_date',
    p_top_n         INT     DEFAULT 12
)
RETURNS TABLE(
    label        TEXT,
    nama_variant TEXT,
    harga        NUMERIC
)
LANGUAGE plpgsql STABLE SECURITY DEFINER AS $$
BEGIN
    -- -------------------------------------------------------
    -- Mode: top_highest_date
    -- Top-N tanggal dengan rata-rata harga tertinggi (tahun berjalan)
    -- -------------------------------------------------------
    IF p_mode = 'top_highest_date' THEN
        RETURN QUERY
        WITH tahun_ini AS (SELECT EXTRACT(YEAR FROM CURRENT_DATE)::INT AS tahun),
        harian AS (
            SELECT
                hb.tanggal                  AS tgl,
                v.nama_variant              AS varian,
                ROUND(AVG(hb.harga), 0)    AS h
            FROM harga_beras hb
            JOIN variant v ON v.variant_id = hb.variant_id
            JOIN tahun_ini ti ON EXTRACT(YEAR FROM hb.tanggal) = ti.tahun
            WHERE (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
              AND (p_kode_kab_kota IS NULL OR hb.kode_kab_kota = p_kode_kab_kota::INT)
              AND (v.nama_variant ILIKE '%medium%' OR v.nama_variant ILIKE '%premium%')
            GROUP BY hb.tanggal, v.nama_variant
        ),
        harian_with_max AS (
            SELECT tgl, varian, h,
                   MAX(h) OVER (PARTITION BY tgl) AS max_h_per_tgl
            FROM harian
        ),
        top_dates AS (
            SELECT tgl, max_h_per_tgl
            FROM harian_with_max
            GROUP BY tgl, max_h_per_tgl
            ORDER BY max_h_per_tgl DESC NULLS LAST, tgl DESC
            LIMIT p_top_n
        )
        SELECT TO_CHAR(hw.tgl, 'DD Mon YYYY')::TEXT, hw.varian::TEXT, hw.h
        FROM harian_with_max hw
        WHERE hw.tgl IN (SELECT tgl FROM top_dates)
        ORDER BY hw.max_h_per_tgl DESC NULLS LAST, hw.tgl, hw.varian;

    ELSIF p_mode = 'top_lowest_date' THEN
        RETURN QUERY
        WITH tahun_ini AS (SELECT EXTRACT(YEAR FROM CURRENT_DATE)::INT AS tahun),
        harian AS (
            SELECT
                hb.tanggal                  AS tgl,
                v.nama_variant              AS varian,
                ROUND(AVG(hb.harga), 0)    AS h
            FROM harga_beras hb
            JOIN variant v ON v.variant_id = hb.variant_id
            JOIN tahun_ini ti ON EXTRACT(YEAR FROM hb.tanggal) = ti.tahun
            WHERE (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
              AND (p_kode_kab_kota IS NULL OR hb.kode_kab_kota = p_kode_kab_kota::INT)
              AND (v.nama_variant ILIKE '%medium%' OR v.nama_variant ILIKE '%premium%')
            GROUP BY hb.tanggal, v.nama_variant
        ),
        harian_with_min AS (
            SELECT tgl, varian, h,
                   MIN(h) OVER (PARTITION BY tgl) AS min_h_per_tgl
            FROM harian
        ),
        top_dates AS (
            SELECT tgl, min_h_per_tgl
            FROM harian_with_min
            GROUP BY tgl, min_h_per_tgl
            ORDER BY min_h_per_tgl ASC NULLS LAST, tgl DESC
            LIMIT p_top_n
        )
        SELECT TO_CHAR(hw.tgl, 'DD Mon YYYY')::TEXT, hw.varian::TEXT, hw.h
        FROM harian_with_min hw
        WHERE hw.tgl IN (SELECT tgl FROM top_dates)
        ORDER BY hw.min_h_per_tgl ASC NULLS LAST, hw.tgl, hw.varian;

    ELSIF p_mode = 'top_highest_city' THEN
        RETURN QUERY
        WITH tahun_ini AS (SELECT EXTRACT(YEAR FROM CURRENT_DATE)::INT AS tahun),
        per_kota AS (
            SELECT
                k.nama_kab_kota            AS nm_kota,
                v.nama_variant              AS varian,
                ROUND(AVG(hb.harga), 0)    AS h
            FROM harga_beras hb
            JOIN variant v ON v.variant_id = hb.variant_id
            JOIN kota k ON k.kode_kab_kota = hb.kode_kab_kota
            JOIN tahun_ini ti ON EXTRACT(YEAR FROM hb.tanggal) = ti.tahun
            WHERE (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
              AND (v.nama_variant ILIKE '%medium%' OR v.nama_variant ILIKE '%premium%')
            GROUP BY k.nama_kab_kota, v.nama_variant
        ),
        per_kota_with_max AS (
            SELECT nm_kota, varian, h,
                   MAX(h) OVER (PARTITION BY nm_kota) AS max_h_per_kota
            FROM per_kota
        ),
        top_kota AS (
            SELECT nm_kota, max_h_per_kota
            FROM per_kota_with_max
            GROUP BY nm_kota, max_h_per_kota
            ORDER BY max_h_per_kota DESC NULLS LAST
            LIMIT p_top_n
        )
        SELECT pk.nm_kota::TEXT, pk.varian::TEXT, pk.h
        FROM per_kota_with_max pk
        WHERE pk.nm_kota IN (SELECT nm_kota FROM top_kota)
        ORDER BY pk.max_h_per_kota DESC NULLS LAST, pk.nm_kota, pk.varian;

    ELSIF p_mode = 'top_lowest_city' THEN
        RETURN QUERY
        WITH tahun_ini AS (SELECT EXTRACT(YEAR FROM CURRENT_DATE)::INT AS tahun),
        per_kota AS (
            SELECT
                k.nama_kab_kota            AS nm_kota,
                v.nama_variant              AS varian,
                ROUND(AVG(hb.harga), 0)    AS h
            FROM harga_beras hb
            JOIN variant v ON v.variant_id = hb.variant_id
            JOIN kota k ON k.kode_kab_kota = hb.kode_kab_kota
            JOIN tahun_ini ti ON EXTRACT(YEAR FROM hb.tanggal) = ti.tahun
            WHERE (p_tipe_harga_id IS NULL OR hb.tipe_harga_id = p_tipe_harga_id)
              AND (v.nama_variant ILIKE '%medium%' OR v.nama_variant ILIKE '%premium%')
            GROUP BY k.nama_kab_kota, v.nama_variant
        ),
        per_kota_with_min AS (
            SELECT nm_kota, varian, h,
                   MIN(h) OVER (PARTITION BY nm_kota) AS min_h_per_kota
            FROM per_kota
        ),
        top_kota AS (
            SELECT nm_kota, min_h_per_kota
            FROM per_kota_with_min
            GROUP BY nm_kota, min_h_per_kota
            ORDER BY min_h_per_kota ASC NULLS LAST
            LIMIT p_top_n
        )
        SELECT pk.nm_kota::TEXT, pk.varian::TEXT, pk.h
        FROM per_kota_with_min pk
        WHERE pk.nm_kota IN (SELECT nm_kota FROM top_kota)
        ORDER BY pk.min_h_per_kota ASC NULLS LAST, pk.nm_kota, pk.varian;
    END IF;
END;
$$;
