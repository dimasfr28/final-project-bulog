import React from 'react';
import { dashboardService, dataService } from '../services/dashboardService';
import { authService } from '../services/authService';

const formatRupiah = (val) =>
  val != null ? `Rp ${Number(val).toLocaleString('id-ID')}` : '-';

const formatPct = (val) => {
  if (val == null) return <span className="text-gray-400">-</span>;
  const num = Number(val);
  const color =
    num > 0 ? 'text-red-600' : num < 0 ? 'text-green-600' : 'text-gray-600';
  return (
    <span className={color}>
      {num > 0 ? '+' : ''}
      {num.toFixed(2)}%
    </span>
  );
};

// Hitung default tanggal: tanggal_end = h-1 jika jam < 14:30, hari ini jika >= 14:30
// tanggal_start = h-5 dari tanggal_end
const getDefaultTanggal = () => {
  const now = new Date();
  const jam = now.getHours() * 60 + now.getMinutes();
  const cutoff = 14 * 60 + 30;

  const end = new Date(now);
  if (jam < cutoff) end.setDate(end.getDate() - 1);

  const start = new Date(end);
  start.setDate(start.getDate() - 5);

  const fmt = (d) => d.toISOString().slice(0, 10);
  return { tanggalStart: fmt(start), tanggalEnd: fmt(end) };
};

export const ManageData = () => {
  const username = authService.getCurrentUser();
  const [tipeHargaList, setTipeHargaList] = React.useState([]);
  const [variantList, setVariantList] = React.useState([]);

  const { tanggalStart: defaultStart, tanggalEnd: defaultEnd } =
    getDefaultTanggal();

  // Default: tipe harga = pasar (id=1), variant = medium (variant_id=1)
  const [filters, setFilters] = React.useState({
    tipeHargaId: null, // diisi setelah tipeHargaList load
    variantId: null, // diisi setelah variantList load
    tanggalStart: defaultStart,
    tanggalEnd: defaultEnd,
  });
  const filtersReady = React.useRef(false);

  const [data, setData] = React.useState([]);
  const [loading, setLoading] = React.useState(false);
  const [exporting, setExporting] = React.useState(false);
  const [total, setTotal] = React.useState(0);

  // Cache: key = JSON.stringify(filters), value = { data, total }
  const cache = React.useRef({});
  const lastLoadedKey = React.useRef(null);

  // Load filter options, lalu set default id dan trigger load data
  React.useEffect(() => {
    Promise.all([
      dashboardService.getTipeHarga(),
      dashboardService.getVariant(),
    ])
      .then(([resTipe, resVariant]) => {
        const tipeList = resTipe.data || [];
        const variantData = resVariant.data || [];

        setTipeHargaList(tipeList);
        setVariantList(variantData);

        // Cari id default: tipe harga pasar
        const defaultTipe = tipeList.find((t) =>
          (t.nama_tipe || '').toLowerCase().includes('pasar')
        );
        // Cari id default: variant medium
        const defaultVariant = variantData.find((v) =>
          (v.nama_variant || '').toLowerCase().includes('medium')
        );

        const initFilters = {
          tipeHargaId: defaultTipe?.id ?? tipeList[0]?.id ?? null,
          variantId:
            defaultVariant?.variant_id ?? variantData[0]?.variant_id ?? null,
          tanggalStart: defaultStart,
          tanggalEnd: defaultEnd,
        };

        setFilters(initFilters);
        filtersReady.current = true;
        loadData(initFilters);
      })
      .catch(console.error);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const loadData = async (f, force = false) => {
    const cacheKey = JSON.stringify({
      tipeHargaId: f.tipeHargaId,
      variantId: f.variantId,
      tanggalStart: f.tanggalStart || null,
      tanggalEnd: f.tanggalEnd || null,
    });

    // Jika filter sama dan tidak di-force, gunakan cache
    if (!force && lastLoadedKey.current === cacheKey && cache.current[cacheKey]) {
      const cached = cache.current[cacheKey];
      setData(cached.data);
      setTotal(cached.total);
      return;
    }

    setLoading(true);
    try {
      const res = await dataService.getHargaBeras({
        tipeHargaId: f.tipeHargaId,
        variantId: f.variantId,
        tanggalStart: f.tanggalStart || undefined,
        tanggalEnd: f.tanggalEnd || undefined,
      });
      const rows = res.data || [];
      const tot = res.total || rows.length;

      // Simpan ke cache
      cache.current[cacheKey] = { data: rows, total: tot };
      lastLoadedKey.current = cacheKey;

      setData(rows);
      setTotal(tot);
    } catch (err) {
      console.error('Error loading harga beras:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleFilter = (key, rawValue) => {
    const value =
      rawValue === ''
        ? null
        : ['tipeHargaId', 'variantId'].includes(key)
          ? Number(rawValue)
          : rawValue;
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  const handleApply = () => loadData(filters);

  const handleReset = () => {
    const { tanggalStart, tanggalEnd } = getDefaultTanggal();
    const tipeDefault = tipeHargaList.find((t) =>
      (t.nama_tipe || '').toLowerCase().includes('pasar')
    );
    const variantDefault = variantList.find((v) =>
      (v.nama_variant || '').toLowerCase().includes('medium')
    );
    const reset = {
      tipeHargaId: tipeDefault?.id ?? tipeHargaList[0]?.id ?? null,
      variantId:
        variantDefault?.variant_id ?? variantList[0]?.variant_id ?? null,
      tanggalStart,
      tanggalEnd,
    };
    setFilters(reset);
    loadData(reset);
  };

  const handleExport = async () => {
    setExporting(true);
    try {
      const tipeHargaNama = tipeHargaList.find((t) => t.id === filters.tipeHargaId)?.nama_tipe || '';
      const variantNama   = variantList.find((v) => v.variant_id === filters.variantId)?.nama_variant || '';
      const blob = await dataService.exportHargaBerasExcel({
        tipeHargaId: filters.tipeHargaId,
        variantId:   filters.variantId,
        tanggalStart: filters.tanggalStart || undefined,
        tanggalEnd:   filters.tanggalEnd   || undefined,
        tipeHargaNama,
        variantNama,
      });
      const url = URL.createObjectURL(blob);
      const a   = document.createElement('a');
      a.href     = url;
      a.download = `harga_beras_${variantNama}_${tipeHargaNama}.xlsx`.replace(/\s+/g, '_');
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error('Export error:', err);
    } finally {
      setExporting(false);
    }
  };

  // Pivot data: per kab/kota × tanggal
  const { kabKotaList, tanggalList, pivotMap, rataRataBulanMap } =
    React.useMemo(() => {
      if (!data.length)
        return {
          kabKotaList: [],
          tanggalList: [],
          pivotMap: {},
          rataRataBulanMap: {},
        };

      const kabMap = new Map();
      const tanggalSet = new Set();
      const pivot = {};
      const bulanMap = {};

      data.forEach((row) => {
        const kode = row.kode_kab_kota;
        const tgl = row.tanggal;
        if (!kabMap.has(kode)) kabMap.set(kode, row.nama_kab_kota || kode);
        tanggalSet.add(tgl);
        if (!pivot[kode]) pivot[kode] = {};
        pivot[kode][tgl] = row.harga_rata_tanggal;
        bulanMap[kode] = row.harga_rata_bulan_lalu;
      });

      const tanggalList = [...tanggalSet].sort();

      // Hitung pct per kab/kota untuk keperluan sorting
      const kabKotaList = [...kabMap.entries()]
        .map(([kode, nama]) => {
          const hargaBulanLalu = bulanMap[kode];
          const validHarga = tanggalList
            .map((tgl) => pivot[kode]?.[tgl])
            .filter((h) => h != null);
          const rata = validHarga.length
            ? validHarga.reduce((a, b) => a + b, 0) / validHarga.length
            : null;
          const pct =
            rata != null && hargaBulanLalu
              ? ((rata - hargaBulanLalu) / hargaBulanLalu) * 100
              : null;
          return { kode, nama, pct };
        })
        .sort((a, b) => Math.abs(b.pct ?? 0) - Math.abs(a.pct ?? 0));

      return {
        kabKotaList,
        tanggalList,
        pivotMap: pivot,
        rataRataBulanMap: bulanMap,
      };
    }, [data]);

  return (
    <div className="bg-gray-50">
      <header className="bg-white border-b border-gray-100 px-4 sm:px-6 lg:px-8 py-4">
        <div className="flex items-center justify-between">
          <div className="pl-10 lg:pl-0">
            <h1 className="text-lg sm:text-xl lg:text-2xl font-bold text-gray-800">
              Dashboard Tabel Data Harga Beras
            </h1>
            <p className="text-gray-500 text-xs sm:text-sm">
              BULOG Jawa Timur — Jawa Timur
              {total > 0 && ` — ${total.toLocaleString('id-ID')} baris`}
            </p>
          </div>
          <div className="flex items-center space-x-3 pl-4 border-l border-gray-200">
            <div className="w-8 h-8 sm:w-10 sm:h-10 bg-gradient-to-br from-blue-500 to-blue-600 rounded-full flex items-center justify-center text-white font-bold text-sm sm:text-base">
              {username ? username.charAt(0).toUpperCase() : 'U'}
            </div>
            <div className="hidden md:block">
              <p className="font-semibold text-gray-800 text-sm">{username || 'User'}</p>
              <p className="text-gray-500 text-xs">Administrator</p>
            </div>
          </div>
        </div>
      </header>

      <div className="p-6">
      <div className="max-w-full mx-auto">

        {/* Filter Bar */}
        <div className="flex flex-wrap gap-3 mb-6 items-end">
          {/* Filter Tipe Harga */}
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-gray-600">
              Tipe Harga
            </label>
            <select
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-indigo-400"
              value={filters.tipeHargaId ?? ''}
              onChange={(e) => handleFilter('tipeHargaId', e.target.value)}
            >
              <option value="">Semua Tipe</option>
              {tipeHargaList.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.nama_tipe}
                </option>
              ))}
            </select>
          </div>

          {/* Filter Variant */}
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-gray-600">Variant</label>
            <select
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-indigo-400"
              value={filters.variantId ?? ''}
              onChange={(e) => handleFilter('variantId', e.target.value)}
            >
              <option value="">Semua Variant</option>
              {variantList.map((v) => (
                <option key={v.variant_id} value={v.variant_id}>
                  {v.nama_variant}
                </option>
              ))}
            </select>
          </div>

          {/* Filter Tanggal */}
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-gray-600">
              Dari Tanggal
            </label>
            <input
              type="date"
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-indigo-400"
              value={filters.tanggalStart}
              onChange={(e) => handleFilter('tanggalStart', e.target.value)}
            />
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-gray-600">
              Sampai Tanggal
            </label>
            <input
              type="date"
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-indigo-400"
              value={filters.tanggalEnd}
              onChange={(e) => handleFilter('tanggalEnd', e.target.value)}
            />
          </div>

          {/* Tombol */}
          <button
            onClick={handleApply}
            disabled={loading}
            className="px-4 py-2 bg-blue-700 text-white rounded-lg text-sm font-medium hover:bg-blue-800 disabled:opacity-50 transition"
          >
            {loading ? 'Memuat...' : 'Terapkan'}
          </button>
          <button
            onClick={handleReset}
            disabled={loading}
            className="px-4 py-2 bg-blue-100 text-blue-700 rounded-lg text-sm font-medium hover:bg-blue-200 disabled:opacity-50 transition"
          >
            Reset
          </button>
          <button
            onClick={() => loadData(filters, true)}
            disabled={loading}
            title="Muat ulang data dari database"
            className="px-3 py-2 bg-gray-100 text-gray-600 rounded-lg text-sm font-medium hover:bg-gray-200 disabled:opacity-50 transition"
          >
            ↻ Refresh
          </button>
          <button
            onClick={handleExport}
            disabled={exporting || loading}
            className="ml-auto px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition"
          >
            {exporting ? 'Mengekspor...' : 'Export Excel'}
          </button>
        </div>

        {/* Tabel */}
        {loading ? (
          <div className="flex justify-center items-center py-20">
            <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-blue-700"></div>
            <span className="ml-3 text-gray-500 text-sm">Memuat data...</span>
          </div>
        ) : data.length === 0 ? (
          <div className="bg-white rounded-lg shadow p-12 text-center text-gray-400">
            Tidak ada data. Ubah filter lalu klik Terapkan.
          </div>
        ) : (
          <div className="rounded-lg shadow overflow-hidden">
          {/* Scroll area — hanya tbody */}
          <div className="overflow-auto max-h-[70vh]">
            <table className="text-sm border-collapse min-w-max w-full">
              <thead>
                <tr className="bg-blue-800 text-white">
                  <th
                    className="px-4 py-3 text-left font-semibold whitespace-nowrap sticky top-0 bg-blue-800 z-10"
                    rowSpan={2}
                  >
                    Kab/Kota
                  </th>
                  <th
                    className="px-4 py-3 text-center font-semibold whitespace-nowrap sticky top-0 bg-blue-800 z-10"
                    rowSpan={2}
                  >
                    Rata-rata Harga Bulan Lalu
                  </th>
                  <th
                    className="px-4 py-3 text-center font-semibold border-l border-blue-600 sticky top-0 bg-blue-800 z-10"
                    colSpan={tanggalList.length}
                  >
                    Tanggal
                  </th>
                  <th
                    className="px-4 py-3 text-center font-semibold border-l border-blue-600 whitespace-nowrap sticky top-0 bg-blue-800 z-10"
                    rowSpan={2}
                  >
                    Rata-rata Harga Terpilih
                  </th>
                  <th
                    className="px-4 py-3 text-center font-semibold whitespace-nowrap sticky top-0 bg-blue-800 z-10"
                    rowSpan={2}
                  >
                    vs Rata-rata Bulan Lalu (%)
                  </th>
                </tr>
                <tr className="bg-blue-700 text-white">
                  {tanggalList.map((tgl) => (
                    <th
                      key={tgl}
                      className="px-3 py-2 text-center font-medium border-l border-blue-600 whitespace-nowrap sticky top-[45px] bg-blue-700 z-10"
                    >
                      {new Date(tgl).toLocaleDateString('id-ID', {
                        day: '2-digit',
                        month: 'short',
                        year: '2-digit',
                      })}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {kabKotaList
                  .filter(({ nama }) => !nama?.toLowerCase().includes('jawa timur'))
                  .map(({ kode, nama }, idx) => {
                    const hargaBulanLalu = rataRataBulanMap[kode];
                    const hargaPerTanggal = tanggalList.map(
                      (tgl) => pivotMap[kode]?.[tgl] ?? null
                    );
                    const validHarga = hargaPerTanggal.filter((h) => h != null);
                    const rataRataTerpilih = validHarga.length
                      ? validHarga.reduce((a, b) => a + b, 0) / validHarga.length
                      : null;
                    const pct =
                      rataRataTerpilih != null && hargaBulanLalu
                        ? ((rataRataTerpilih - hargaBulanLalu) / hargaBulanLalu) * 100
                        : null;

                    return (
                      <tr
                        key={kode}
                        className={`border-b ${idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'} hover:bg-blue-50 transition-colors`}
                      >
                        <td className="px-4 py-3 font-medium text-gray-800 whitespace-nowrap">
                          {nama}
                        </td>
                        <td className="px-4 py-3 text-center text-gray-700">
                          {formatRupiah(hargaBulanLalu)}
                        </td>
                        {hargaPerTanggal.map((h, i) => (
                          <td
                            key={i}
                            className="px-3 py-3 text-center text-gray-700 border-l border-gray-100"
                          >
                            {formatRupiah(h)}
                          </td>
                        ))}
                        <td className="px-4 py-3 text-center font-medium text-gray-800 border-l border-gray-200">
                          {formatRupiah(rataRataTerpilih)}
                        </td>
                        <td className="px-4 py-3 text-center font-medium">
                          {formatPct(pct)}
                        </td>
                      </tr>
                    );
                  })}
              </tbody>
            </table>
          </div>
          {/* Footer tabel — fixed di bawah, tabel terpisah agar tidak ikut scroll */}
          {kabKotaList
            .filter(({ nama }) => nama?.toLowerCase().includes('jawa timur'))
            .map(({ kode, nama }) => {
              const hargaBulanLalu = rataRataBulanMap[kode];
              const hargaPerTanggal = tanggalList.map(
                (tgl) => pivotMap[kode]?.[tgl] ?? null
              );
              const validHarga = hargaPerTanggal.filter((h) => h != null);
              const rataRataTerpilih = validHarga.length
                ? validHarga.reduce((a, b) => a + b, 0) / validHarga.length
                : null;
              const pct =
                rataRataTerpilih != null && hargaBulanLalu
                  ? ((rataRataTerpilih - hargaBulanLalu) / hargaBulanLalu) * 100
                  : null;
              return (
                <div key={kode} className="overflow-x-auto border-t-2 border-blue-700 bg-blue-50">
                  <table className="text-sm border-collapse min-w-max w-full">
                    <tbody>
                      <tr className="font-semibold text-blue-900">
                        <td className="px-4 py-3 whitespace-nowrap">{nama}</td>
                        <td className="px-4 py-3 text-center">{formatRupiah(hargaBulanLalu)}</td>
                        {hargaPerTanggal.map((h, i) => (
                          <td key={i} className="px-3 py-3 text-center border-l border-blue-200">
                            {formatRupiah(h)}
                          </td>
                        ))}
                        <td className="px-4 py-3 text-center border-l border-blue-200">
                          {formatRupiah(rataRataTerpilih)}
                        </td>
                        <td className="px-4 py-3 text-center">{formatPct(pct)}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              );
            })}
          </div>
        )}
      </div>
      </div>
    </div>
  );
};
