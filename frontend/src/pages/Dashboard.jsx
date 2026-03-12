import React from 'react';
import { Line, Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';
import 'hammerjs';
import zoomPlugin from 'chartjs-plugin-zoom';
import { MapContainer, GeoJSON, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';
import { dashboardService } from '../services/dashboardService';
import { authService } from '../services/authService';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler,
  zoomPlugin
);

// Fix leaflet default icon issue with webpack
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
});

const formatCurrency = (num) => {
  if (!num && num !== 0) return 'Rp 0';
  return new Intl.NumberFormat('id-ID', {
    style: 'currency',
    currency: 'IDR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(num);
};

const formatOutlierDescription = (desc) => {
  if (!desc) return ['Data anomali'];

  // Split per baris, hapus baris kosong
  const rawLines = desc.split('\n').map(s => s.trim()).filter(s => s.length > 0);
  if (rawLines.length === 0) return ['Data anomali'];

  const result = [];
  let itemCount = 0;     // jumlah item "-" dalam blok saat ini
  let skipped = 0;       // item yang dilewati (melebihi 3)
  const MAX_ITEMS = 3;

  const flushSkipped = () => {
    if (skipped > 0) {
      result.push(`  ... +${skipped} lainnya`);
      skipped = 0;
    }
  };

  rawLines.forEach(line => {
    const isItem = line.startsWith('-');
    if (!isItem) {
      // Header baru: flush skipped dari blok sebelumnya, reset counter
      flushSkipped();
      itemCount = 0;
      result.push(line);
    } else {
      itemCount++;
      if (itemCount <= MAX_ITEMS) {
        result.push(line);
      } else {
        skipped++;
      }
    }
  });

  flushSkipped();
  return result.length > 0 ? result : ['Data anomali'];
};

// =====================================================
// StatCard Component - untuk Cards 1 & 2
// =====================================================
const StatCard = ({ title, harga, delta, tanggal, colorClass }) => {
  const isUp = delta > 0;
  const isNeutral = delta === 0 || delta === null || delta === undefined;
  const deltaPercent = delta !== null && delta !== undefined
    ? ((Math.abs(delta)) * 100).toFixed(2)
    : null;

  return (
    <div className="bg-white rounded-2xl p-5 shadow-sm border border-gray-100 hover:shadow-md transition-shadow">
      <div className="flex items-start justify-between mb-3">
        <div className={`p-2.5 rounded-xl ${colorClass.bg}`}>
          <svg className={`w-5 h-5 ${colorClass.icon}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2"
              d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" />
          </svg>
        </div>
        {!isNeutral && deltaPercent !== null && (
          <span className={`inline-flex items-center gap-1 px-2 py-1 rounded-lg text-xs font-medium ${
            isUp ? 'bg-red-50 text-red-600' : 'bg-green-50 text-green-600'
          }`}>
            {isUp ? (
              <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M5.293 9.707a1 1 0 010-1.414l4-4a1 1 0 011.414 0l4 4a1 1 0 01-1.414 1.414L11 7.414V15a1 1 0 11-2 0V7.414L6.707 9.707a1 1 0 01-1.414 0z" clipRule="evenodd" />
              </svg>
            ) : (
              <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M14.707 10.293a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 111.414-1.414L9 12.586V5a1 1 0 012 0v7.586l2.293-2.293a1 1 0 011.414 0z" clipRule="evenodd" />
              </svg>
            )}
            {deltaPercent}%
          </span>
        )}
      </div>
      <p className="text-gray-500 text-xs mb-1">{title}</p>
      <p className="text-xl font-bold text-gray-800">{formatCurrency(harga)}</p>
      {tanggal && (
        <p className="text-xs text-gray-400 mt-1">
          Per {new Date(tanggal).toLocaleDateString('id-ID', { day: '2-digit', month: 'short', year: 'numeric' })}
        </p>
      )}
    </div>
  );
};

// =====================================================
// MinMaxCard Component - untuk Cards 3-6
// =====================================================
const MinMaxCard = ({ title, subtitle, harga, selisih, namaKota, tanggal, colorClass, isMax }) => {
  const isPositiveSelisih = selisih >= 0;

  return (
    <div className="bg-white rounded-2xl p-4 shadow-sm border border-gray-100 hover:shadow-md transition-shadow">
      <div className="flex items-start justify-between mb-2">
        <div className={`p-2 rounded-xl ${colorClass.bg}`}>
          <svg className={`w-4 h-4 ${colorClass.icon}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            {isMax ? (
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2"
                d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
            ) : (
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2"
                d="M13 17h8m0 0V9m0 8l-8-8-4 4-6-6" />
            )}
          </svg>
        </div>
        {selisih !== null && selisih !== undefined && (
          <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-xs font-medium ${
            isPositiveSelisih ? 'bg-red-50 text-red-500' : 'bg-green-50 text-green-600'
          }`}>
            {isPositiveSelisih ? '+' : ''}{formatCurrency(selisih)}
          </span>
        )}
      </div>
      <p className="text-gray-400 text-xs">{title}</p>
      {subtitle && <p className="text-gray-500 text-xs font-medium">{subtitle}</p>}
      <p className="text-base font-bold text-gray-800 mt-1">{formatCurrency(harga)}</p>
      {namaKota && <p className="text-xs text-blue-600 mt-0.5 truncate">{namaKota}</p>}
      {tanggal && (
        <p className="text-xs text-gray-400 mt-0.5">
          {new Date(tanggal).toLocaleDateString('id-ID', { day: '2-digit', month: 'short', year: 'numeric' })}
        </p>
      )}
    </div>
  );
};

// =====================================================
// ChoroplethMap Component
// =====================================================
// Normalisasi nama kota: "Kab. Bangkalan" / "Kota Surabaya" → "bangkalan" / "surabaya"
const normNama = (s) =>
  (s || '').toLowerCase().replace(/^(kab\.|kota\.?)\s*/i, '').trim();

function ChoroplethLayer({ geoData, priceData }) {
  const map = useMap();

  // priceData sekarang: [{kode_kab_kota, nama_kab_kota, nama_variant, harga, tanggal}, ...]
  // Group by nama_kab_kota -> { variant -> harga }
  const priceMap = React.useMemo(() => {
    if (!priceData || !Array.isArray(priceData)) return {};
    const m = {};
    priceData.forEach(d => {
      const key = normNama(d.nama_kab_kota);
      if (!m[key]) m[key] = { nama_kab_kota: d.nama_kab_kota, variants: {} };
      m[key].variants[d.nama_variant] = d.harga;
    });
    return m;
  }, [priceData]);

  const getPriceByNama = React.useCallback((kabkot) => {
    if (!kabkot) return null;
    return priceMap[normNama(kabkot)] || null;
  }, [priceMap]);

  // Gunakan rata-rata semua variant sebagai nilai warna
  const getRepresentativeHarga = (priceInfo) => {
    if (!priceInfo) return null;
    const vals = Object.values(priceInfo.variants).filter(v => v != null && v > 0);
    if (vals.length === 0) return null;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  };

  const getColor = React.useCallback((harga) => {
    if (!harga) return '#7ea0e6';
    if (!priceData || priceData.length === 0) return '#0077ff';
    const prices = Object.values(priceMap)
      .map(p => getRepresentativeHarga(p))
      .filter(v => v != null && v > 0);
    if (prices.length === 0) return '#96c5fc';
    const min = Math.min(...prices);
    const max = Math.max(...prices);
    const range = max - min;
    if (range === 0) return '#93c5fd';
    const ratio = (harga - min) / range;
    const r = Math.round(219 - ratio * 150);
    const g = Math.round(234 - ratio * 130);
    const b = Math.round(255 - ratio * 50);
    return `rgb(${r},${g},${b})`;
  }, [priceData, priceMap]);

  const style = React.useCallback((feature) => {
    const kabkot = feature.properties?.kabkot || feature.properties?.nama_kab_kota || feature.properties?.nama;
    const priceInfo = getPriceByNama(kabkot);
    return {
      fillColor: getColor(getRepresentativeHarga(priceInfo)),
      weight: 1,
      opacity: 1,
      color: '#ffffff',
      fillOpacity: 0.85,
    };
  }, [getPriceByNama, getColor]);

  const onEachFeature = React.useCallback((feature, layer) => {
    const kabkot = feature.properties?.kabkot || feature.properties?.nama_kab_kota || feature.properties?.nama;
    const priceInfo = getPriceByNama(kabkot);
    const namaLabel = priceInfo?.nama_kab_kota || kabkot;
    const fmt = (v) => new Intl.NumberFormat('id-ID', { style: 'currency', currency: 'IDR', minimumFractionDigits: 0 }).format(v);

    layer.on({
      mouseover: (e) => {
        const l = e.target;
        l.setStyle({ weight: 2, color: '#3b82f6', fillOpacity: 0.9 });
        l.bringToFront();
        const variantLines = priceInfo
          ? Object.entries(priceInfo.variants)
              .filter(([, h]) => h != null)
              .map(([v, h]) => `${v}: <b>${fmt(h)}</b>`)
              .join('<br/>')
          : 'Data tidak tersedia';
        l.bindTooltip(`
          <div style="font-family:sans-serif;font-size:12px;min-width:150px">
            <strong>${namaLabel}</strong><br/>
            ${variantLines}
          </div>
        `, { sticky: true }).openTooltip();
      },
      mouseout: (e) => {
        const l = e.target;
        l.setStyle({ weight: 1, color: '#ffffff', fillOpacity: 0.85 });
        l.closeTooltip();
      },
    });
  }, [getPriceByNama]);

  // fitBounds ke Jatim saat GeoJSON pertama kali dimuat, lalu tambah 1 level zoom
  React.useEffect(() => {
    if (!geoData) return;
    const bounds = L.geoJSON(geoData).getBounds();
    if (bounds.isValid()) {
      map.fitBounds(bounds, { padding: [10, 10], animate: false });
      setTimeout(() => map.setZoom(map.getZoom() + 1), 50);
    }
  }, [geoData, map]);

  // Label nama kota di centroid setiap polygon
  React.useEffect(() => {
    if (!geoData) return;
    const labelMarkers = [];
    geoData.features.forEach((feature) => {
      const kabkot = feature.properties?.kabkot || feature.properties?.nama_kab_kota || feature.properties?.nama;
      if (!kabkot) return;
      try {
        const layer = L.geoJSON(feature);
        const center = layer.getBounds().getCenter();
        const marker = L.marker(center, {
          icon: L.divIcon({
            className: '',
            html: `<div style="
              font-size:9px;
              font-weight:600;
              color:#1e3a5f;
              text-align:center;
              white-space:nowrap;
              text-shadow:0 0 3px #fff,0 0 3px #fff,0 0 3px #fff;
              pointer-events:none;
              line-height:1.2;
            ">${kabkot}</div>`,
            iconAnchor: [0, 0],
          }),
          interactive: false,
          zIndexOffset: 1000,
        });
        marker.addTo(map);
        labelMarkers.push(marker);
      } catch (_) {}
    });
    return () => {
      labelMarkers.forEach(m => m.remove());
    };
  }, [geoData, map]);

  if (!geoData) return null;
  const priceHash = priceData
    ? priceData.map(d => `${d.kode_kab_kota}:${d.nama_variant}:${d.harga}`).join('|')
    : 'empty';
  return <GeoJSON key={priceHash} data={geoData} style={style} onEachFeature={onEachFeature} />;
}

const ChoroplethMap = ({ priceData }) => {
  const [geoData, setGeoData] = React.useState(null);
  const [geoLoading, setGeoLoading] = React.useState(true);

  React.useEffect(() => {
    // Load GeoJSON dari file lokal (public folder)
    fetch('/jawa-timur.geojson')
      .then(r => {
        if (!r.ok) throw new Error('not ok');
        return r.json();
      })
      .then(data => {
        setGeoData(data);
        setGeoLoading(false);
      })
      .catch(() => setGeoLoading(false));
  }, []);

  if (geoLoading) {
    return (
      <div className="h-[480px] flex items-center justify-center text-gray-400">
        <div className="text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mx-auto mb-2"></div>
          <p className="text-sm">Memuat peta...</p>
        </div>
      </div>
    );
  }

  if (!geoData) {
    return (
      <div className="h-[480px] flex items-center justify-center text-gray-400">
        <p className="text-sm">Gagal memuat data peta</p>
      </div>
    );
  }

  // Bounding box Jawa Timur (sedikit lebih lebar dari polygon)
  const jatimBounds = L.latLngBounds(
    L.latLng(-8.9, 110.9),  // SW
    L.latLng(-6.8, 115.0)   // NE
  );

  return (
    <div className="h-[480px] rounded-xl overflow-hidden" style={{ background: '#ebffed' }}>
      <MapContainer
        center={[-7.85, 112.9]}
        zoom={8}
        minZoom={7}
        maxZoom={12}
        maxBounds={jatimBounds}
        maxBoundsViscosity={1.0}
        style={{ height: '100%', width: '100%', background: '#ebffed' }}
        zoomControl={true}
        scrollWheelZoom={false}
      >
        <ChoroplethLayer geoData={geoData} priceData={priceData} />
      </MapContainer>
    </div>
  );
};

// =====================================================
// Main Dashboard Component
// =====================================================
export const Dashboard = () => {
  const username = authService.getCurrentUser();

  // Filter states
  const [kotaList, setKotaList] = React.useState([]);
  const [tipeHargaList, setTipeHargaList] = React.useState([]);
  const [selectedKota, setSelectedKota] = React.useState(''); // '' = Jawa Timur
  const [selectedTipeHarga, setSelectedTipeHarga] = React.useState(null); // null = default

  // Loading states
  const [loadingFilters, setLoadingFilters] = React.useState(true);
  const [loadingCards, setLoadingCards] = React.useState(false);
  const [loadingCharts, setLoadingCharts] = React.useState(false);

  // Cards 1 & 2
  const [latestHarga, setLatestHarga] = React.useState(null);

  // Cards 3-6
  const [minMaxHarga, setMinMaxHarga] = React.useState(null);

  // Choropleth
  const [hargaPeta, setHargaPeta] = React.useState(null);

  // Line Chart
  const [trenHarga, setTrenHarga] = React.useState(null);

  // Bar Chart
  const [barMode, setBarMode] = React.useState('top_highest_date');
  const [barHarga, setBarHarga] = React.useState(null);
  const [loadingBar, setLoadingBar] = React.useState(false);

  const isJatim = !selectedKota;

  // Load filter options on mount
  React.useEffect(() => {
    const loadFilters = async () => {
      try {
        const [kotaRes, tipeRes] = await Promise.all([
          dashboardService.getKota(),
          dashboardService.getTipeHarga(),
        ]);
        setKotaList(kotaRes?.data || []);
        const tipeList = tipeRes?.data || [];
        setTipeHargaList(tipeList);
        // Set default tipe harga = harga_pasar
        const defaultTipe = tipeList.find(t => t.nama_tipe === 'harga_pasar');
        if (defaultTipe) setSelectedTipeHarga(defaultTipe.id);
        else if (tipeList.length > 0) setSelectedTipeHarga(tipeList[0].id);
      } catch (_err) {
        // silently handle error
      } finally {
        setLoadingFilters(false);
      }
    };
    loadFilters();
  }, []);

  const loadDashboardData = React.useCallback(async () => {
    setLoadingCards(true);
    setLoadingCharts(true);

    // Cards + map: jika satu gagal yang lain tetap jalan
    const [latestRes, minMaxRes, petaRes] = await Promise.allSettled([
      dashboardService.getLatestHarga(selectedKota || null, selectedTipeHarga),
      dashboardService.getMinMaxHargaTahunBerjalan(selectedKota || null, selectedTipeHarga),
      dashboardService.getHargaPeta(selectedTipeHarga),
    ]);
    if (latestRes.status === 'fulfilled') setLatestHarga(latestRes.value?.data);
    else console.error('[DEBUG] latestHarga error:', latestRes.reason);
    if (minMaxRes.status === 'fulfilled') setMinMaxHarga(minMaxRes.value?.data);
    else console.error('[DEBUG] minMaxHarga error:', minMaxRes.reason);
    if (petaRes.status === 'fulfilled') setHargaPeta(petaRes.value?.data);
    else console.error('[DEBUG] hargaPeta error:', petaRes.reason);

    setLoadingCards(false);

    // Tren harga: query besar, load terpisah agar tidak blokir cards
    try {
      const trenRes = await dashboardService.getTrenHarga(selectedKota || null, selectedTipeHarga);
      setTrenHarga(trenRes?.data);
    } catch (err) {
      console.error('[DEBUG] trenHarga error:', err);
    } finally {
      setLoadingCharts(false);
    }
  }, [selectedKota, selectedTipeHarga]);

  const loadBarData = React.useCallback(async () => {
    setLoadingBar(true);
    try {
      const res = await dashboardService.getBarHarga(selectedKota || null, selectedTipeHarga, barMode);
      console.log('[DEBUG] barHarga:', res?.data?.slice(0, 5));
      setBarHarga(res?.data);
    } catch (err) {
      console.error('[DEBUG] loadBarData error:', err);
    } finally {
      setLoadingBar(false);
    }
  }, [selectedKota, selectedTipeHarga, barMode]);

  // Load semua data ketika filter kota/tipe berubah (setelah filter siap)
  React.useEffect(() => {
    if (loadingFilters) return;
    // Jika kota berubah ke bukan Jatim dan mode adalah city mode, reset ke top_highest_date
    // setBarMode akan memicu useEffect barMode yang memanggil loadBarData
    if (selectedKota && (barMode === 'top_highest_city' || barMode === 'top_lowest_city')) {
      setBarMode('top_highest_date');
    } else {
      loadBarData();
    }
    loadDashboardData();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedKota, selectedTipeHarga, loadingFilters]);

  // Load bar saja ketika mode bar berubah (filter sudah pasti sudah siap)
  React.useEffect(() => {
    if (loadingFilters || selectedTipeHarga === null) return;
    loadBarData();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [barMode]);

  // Derived card data
  // get_latest_harga mengembalikan array: [medium row, premium row] diurutkan medium dulu
  const mediumLatest = Array.isArray(latestHarga)
    ? latestHarga.find(d => d.variant?.toLowerCase().includes('medium'))
    : latestHarga?.medium;
  const premiumLatest = Array.isArray(latestHarga)
    ? latestHarga.find(d => d.variant?.toLowerCase().includes('premium'))
    : latestHarga?.premium;

  // get_minmax_harga_tahun_berjalan mengembalikan array dengan field tipe:
  // 'min_medium' | 'max_medium' | 'min_premium' | 'max_premium'
  const mediumMin  = Array.isArray(minMaxHarga) ? minMaxHarga.find(d => d.tipe === 'min_medium')  : null;
  const premiumMin = Array.isArray(minMaxHarga) ? minMaxHarga.find(d => d.tipe === 'min_premium') : null;
  const mediumMax  = Array.isArray(minMaxHarga) ? minMaxHarga.find(d => d.tipe === 'max_medium')  : null;
  const premiumMax = Array.isArray(minMaxHarga) ? minMaxHarga.find(d => d.tipe === 'max_premium') : null;

  // =====================================================
  // Line Chart
  // =====================================================
  const DEFAULT_VISIBLE = 10;

  // trenHarga sekarang: [{tanggal, nama_variant, harga, is_outlier, deskripsi_outlier}, ...]
  // Group by tanggal, lalu pisah per variant
  const trenHargaGrouped = React.useMemo(() => {
    if (!trenHarga || !Array.isArray(trenHarga) || trenHarga.length === 0) return null;
    // Kumpulkan semua tanggal unik (urut)
    const tanggalSet = [...new Set(trenHarga.map(d => d.tanggal))].sort();
    // Kumpulkan semua variant unik
    const variantSet = [...new Set(trenHarga.map(d => d.nama_variant))].sort();
    // Map: tanggal -> variant -> data
    const map = {};
    trenHarga.forEach(d => {
      if (!map[d.tanggal]) map[d.tanggal] = {};
      map[d.tanggal][d.nama_variant] = d;
    });
    return { tanggalSet, variantSet, map };
  }, [trenHarga]);

  const VARIANT_COLORS = {
    medium:  { border: 'rgba(59, 130, 246, 1)',  bg: 'rgba(59, 130, 246, 0.1)',  point: 'rgba(59, 130, 246, 1)'  },
    premium: { border: 'rgba(16, 185, 129, 1)',  bg: 'rgba(16, 185, 129, 0.1)',  point: 'rgba(16, 185, 129, 1)'  },
  };

  const getVariantColor = (nama_variant) => {
    if (nama_variant?.toLowerCase().includes('medium'))  return VARIANT_COLORS.medium;
    if (nama_variant?.toLowerCase().includes('premium')) return VARIANT_COLORS.premium;
    return { border: 'rgba(107,114,128,1)', bg: 'rgba(107,114,128,0.1)', point: 'rgba(107,114,128,1)' };
  };

  const lineChartData = React.useMemo(() => {
    if (!trenHargaGrouped) return null;
    const { tanggalSet, variantSet, map } = trenHargaGrouped;
    const labels = tanggalSet.map(t => {
      const dt = new Date(t);
      return dt.toLocaleDateString('id-ID', { day: '2-digit', month: 'short', year: 'numeric' });
    });
    const datasets = variantSet.map((variant, idx) => {
      const c = getVariantColor(variant);
      const rows = tanggalSet.map(t => map[t]?.[variant] || null);
      // dataset pertama pakai axis kiri (y), berikutnya axis kanan (y1)
      const yAxisID = idx === 0 ? 'y' : 'y1';
      return {
        label: variant,
        data: rows.map(r => r?.harga ?? null),
        yAxisID,
        borderColor: c.border,
        backgroundColor: c.bg,
        borderWidth: 1.5,
        tension: 0.4,
        fill: false,
        pointBackgroundColor: rows.map(r => r?.is_outlier ? 'rgba(239, 68, 68, 1)' : c.point),
        pointRadius: rows.map(r => r?.is_outlier ? 6 : 3),
        pointHoverRadius: rows.map(r => r?.is_outlier ? 8 : 5),
        pointBorderWidth: rows.map(r => r?.is_outlier ? 2 : 1),
        pointBorderColor: rows.map(r => r?.is_outlier ? 'rgba(239,68,68,0.5)' : c.border),
      };
    });
    const outlierInfo = tanggalSet.map(t => {
      const entry = {};
      variantSet.forEach(v => { entry[v] = map[t]?.[v] || null; });
      return entry;
    });
    return { labels, datasets, outlierInfo, variantSet };
  }, [trenHargaGrouped]);

  const lineTotal = trenHargaGrouped?.tanggalSet?.length || 0;


  const lineChartOptions = React.useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'top', labels: { usePointStyle: true, padding: 15 } },
        lineValueLabel: {},
        zoom: lineTotal > DEFAULT_VISIBLE ? {
          pan: { enabled: true, mode: 'x' },
          limits: { x: { min: 0, max: lineTotal - 1, minRange: 7 } },
        } : { pan: { enabled: false } },
        tooltip: {
          backgroundColor: 'rgba(255, 255, 255, 0.97)',
          titleColor: '#1f2937',
          bodyColor: '#4b5563',
          borderColor: '#e5e7eb',
          borderWidth: 1,
          padding: 12,
          boxPadding: 4,
          callbacks: {
            label: (context) => `${context.dataset.label}: ${formatCurrency(context.parsed.y)}`,
            afterLabel: (context) => {
              if (!lineChartData?.outlierInfo) return [];
              const info = lineChartData.outlierInfo[context.dataIndex];
              const variantName = lineChartData.variantSet?.[context.datasetIndex];
              const row = info?.[variantName];
              if (row?.is_outlier) {
                const lines = formatOutlierDescription(row.deskripsi_outlier);
                return ['', '⚠️ OUTLIER:', ...lines];
              }
              return [];
            },
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          min: lineTotal > DEFAULT_VISIBLE ? lineTotal - DEFAULT_VISIBLE : undefined,
          max: lineTotal > DEFAULT_VISIBLE ? lineTotal - 1 : undefined,
          ticks: { autoSkip: true, maxTicksLimit: 20, maxRotation: 45, font: { size: 10 } },
        },
        y: {
          position: 'left',
          grid: { color: '#f3f4f6' },
          ticks: { callback: (value) => formatCurrency(value), font: { size: 10 } },
          title: { display: true, text: lineChartData?.variantSet?.[0] || '', font: { size: 10 }, color: 'rgba(59,130,246,1)' },
          afterDataLimitsUpdate: (scale) => {
            const chart = scale.chart;
            const xScale = chart.scales.x;
            if (!xScale) return;
            const minX = Math.max(0, Math.floor(xScale.min));
            const maxX = Math.min(chart.data.labels.length - 1, Math.ceil(xScale.max));
            let visMin = Infinity, visMax = -Infinity;
            chart.data.datasets.forEach((ds) => {
              if (ds.yAxisID !== 'y') return;
              for (let i = minX; i <= maxX; i++) {
                const v = ds.data[i];
                if (v != null && !isNaN(v)) { if (v < visMin) visMin = v; if (v > visMax) visMax = v; }
              }
            });
            if (visMin !== Infinity && visMax !== -Infinity) {
              const range = visMax - visMin;
              const padding = Math.max(range * 0.1, 50);
              scale.min = Math.floor(visMin - padding);
              scale.max = Math.ceil(visMax + padding);
            }
          },
        },
        y1: {
          position: 'right',
          grid: { drawOnChartArea: false },
          ticks: { callback: (value) => formatCurrency(value), font: { size: 10 } },
          title: { display: true, text: lineChartData?.variantSet?.[1] || '', font: { size: 10 }, color: 'rgba(16,185,129,1)' },
          afterDataLimitsUpdate: (scale) => {
            const chart = scale.chart;
            const xScale = chart.scales.x;
            if (!xScale) return;
            const minX = Math.max(0, Math.floor(xScale.min));
            const maxX = Math.min(chart.data.labels.length - 1, Math.ceil(xScale.max));
            let visMin = Infinity, visMax = -Infinity;
            chart.data.datasets.forEach((ds) => {
              if (ds.yAxisID !== 'y1') return;
              for (let i = minX; i <= maxX; i++) {
                const v = ds.data[i];
                if (v != null && !isNaN(v)) { if (v < visMin) visMin = v; if (v > visMax) visMax = v; }
              }
            });
            if (visMin !== Infinity && visMax !== -Infinity) {
              const range = visMax - visMin;
              const padding = Math.max(range * 0.1, 50);
              scale.min = Math.floor(visMin - padding);
              scale.max = Math.ceil(visMax + padding);
            }
          },
        },
      },
    }),
    [lineTotal, lineChartData]
  );

  // =====================================================
  // Bar Chart
  // barHarga sekarang: [{label, nama_variant, harga}, ...]
  // =====================================================
  const barChartData = React.useMemo(() => {
    if (!barHarga || !Array.isArray(barHarga) || barHarga.length === 0) return null;
    // Kumpulkan label unik (urutan kemunculan pertama)
    const labelSet = [...new Set(barHarga.map(d => d.label))];
    // Kumpulkan variant unik
    const variantSet = [...new Set(barHarga.map(d => d.nama_variant))].sort();
    // Map: label -> variant -> harga
    const map = {};
    barHarga.forEach(d => {
      if (!map[d.label]) map[d.label] = {};
      map[d.label][d.nama_variant] = d.harga;
    });
    const BAR_COLORS = [
      'rgba(59, 130, 246, 0.8)',
      'rgba(16, 185, 129, 0.8)',
      'rgba(245, 158, 11, 0.8)',
      'rgba(239, 68, 68, 0.8)',
    ];
    const datasets = variantSet.map((variant, i) => ({
      label: variant,
      data: labelSet.map(lbl => map[lbl]?.[variant] ?? null),
      backgroundColor: BAR_COLORS[i % BAR_COLORS.length],
      borderRadius: 4,
      yAxisID: i === 0 ? 'y' : 'y1',
    }));
    return { labels: labelSet, datasets, variantSet };
  }, [barHarga]);

  const lineValueLabelPlugin = React.useMemo(() => ({
    id: 'lineValueLabel',
    afterDatasetsDraw(chart) {
      const { ctx } = chart;
      const COLORS = [
        { bg: 'rgba(59,130,246,0.12)', border: 'rgba(59,130,246,0.7)', text: '#1d4ed8' },
        { bg: 'rgba(16,185,129,0.12)', border: 'rgba(16,185,129,0.7)', text: '#065f46' },
      ];
      const xScale = chart.scales.x;
      const visMin = Math.floor(xScale.min);
      const visMax = Math.ceil(xScale.max);
      const visRange = visMax - visMin;
      if (visRange > 60) return;
      const skipStep = visRange > 30 ? 3 : visRange > 14 ? 2 : 1;
      const boxH = 14;
      const OFFSET = 6;
      // dsIndex 0 = medium → selalu di bawah, dsIndex 1 = premium → selalu di atas
      const ABOVE_BY_INDEX = [false, true];

      const drawPill = (point, label, above, color) => {
        ctx.save();
        ctx.font = 'bold 9px sans-serif';
        const textW = ctx.measureText(label).width;
        const padX = 5;
        const boxW = textW + padX * 2;
        const x = point.x - boxW / 2;
        const y = above ? point.y - boxH - OFFSET : point.y + OFFSET;
        const r = 4;
        ctx.beginPath();
        ctx.moveTo(x + r, y);
        ctx.lineTo(x + boxW - r, y);
        ctx.arcTo(x + boxW, y, x + boxW, y + r, r);
        ctx.lineTo(x + boxW, y + boxH - r);
        ctx.arcTo(x + boxW, y + boxH, x + boxW - r, y + boxH, r);
        ctx.lineTo(x + r, y + boxH);
        ctx.arcTo(x, y + boxH, x, y + boxH - r, r);
        ctx.lineTo(x, y + r);
        ctx.arcTo(x, y, x + r, y, r);
        ctx.closePath();
        ctx.fillStyle = color.bg;
        ctx.fill();
        ctx.strokeStyle = color.border;
        ctx.lineWidth = 1;
        ctx.stroke();
        ctx.fillStyle = color.text;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(label, point.x, y + boxH / 2);
        ctx.restore();
      };

      chart.data.datasets.forEach((dataset, dsIndex) => {
        const meta = chart.getDatasetMeta(dsIndex);
        if (meta.hidden) return;
        const color = COLORS[dsIndex] || COLORS[0];
        const above = ABOVE_BY_INDEX[dsIndex] ?? true;

        meta.data.forEach((point, index) => {
          if (index < visMin || index > visMax) return;
          if ((index - visMin) % skipStep !== 0) return;
          const value = dataset.data[index];
          if (value == null) return;

          const label = new Intl.NumberFormat('id-ID', {
            style: 'currency', currency: 'IDR',
            minimumFractionDigits: 0, maximumFractionDigits: 0,
          }).format(value);

          drawPill(point, label, above, color);
        });
      });
    },
  }), []);

  const barValueLabelPlugin = React.useMemo(() => ({
    id: 'barValueLabel',
    afterDatasetsDraw(chart) {
      const { ctx } = chart;
      const COLORS = [
        { bg: 'rgba(59,130,246,0.15)', border: 'rgba(59,130,246,0.8)', text: '#1d4ed8' },
        { bg: 'rgba(16,185,129,0.15)', border: 'rgba(16,185,129,0.8)', text: '#065f46' },
      ];
      chart.data.datasets.forEach((dataset, datasetIndex) => {
        const meta = chart.getDatasetMeta(datasetIndex);
        if (meta.hidden) return;
        const color = COLORS[datasetIndex] || COLORS[0];
        meta.data.forEach((bar, index) => {
          const value = dataset.data[index];
          if (value == null) return;
          const label = new Intl.NumberFormat('id-ID', {
            style: 'currency', currency: 'IDR',
            minimumFractionDigits: 0, maximumFractionDigits: 0,
          }).format(value);
          ctx.save();
          ctx.font = 'bold 9px sans-serif';
          const textW = ctx.measureText(label).width;
          const padX = 5, padY = 3;
          const boxW = textW + padX * 2;
          const boxH = 14;
          const x = bar.x - boxW / 2;
          const y = bar.y - boxH - 4;
          // background pill
          const r = 4;
          ctx.beginPath();
          ctx.moveTo(x + r, y);
          ctx.lineTo(x + boxW - r, y);
          ctx.arcTo(x + boxW, y, x + boxW, y + r, r);
          ctx.lineTo(x + boxW, y + boxH - r);
          ctx.arcTo(x + boxW, y + boxH, x + boxW - r, y + boxH, r);
          ctx.lineTo(x + r, y + boxH);
          ctx.arcTo(x, y + boxH, x, y + boxH - r, r);
          ctx.lineTo(x, y + r);
          ctx.arcTo(x, y, x + r, y, r);
          ctx.closePath();
          ctx.fillStyle = color.bg;
          ctx.fill();
          ctx.strokeStyle = color.border;
          ctx.lineWidth = 1;
          ctx.stroke();
          // text
          ctx.fillStyle = color.text;
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          ctx.fillText(label, bar.x, y + boxH / 2);
          ctx.restore();
        });
      });
    },
  }), []);

  const barYRange = React.useMemo(() => {
    if (!barHarga || barHarga.length === 0) return {};
    const result = {};
    const variantSet = [...new Set(barHarga.map(d => d.nama_variant))].sort();
    variantSet.forEach((variant, idx) => {
      const key = idx === 0 ? 'y' : 'y1';
      const vals = barHarga.filter(d => d.nama_variant === variant && d.harga != null).map(d => d.harga);
      if (vals.length === 0) return;
      result[key] = { min: Math.floor(Math.min(...vals)) - 50, max: Math.ceil(Math.max(...vals)) + 50 };
    });
    return result;
  }, [barHarga]);

  const barChartOptions = React.useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: { top: 24 } },
      plugins: {
        legend: { position: 'top', labels: { usePointStyle: true, padding: 15 } },
        barValueLabel: {},
        tooltip: {
          backgroundColor: 'rgba(255, 255, 255, 0.95)',
          titleColor: '#1f2937',
          bodyColor: '#4b5563',
          borderColor: '#e5e7eb',
          borderWidth: 1,
          callbacks: {
            label: (context) => `${context.dataset.label}: ${formatCurrency(context.parsed.y)}`,
          },
        },
      },
      scales: {
        x: { grid: { display: false } },
        y: {
          position: 'left',
          grid: { color: '#f3f4f6' },
          min: barYRange.y?.min,
          max: barYRange.y?.max,
          ticks: { callback: (value) => formatCurrency(value), font: { size: 10 } },
          title: { display: true, text: barChartData?.variantSet?.[0] || '', font: { size: 10 }, color: 'rgba(59,130,246,1)' },
        },
        y1: {
          position: 'right',
          grid: { drawOnChartArea: false },
          min: barYRange.y1?.min,
          max: barYRange.y1?.max,
          ticks: { callback: (value) => formatCurrency(value), font: { size: 10 } },
          title: { display: true, text: barChartData?.variantSet?.[1] || '', font: { size: 10 }, color: 'rgba(16,185,129,1)' },
        },
      },
    }),
    [barYRange, barChartData]
  );

  const selectedKotaNama = kotaList.find(k => k.kode_kab_kota === selectedKota)?.nama_kab_kota || 'Jawa Timur';
  const tipeHargaNama = tipeHargaList.find(t => t.id === selectedTipeHarga)?.nama_tipe || '';

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-100 px-4 sm:px-6 lg:px-8 py-4">
        <div className="flex items-center justify-between">
          <div className="pl-10 lg:pl-0">
            <h1 className="text-lg sm:text-xl lg:text-2xl font-bold text-gray-800">
              Dashboard Analisa Harga Beras
            </h1>
            <p className="text-gray-500 text-xs sm:text-sm">
              BULOG Jawa Timur — {selectedKotaNama}
              {tipeHargaNama && ` · ${tipeHargaNama.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}`}
            </p>
          </div>
          <div className="flex items-center space-x-4">
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
        </div>
      </header>

      <div className="p-4 sm:p-6 lg:p-8 space-y-6">

        {/* Row 1: Filters + Cards 1 & 2 */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4">
          {/* Filter Panel */}
          <div className="lg:col-span-1 bg-white rounded-2xl p-5 shadow-sm border border-gray-100 flex flex-col gap-5">
            {/* Filter Kota */}
            <div>
              <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
                Kota / Kabupaten
              </label>
              <select
                value={selectedKota}
                onChange={(e) => setSelectedKota(e.target.value)}
                className="w-full text-sm border border-gray-200 rounded-xl px-3 py-2.5 text-gray-700 bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-300 focus:border-blue-400 transition"
                disabled={loadingFilters}
              >
                <option value="">Jawa Timur (Semua)</option>
                {kotaList.map((k) => (
                  <option key={k.kode_kab_kota} value={k.kode_kab_kota}>
                    {k.nama_kab_kota}
                  </option>
                ))}
              </select>
            </div>

            {/* Filter Tipe Harga */}
            <div>
              <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
                Tipe Harga
              </label>
              <select
                value={selectedTipeHarga ?? ''}
                onChange={(e) => setSelectedTipeHarga(e.target.value ? Number(e.target.value) : null)}
                className="w-full text-sm border border-gray-200 rounded-xl px-3 py-2.5 text-gray-700 bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-300 focus:border-blue-400 transition"
                disabled={loadingFilters}
              >
                {tipeHargaList.map((t) => (
                  <option key={t.id} value={t.id}>
                    {t.nama_tipe.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
                  </option>
                ))}
              </select>
            </div>

            {loadingFilters && (
              <p className="text-xs text-gray-400 text-center">Memuat opsi filter...</p>
            )}
          </div>

          {/* Cards 1 & 2 */}
          <div className="lg:col-span-3 grid grid-cols-1 sm:grid-cols-2 gap-4">
            {loadingCards ? (
              <>
                <div className="bg-white rounded-2xl p-5 shadow-sm border border-gray-100 animate-pulse h-32"></div>
                <div className="bg-white rounded-2xl p-5 shadow-sm border border-gray-100 animate-pulse h-32"></div>
              </>
            ) : (
              <>
                <StatCard
                  title="Harga Terakhir Beras Medium"
                  harga={mediumLatest?.harga}
                  delta={mediumLatest?.delta}
                  tanggal={mediumLatest?.tanggal}
                  colorClass={{ bg: 'bg-blue-100', icon: 'text-blue-600' }}
                />
                <StatCard
                  title="Harga Terakhir Beras Premium"
                  harga={premiumLatest?.harga}
                  delta={premiumLatest?.delta}
                  tanggal={premiumLatest?.tanggal}
                  colorClass={{ bg: 'bg-emerald-100', icon: 'text-emerald-600' }}
                />
              </>
            )}
          </div>
        </div>

        {/* Row 2: Cards 3-6 */}
        {loadingCards ? (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {[1, 2, 3, 4].map(i => (
              <div key={i} className="bg-white rounded-2xl p-4 shadow-sm border border-gray-100 animate-pulse h-28"></div>
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <MinMaxCard
              title="Min Medium Tahun Ini"
              subtitle={isJatim ? mediumMin?.nama_kab_kota : null}
              harga={mediumMin?.harga}
              selisih={mediumMin?.selisih}
              tanggal={mediumMin?.tanggal}
              colorClass={{ bg: 'bg-sky-100', icon: 'text-sky-600' }}
              isMax={false}
            />
            <MinMaxCard
              title="Min Premium Tahun Ini"
              subtitle={isJatim ? premiumMin?.nama_kab_kota : null}
              harga={premiumMin?.harga}
              selisih={premiumMin?.selisih}
              tanggal={premiumMin?.tanggal}
              colorClass={{ bg: 'bg-teal-100', icon: 'text-teal-600' }}
              isMax={false}
            />
            <MinMaxCard
              title="Maks Medium Tahun Ini"
              subtitle={isJatim ? mediumMax?.nama_kab_kota : null}
              harga={mediumMax?.harga}
              selisih={mediumMax?.selisih}
              tanggal={mediumMax?.tanggal}
              colorClass={{ bg: 'bg-orange-100', icon: 'text-orange-600' }}
              isMax={true}
            />
            <MinMaxCard
              title="Maks Premium Tahun Ini"
              subtitle={isJatim ? premiumMax?.nama_kab_kota : null}
              harga={premiumMax?.harga}
              selisih={premiumMax?.selisih}
              tanggal={premiumMax?.tanggal}
              colorClass={{ bg: 'bg-rose-100', icon: 'text-rose-600' }}
              isMax={true}
            />
          </div>
        )}

        {/* Row 3: Choropleth Map */}
        <div className="bg-white rounded-2xl p-5 shadow-sm border border-gray-100">
          <div className="mb-4">
            <h3 className="text-base sm:text-lg font-semibold text-gray-800">Peta Harga Beras Jawa Timur</h3>
            <p className="text-gray-500 text-xs sm:text-sm">
              Distribusi harga per kabupaten/kota — gradasi warna berdasarkan tingkat harga
            </p>
          </div>
          <ChoroplethMap priceData={hargaPeta} />
        </div>

        {/* Row 4: Line Chart */}
        <div className="bg-white rounded-2xl p-5 shadow-sm border border-gray-100">
          <div className="mb-4">
            <h3 className="text-base sm:text-lg font-semibold text-gray-800">Tren Harga Harian</h3>
            <p className="text-gray-500 text-xs sm:text-sm">
              Medium & Premium — titik merah menandai data outlier. Geser grafik untuk histori lebih panjang.
            </p>
          </div>
          {loadingCharts ? (
            <div className="h-96 flex items-center justify-center">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
            </div>
          ) : lineChartData ? (
            <div className="h-96">
              <Line data={lineChartData} options={lineChartOptions} plugins={[lineValueLabelPlugin]} />
            </div>
          ) : (
            <div className="h-96 flex items-center justify-center text-gray-400 text-sm">
              Data tidak tersedia
            </div>
          )}
        </div>

        {/* Row 5: Bar Chart */}
        <div className="bg-white rounded-2xl p-5 shadow-sm border border-gray-100">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-4">
            <div>
              <h3 className="text-base sm:text-lg font-semibold text-gray-800">Perbandingan Harga</h3>
              <p className="text-gray-500 text-xs sm:text-sm">
                {barMode === 'top_highest_date' && `Top 15 tanggal harga tertinggi tahun berjalan — ${selectedKotaNama}`}
                {barMode === 'top_lowest_date'  && `Top 15 tanggal harga terendah tahun berjalan — ${selectedKotaNama}`}
                {barMode === 'top_highest_city' && 'Top 15 kab/kota harga beras tertinggi tahun berjalan'}
                {barMode === 'top_lowest_city'  && 'Top 15 kab/kota harga beras terendah tahun berjalan'}
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                onClick={() => setBarMode('top_highest_date')}
                className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                  barMode === 'top_highest_date' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                Harga Tertinggi Tahunan
              </button>
              <button
                onClick={() => setBarMode('top_lowest_date')}
                className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                  barMode === 'top_lowest_date' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                Harga Terendah Tahunan
              </button>
              {isJatim && (
                <>
                  <button
                    onClick={() => setBarMode('top_highest_city')}
                    className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                      barMode === 'top_highest_city' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                    }`}
                  >
                    Kota Harga Tertinggi
                  </button>
                  <button
                    onClick={() => setBarMode('top_lowest_city')}
                    className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                      barMode === 'top_lowest_city' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                    }`}
                  >
                    Kota Harga Terendah
                  </button>
                </>
              )}
            </div>
          </div>
          {loadingBar ? (
            <div className="h-72 flex items-center justify-center">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
            </div>
          ) : barChartData ? (
            <div className="h-72">
              <Bar data={barChartData} options={barChartOptions} plugins={[barValueLabelPlugin]} />
            </div>
          ) : (
            <div className="h-72 flex items-center justify-center text-gray-400 text-sm">
              Data tidak tersedia
            </div>
          )}
        </div>

      </div>
    </div>
  );
};
