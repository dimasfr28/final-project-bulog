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
import { dashboardService, prediksiService } from '../services/dashboardService';
import { authService } from '../services/authService';

ChartJS.register(
  CategoryScale, LinearScale, PointElement, LineElement,
  BarElement, Title, Tooltip, Legend, Filler, zoomPlugin
);

const formatRupiah = (val) =>
  val != null ? `Rp ${Number(val).toLocaleString('id-ID')}` : '-';

const formatPValue = (val) => {
  if (val == null) return <span className="text-gray-400">-</span>;
  const num = Number(val);
  const signifikan = num < 0.05;
  return (
    <span className={signifikan ? 'text-green-600 font-semibold' : 'text-red-500 font-semibold'}>
      {num.toFixed(4)}
    </span>
  );
};

const DEFAULT_VISIBLE = 20;

export const PrediksiHarga = () => {
  const username = authService.getCurrentUser();

  const [tipeHargaList, setTipeHargaList] = React.useState([]);
  const [variantList, setVariantList] = React.useState([]);
  const [filters, setFilters] = React.useState({ tipeHargaId: null, variantId: null });
  const [evaluasi, setEvaluasi] = React.useState(null);
  const [chartRaw, setChartRaw] = React.useState(null);
  const [loading, setLoading] = React.useState(false);

  // Load filter options lalu set default & fetch
  React.useEffect(() => {
    Promise.all([dashboardService.getTipeHarga(), dashboardService.getVariant()])
      .then(([resTipe, resVariant]) => {
        const tipeList = resTipe.data || [];
        const variantData = resVariant.data || [];
        setTipeHargaList(tipeList);
        setVariantList(variantData);

        const defaultTipe = tipeList.find((t) =>
          (t.nama_tipe || '').toLowerCase().includes('pasar')
        );
        const defaultVariant = variantData.find((v) =>
          (v.nama_variant || '').toLowerCase().includes('medium')
        );
        const initFilters = {
          tipeHargaId: defaultTipe?.id ?? tipeList[0]?.id ?? null,
          variantId: defaultVariant?.variant_id ?? variantData[0]?.variant_id ?? null,
        };
        setFilters(initFilters);
        fetchData(initFilters);
      })
      .catch(console.error);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const fetchData = async (f) => {
    setLoading(true);
    try {
      const [resEval, resChart] = await Promise.all([
        prediksiService.getEvaluasi({ tipeHargaId: f.tipeHargaId, variantId: f.variantId }),
        prediksiService.getChart({ tipeHargaId: f.tipeHargaId, variantId: f.variantId }),
      ]);
      setEvaluasi(resEval.data?.data || null);
      setChartRaw(resChart.data || null);
    } catch (err) {
      console.error('Error fetching prediksi:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleFilterChange = (key, rawValue) => {
    const value = rawValue === '' ? null : Number(rawValue);
    const newFilters = { ...filters, [key]: value };
    setFilters(newFilters);
    fetchData(newFilters);
  };

  // ── Line Chart data ──────────────────────────────────────
  const lineChartData = React.useMemo(() => {
    if (!chartRaw) return null;
    const aktual = (chartRaw.aktual || []).sort((a, b) => a.tanggal > b.tanggal ? 1 : -1);
    const prediksi = (chartRaw.prediksi || []).sort((a, b) => a.tanggal > b.tanggal ? 1 : -1);

    // Gabungkan semua tanggal unik
    const allDates = [...new Set([
      ...aktual.map(d => d.tanggal),
      ...prediksi.map(d => d.tanggal),
    ])].sort();

    const aktualMap = Object.fromEntries(aktual.map(d => [d.tanggal, d.harga]));
    const prediksiMap = Object.fromEntries(prediksi.map(d => [d.tanggal, d.harga]));

    const labels = allDates.map(t => {
      const dt = new Date(t);
      return dt.toLocaleDateString('id-ID', { day: '2-digit', month: 'short', year: 'numeric' });
    });

    return {
      labels,
      allDates,
      datasets: [
        {
          label: 'Aktual',
          data: allDates.map(t => aktualMap[t] ?? null),
          borderColor: 'rgba(59, 130, 246, 1)',
          backgroundColor: 'rgba(59, 130, 246, 0.1)',
          borderWidth: 2,
          tension: 0.4,
          fill: false,
          pointRadius: 3,
          pointHoverRadius: 5,
          yAxisID: 'y',
        },
        {
          label: 'Prediksi',
          data: allDates.map(t => prediksiMap[t] ?? null),
          borderColor: 'rgba(249, 115, 22, 1)',
          backgroundColor: 'rgba(249, 115, 22, 0.08)',
          borderWidth: 2,
          borderDash: [6, 3],
          tension: 0.4,
          fill: false,
          pointRadius: 3,
          pointHoverRadius: 5,
          pointStyle: 'rectRot',
          yAxisID: 'y',
        },
      ],
    };
  }, [chartRaw]);

  const lineTotal = lineChartData?.allDates?.length || 0;

  // Plugin pill label — aktual (biru) selalu di atas, prediksi (oranye) selalu di bawah
  const lineValueLabelPlugin = React.useMemo(() => ({
    id: 'prediksiValueLabel',
    afterDatasetsDraw(chart) {
      const { ctx } = chart;
      const COLORS = [
        { bg: 'rgba(59,130,246,0.12)', border: 'rgba(59,130,246,0.7)', text: '#1d4ed8' },   // aktual
        { bg: 'rgba(249,115,22,0.12)', border: 'rgba(249,115,22,0.7)', text: '#c2410c' },   // prediksi
      ];
      const xScale = chart.scales.x;
      const visMin = Math.floor(xScale.min);
      const visMax = Math.ceil(xScale.max);
      const visRange = visMax - visMin;
      if (visRange > 100) return;
      const skipStep = 1;
      const boxH = 14;
      const OFFSET = 6;
      // dsIndex 0 (aktual) → pill di atas, dsIndex 1 (prediksi) → pill di bawah
      const ABOVE = [true, false];

      const drawPill = (point, label, above, color) => {
        ctx.save();
        ctx.font = 'bold 9px sans-serif';
        const textW = ctx.measureText(label).width;
        const boxW = textW + 10;
        const x = point.x - boxW / 2;
        const y = above ? point.y - boxH - OFFSET : point.y + OFFSET;
        const r = 4;
        ctx.beginPath();
        ctx.moveTo(x + r, y); ctx.lineTo(x + boxW - r, y);
        ctx.arcTo(x + boxW, y, x + boxW, y + r, r);
        ctx.lineTo(x + boxW, y + boxH - r);
        ctx.arcTo(x + boxW, y + boxH, x + boxW - r, y + boxH, r);
        ctx.lineTo(x + r, y + boxH);
        ctx.arcTo(x, y + boxH, x, y + boxH - r, r);
        ctx.lineTo(x, y + r);
        ctx.arcTo(x, y, x + r, y, r);
        ctx.closePath();
        ctx.fillStyle = color.bg; ctx.fill();
        ctx.strokeStyle = color.border; ctx.lineWidth = 1; ctx.stroke();
        ctx.fillStyle = color.text; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        ctx.fillText(label, point.x, y + boxH / 2);
        ctx.restore();
      };

      chart.data.datasets.forEach((dataset, dsIndex) => {
        const meta = chart.getDatasetMeta(dsIndex);
        if (meta.hidden) return;
        const color = COLORS[dsIndex] || COLORS[0];
        const above = ABOVE[dsIndex] ?? true;
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

  const lineChartOptions = React.useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { position: 'top', labels: { usePointStyle: true, padding: 15 } },
      prediksiValueLabel: {},
      zoom: lineTotal > DEFAULT_VISIBLE ? {
        pan: { enabled: true, mode: 'x' },
        limits: { x: { min: 0, max: lineTotal - 1, minRange: 7 } },
      } : { pan: { enabled: false } },
      tooltip: {
        backgroundColor: 'rgba(255,255,255,0.97)',
        titleColor: '#1f2937',
        bodyColor: '#4b5563',
        borderColor: '#e5e7eb',
        borderWidth: 1,
        padding: 12,
        boxPadding: 4,
        callbacks: {
          label: (ctx) => `${ctx.dataset.label}: ${formatRupiah(ctx.parsed.y)}`,
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
        ticks: { callback: (v) => formatRupiah(v), font: { size: 10 } },
        afterDataLimitsUpdate: (scale) => {
          const chart = scale.chart;
          const xScale = chart.scales.x;
          if (!xScale) return;
          const minX = Math.max(0, Math.floor(xScale.min));
          const maxX = Math.min((chart.data.labels?.length || 1) - 1, Math.ceil(xScale.max));
          let visMin = Infinity, visMax = -Infinity;
          chart.data.datasets.forEach((ds) => {
            for (let i = minX; i <= maxX; i++) {
              const v = ds.data[i];
              if (v != null && !isNaN(v)) { if (v < visMin) visMin = v; if (v > visMax) visMax = v; }
            }
          });
          if (visMin !== Infinity) {
            const padding = Math.max((visMax - visMin) * 0.1, 50);
            scale.min = Math.floor(visMin - padding);
            scale.max = Math.ceil(visMax + padding);
          }
        },
      },
    },
  }), [lineTotal]);

  // ── Bar Chart ACF & PACF ─────────────────────────────────
  const acfPacfData = React.useMemo(() => {
    if (!evaluasi) return null;
    const parseLags = (val) => {
      if (!val) return [];
      if (Array.isArray(val)) return val.map(Number).filter(n => !isNaN(n));
      if (typeof val === 'string') {
        try { return JSON.parse(val).map(Number).filter(n => !isNaN(n)); } catch { return []; }
      }
      return [];
    };
    const acfLags = parseLags(evaluasi.acf_signifikan_lag);
    const pacfLags = parseLags(evaluasi.pacf_signifikan_lag);
    if (acfLags.length === 0 && pacfLags.length === 0) return null;

    const allLags = [...new Set([...acfLags, ...pacfLags])].sort((a, b) => a - b);
    return {
      labels: allLags.map(l => `Lag ${l}`),
      datasets: [
        {
          label: 'ACF',
          data: allLags.map(l => acfLags.includes(l) ? 1 : 0),
          backgroundColor: 'rgba(59,130,246,0.6)',
          borderColor: 'rgba(59,130,246,1)',
          borderWidth: 1,
          borderRadius: 4,
        },
        {
          label: 'PACF',
          data: allLags.map(l => pacfLags.includes(l) ? 1 : 0),
          backgroundColor: 'rgba(16,185,129,0.6)',
          borderColor: 'rgba(16,185,129,1)',
          borderWidth: 1,
          borderRadius: 4,
        },
      ],
    };
  }, [evaluasi]);

  const barChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { position: 'top', labels: { usePointStyle: true, padding: 15 } },
      tooltip: {
        callbacks: {
          label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y ? 'Signifikan' : 'Tidak'}`,
        },
      },
    },
    scales: {
      x: { grid: { display: false }, ticks: { font: { size: 10 } } },
      y: {
        display: false,
        min: 0, max: 1.2,
      },
    },
  };

  const ljungBoxOk = evaluasi?.ljung_box_pvalue != null && Number(evaluasi.ljung_box_pvalue) > 0.05;

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between sticky top-0 z-10">
        <div>
          <h1 className="text-xl font-bold text-gray-800">Dashboard Prediksi Harga Beras</h1>
          <p className="text-gray-500 text-sm">Data mingguan beras Jawa Timur</p>
        </div>
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 bg-gradient-to-br from-blue-500 to-blue-700 rounded-full flex items-center justify-center text-white font-bold text-sm shadow">
            {username ? username.charAt(0).toUpperCase() : 'U'}
          </div>
          <span className="text-sm font-medium text-gray-700 hidden sm:block">{username}</span>
        </div>
      </div>

      <div className="p-6 space-y-6">
        {/* Filter + Metric Cards — satu baris grid */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-7 gap-3">

          {/* Filter Tipe Harga */}
          <div className="bg-white rounded-2xl p-4 shadow-sm border border-gray-100 flex flex-col justify-between">
            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Tipe Harga</p>
            <select
              value={filters.tipeHargaId ?? ''}
              onChange={(e) => handleFilterChange('tipeHargaId', e.target.value)}
              className="w-full text-sm font-semibold text-gray-800 bg-transparent border-none outline-none cursor-pointer"
            >
              {tipeHargaList.map((t) => (
                <option key={t.id} value={t.id}>{t.nama_tipe}</option>
              ))}
            </select>
          </div>

          {/* Filter Variant */}
          <div className="bg-white rounded-2xl p-4 shadow-sm border border-gray-100 flex flex-col justify-between">
            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Variant</p>
            <select
              value={filters.variantId ?? ''}
              onChange={(e) => handleFilterChange('variantId', e.target.value)}
              className="w-full text-sm font-semibold text-gray-800 bg-transparent border-none outline-none cursor-pointer"
            >
              {variantList.map((v) => (
                <option key={v.variant_id} value={v.variant_id}>{v.nama_variant}</option>
              ))}
            </select>
          </div>

          {/* MAPE */}
          <div className="bg-gradient-to-br from-blue-500 to-blue-600 rounded-2xl p-4 shadow-sm text-white">
            <div className="flex items-center justify-between mb-2">
              <p className="text-xs font-semibold text-blue-100 uppercase tracking-wider">MAPE</p>
              <div className="w-7 h-7 bg-white/20 rounded-lg flex items-center justify-center">
                <svg className="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10" />
                </svg>
              </div>
            </div>
            <p className="text-2xl font-bold">
              {loading ? '...' : (evaluasi?.mape != null ? `${Number(evaluasi.mape).toFixed(2)}%` : '-')}
            </p>
            <p className="text-blue-100 text-xs mt-1">Mean Abs. % Error</p>
          </div>

          {/* MAE */}
          <div className="bg-gradient-to-br from-indigo-500 to-indigo-600 rounded-2xl p-4 shadow-sm text-white">
            <div className="flex items-center justify-between mb-2">
              <p className="text-xs font-semibold text-indigo-100 uppercase tracking-wider">MAE</p>
              <div className="w-7 h-7 bg-white/20 rounded-lg flex items-center justify-center">
                <svg className="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M13 17h8m0 0V9m0 8l-8-8-4 4-6-6" />
                </svg>
              </div>
            </div>
            <p className="text-xl font-bold truncate">
              {loading ? '...' : (evaluasi?.mae != null ? formatRupiah(evaluasi.mae) : '-')}
            </p>
            <p className="text-indigo-100 text-xs mt-1">Mean Absolute Error</p>
          </div>

          {/* RMSE */}
          <div className="bg-gradient-to-br from-violet-500 to-violet-600 rounded-2xl p-4 shadow-sm text-white">
            <div className="flex items-center justify-between mb-2">
              <p className="text-xs font-semibold text-violet-100 uppercase tracking-wider">RMSE</p>
              <div className="w-7 h-7 bg-white/20 rounded-lg flex items-center justify-center">
                <svg className="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
                </svg>
              </div>
            </div>
            <p className="text-xl font-bold truncate">
              {loading ? '...' : (evaluasi?.rmse != null ? formatRupiah(evaluasi.rmse) : '-')}
            </p>
            <p className="text-violet-100 text-xs mt-1">Root Mean Sq. Error</p>
          </div>

          {/* Model Terpilih — span 2 kolom */}
          <div className="bg-white rounded-2xl p-4 shadow-sm border border-gray-100 lg:col-span-2">
            <div className="flex items-center justify-between mb-2">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Model Terpilih</p>
              <div className="w-7 h-7 bg-blue-50 rounded-lg flex items-center justify-center">
                <svg className="w-4 h-4 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                </svg>
              </div>
            </div>
            <p className="text-xl font-bold text-gray-800 truncate">
              {loading ? '...' : (evaluasi?.model || '-')}
            </p>
            <p className="text-gray-400 text-xs mt-1">Algoritma prediksi terbaik</p>
          </div>

        </div>

        {/* Line Chart */}
        <div className="bg-white rounded-2xl p-5 shadow-sm border border-gray-100">
          <div className="mb-4">
            <h3 className="text-base font-semibold text-gray-800">Grafik Prediksi vs Aktual</h3>
            <p className="text-gray-500 text-xs mt-0.5">Data mingguan — geser grafik untuk melihat histori</p>
          </div>
          {loading ? (
            <div className="h-80 flex items-center justify-center">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
            </div>
          ) : lineChartData ? (
            <div className="h-80">
              <Line data={lineChartData} options={lineChartOptions} plugins={[lineValueLabelPlugin]} />
            </div>
          ) : (
            <div className="h-80 flex items-center justify-center text-gray-400 text-sm">
              Data tidak tersedia
            </div>
          )}
        </div>

        {/* Tabel Uji Statistik */}
        <div className="bg-white rounded-2xl p-5 shadow-sm border border-gray-100">
          <h3 className="text-base font-semibold text-gray-800 mb-4">Uji Statistik Model</h3>
          {loading ? (
            <div className="h-20 flex items-center justify-center">
              <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500" />
            </div>
          ) : (
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              {/* Uji Stasioneritas */}
              <div className="border border-gray-100 rounded-xl overflow-hidden">
                <div className="bg-blue-50 px-4 py-2 border-b border-blue-100">
                  <p className="text-sm font-semibold text-blue-800">Uji Stasioneritas</p>
                </div>
                <div className="px-4 py-3 space-y-2 text-sm">
                  <div className="flex justify-between items-center">
                    <span className="text-gray-500">ADF p-value</span>
                    <span>{formatPValue(evaluasi?.adf_pvalue)}</span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-gray-500">ARCH p-value</span>
                    <span>{formatPValue(evaluasi?.arch_pvalue)}</span>
                  </div>
                </div>
              </div>

              {/* Uji Residual */}
              <div className="border border-gray-100 rounded-xl overflow-hidden">
                <div className="bg-green-50 px-4 py-2 border-b border-green-100">
                  <p className="text-sm font-semibold text-green-800">Uji Residual</p>
                </div>
                <div className="px-4 py-3 space-y-2 text-sm">
                  <div className="flex justify-between items-center">
                    <span className="text-gray-500">Ljung-Box p-value</span>
                    <span>{formatPValue(evaluasi?.ljung_box_pvalue)}</span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-gray-500">Residual White Noise</span>
                    <span className={ljungBoxOk ? 'text-green-600 font-semibold' : 'text-red-500 font-semibold'}>
                      {evaluasi ? (ljungBoxOk ? '✔ Ya' : '✘ Tidak') : '-'}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Bar Chart ACF & PACF */}
        <div className="bg-white rounded-2xl p-5 shadow-sm border border-gray-100">
          <h3 className="text-base font-semibold text-gray-800 mb-1">Lag Signifikan ACF & PACF</h3>
          <p className="text-gray-500 text-xs mb-4">Lag yang diidentifikasi signifikan dari analisis autokorelasi</p>
          {loading ? (
            <div className="h-52 flex items-center justify-center">
              <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500" />
            </div>
          ) : acfPacfData ? (
            <div className="h-52">
              <Bar data={acfPacfData} options={barChartOptions} />
            </div>
          ) : (
            <div className="h-52 flex items-center justify-center text-gray-400 text-sm">
              Data lag tidak tersedia
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
