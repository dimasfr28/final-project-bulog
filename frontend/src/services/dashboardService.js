import api from './api';

export const dashboardService = {
  // Filter options
  getKota: async () => {
    const response = await api.get('/api/dashboard/kota');
    return response.data;
  },

  getTipeHarga: async () => {
    const response = await api.get('/api/dashboard/tipe-harga');
    return response.data;
  },

  // Cards 1 & 2 - Harga terakhir Medium & Premium
  getLatestHarga: async (kodeKabKota, tipeHargaId) => {
    const params = {};
    if (kodeKabKota) params.kode_kab_kota = kodeKabKota;
    if (tipeHargaId !== undefined && tipeHargaId !== null) params.tipe_harga_id = tipeHargaId;
    const response = await api.get('/api/dashboard/latest-harga', { params });
    return response.data;
  },

  // Cards 3-6 - Min/Max tahun berjalan
  getMinMaxHargaTahunBerjalan: async (kodeKabKota, tipeHargaId) => {
    const params = {};
    if (kodeKabKota) params.kode_kab_kota = kodeKabKota;
    if (tipeHargaId !== undefined && tipeHargaId !== null) params.tipe_harga_id = tipeHargaId;
    const response = await api.get('/api/dashboard/minmax-harga-tahun-berjalan', { params });
    return response.data;
  },

  // Choropleth Map
  getHargaPeta: async (tipeHargaId) => {
    const params = {};
    if (tipeHargaId !== undefined && tipeHargaId !== null) params.tipe_harga_id = tipeHargaId;
    const response = await api.get('/api/dashboard/harga-peta', { params, timeout: 30000 });
    return response.data;
  },

  // Line Chart - Tren harga dengan outlier
  getTrenHarga: async (kodeKabKota, tipeHargaId) => {
    const params = {};
    if (kodeKabKota) params.kode_kab_kota = kodeKabKota;
    if (tipeHargaId !== undefined && tipeHargaId !== null) params.tipe_harga_id = tipeHargaId;
    const response = await api.get('/api/dashboard/tren-harga', { params, timeout: 60000 });
    return response.data;
  },

  // Bar Chart
  getBarHarga: async (kodeKabKota, tipeHargaId, mode = 'top_highest_date') => {
    const params = { mode, top_n: 12 };
    if (kodeKabKota) params.kode_kab_kota = kodeKabKota;
    if (tipeHargaId !== undefined && tipeHargaId !== null) params.tipe_harga_id = tipeHargaId;
    const response = await api.get('/api/dashboard/bar-harga', { params });
    return response.data;
  },

  getVariant: async () => {
    const response = await api.get('/api/data/variant');
    return response.data;
  },
};

export const prediksiService = {
  getEvaluasi: ({ tipeHargaId, variantId } = {}) => {
    const params = {};
    if (tipeHargaId != null) params.tipe_harga_id = tipeHargaId;
    if (variantId != null) params.variant_id = variantId;
    return api.get('/api/prediksi/evaluasi', { params });
  },

  getChart: ({ tipeHargaId, variantId } = {}) => {
    const params = {};
    if (tipeHargaId != null) params.tipe_harga_id = tipeHargaId;
    if (variantId != null) params.variant_id = variantId;
    return api.get('/api/prediksi/chart', { params, timeout: 60000 });
  },
};

export const dataService = {
  listData: async () => {
    const response = await api.get('/api/data/list');
    return response.data;
  },

  uploadData: async (file) => {
    const formData = new FormData();
    formData.append('file', file);
    const response = await api.post('/api/data/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  },

  deleteData: async (dataId) => {
    const response = await api.delete(`/api/data/delete/${dataId}`);
    return response.data;
  },

  getHargaBeras: async ({ tipeHargaId, variantId, tanggalStart, tanggalEnd } = {}) => {
    const params = {};
    if (tipeHargaId != null) params.tipe_harga_id = tipeHargaId;
    if (variantId != null) params.variant_id = variantId;
    if (tanggalStart) params.tanggal_start = tanggalStart;
    if (tanggalEnd) params.tanggal_end = tanggalEnd;
    const response = await api.get('/api/data/harga-beras', { params, timeout: 60000 });
    return response.data;
  },

  exportHargaBeras: ({ tipeHargaId, variantId, tanggalStart, tanggalEnd } = {}) => {
    const params = new URLSearchParams();
    if (tipeHargaId != null) params.append('tipe_harga_id', tipeHargaId);
    if (variantId != null) params.append('variant_id', variantId);
    if (tanggalStart) params.append('tanggal_start', tanggalStart);
    if (tanggalEnd) params.append('tanggal_end', tanggalEnd);
    const token = localStorage.getItem('access_token');
    const query = params.toString() ? `?${params.toString()}` : '';
    const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
    return fetch(`${API_URL}/api/data/harga-beras/export${query}`, {
      headers: { Authorization: `Bearer ${token}` },
    }).then((res) => res.blob());
  },

  exportHargaBerasExcel: ({ tipeHargaId, variantId, tanggalStart, tanggalEnd, tipeHargaNama, variantNama } = {}) => {
    const params = new URLSearchParams();
    if (tipeHargaId != null) params.append('tipe_harga_id', tipeHargaId);
    if (variantId != null) params.append('variant_id', variantId);
    if (tanggalStart) params.append('tanggal_start', tanggalStart);
    if (tanggalEnd) params.append('tanggal_end', tanggalEnd);
    if (tipeHargaNama) params.append('tipe_harga_nama', tipeHargaNama);
    if (variantNama) params.append('variant_nama', variantNama);
    const token = localStorage.getItem('access_token');
    const query = params.toString() ? `?${params.toString()}` : '';
    const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
    return fetch(`${API_URL}/api/data/harga-beras/export-excel${query}`, {
      headers: { Authorization: `Bearer ${token}` },
    }).then((res) => res.blob());
  },
};
