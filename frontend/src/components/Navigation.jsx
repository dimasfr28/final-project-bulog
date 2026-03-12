import React from 'react';
import { authService } from '../services/authService';

export const Navigation = ({ currentPage, setCurrentPage, onLogout }) => {
  const username = authService.getCurrentUser();

  return (
    <nav className="bg-indigo-600 text-white p-4 shadow-lg">
      <div className="max-w-7xl mx-auto flex justify-between items-center">
        <div className="flex items-center space-x-8">
          <h1 className="text-2xl font-bold">BULOG Dashboard</h1>
          <div className="flex space-x-4">
            <button
              onClick={() => setCurrentPage('dashboard')}
              className={`px-4 py-2 rounded transition ${
                currentPage === 'dashboard'
                  ? 'bg-white text-indigo-600'
                  : 'hover:bg-indigo-700'
              }`}
            >
              Dashboard
            </button>
            <button
              onClick={() => setCurrentPage('manage-data')}
              className={`px-4 py-2 rounded transition ${
                currentPage === 'manage-data'
                  ? 'bg-white text-indigo-600'
                  : 'hover:bg-indigo-700'
              }`}
            >
              Manage Data
            </button>
          </div>
        </div>
        <div className="flex items-center space-x-4">
          <span>Welcome, {username}</span>
          <button
            onClick={onLogout}
            className="px-4 py-2 bg-red-500 rounded hover:bg-red-600 transition"
          >
            Logout
          </button>
        </div>
      </div>
    </nav>
  );
};
