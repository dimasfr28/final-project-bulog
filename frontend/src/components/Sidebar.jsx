import React from 'react';
import { authService } from '../services/authService';

export const Sidebar = ({ currentPage, setCurrentPage, onLogout, isMinimized, setIsMinimized }) => {
  const username = authService.getCurrentUser();
  const [mobileOpen, setMobileOpen] = React.useState(false);

  const menuItems = [
    {
      id: 'dashboard',
      label: 'Analisa Harga',
      icon: (
        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z" />
        </svg>
      ),
    },
    {
      id: 'manage-data',
      label: 'Tabel Harga',
      icon: (
        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
        </svg>
      ),
    },
    {
      id: 'prediksi-harga',
      label: 'Prediksi Harga',
      icon: (
        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
        </svg>
      ),
    },
  ];

  const bottomMenuItems = [
  ];

  const handleNavClick = (id) => {
    setCurrentPage(id);
    setMobileOpen(false);
  };

  const sidebarContent = (
    <>
      {/* Logo Section */}
      <div className={`${isMinimized ? 'p-4' : 'p-6'} border-b border-blue-500/30`}>
        <div className={`flex items-center ${isMinimized ? 'justify-center' : 'space-x-3'}`}>
          <div className="w-10 h-10 bg-white/20 rounded-xl flex items-center justify-center backdrop-blur-sm flex-shrink-0">
            <img
              src="/assets/images/logo-bulog.png"
              alt="Logo"
              className="w-7 h-7 object-contain"
            />
          </div>
          {!isMinimized && (
            <div>
              <h1 className="font-bold text-lg">BULOG</h1>
              <p className="text-blue-200 text-xs">Kanwil Jatim</p>
            </div>
          )}
        </div>
      </div>

      {/* Main Menu */}
      <nav className={`flex-1 ${isMinimized ? 'p-2' : 'p-4'} space-y-2`}>
        {!isMinimized && (
          <p className="text-blue-300 text-xs font-semibold uppercase tracking-wider mb-4 px-3">
            Menu Utama
          </p>
        )}
        {menuItems.map((item) => (
          <button
            key={item.id}
            onClick={() => handleNavClick(item.id)}
            title={isMinimized ? item.label : ''}
            className={`w-full flex items-center ${isMinimized ? 'justify-center px-2' : 'space-x-3 px-4'} py-3 rounded-xl transition-all duration-200 ${
              currentPage === item.id
                ? 'bg-white text-blue-700 shadow-lg shadow-blue-900/30 font-semibold'
                : 'text-blue-100 hover:bg-white/10'
            }`}
          >
            <span className={`flex-shrink-0 ${currentPage === item.id ? 'text-blue-600' : ''}`}>
              {item.icon}
            </span>
            {!isMinimized && (
              <>
                <span>{item.label}</span>
                {currentPage === item.id && (
                  <span className="ml-auto w-2 h-2 bg-blue-500 rounded-full"></span>
                )}
              </>
            )}
          </button>
        ))}

        <div className={isMinimized ? 'pt-4' : 'pt-6'}>
          {bottomMenuItems.map((item) => (
            <button
              key={item.id}
              onClick={() => handleNavClick(item.id)}
              title={isMinimized ? item.label : ''}
              className={`w-full flex items-center ${isMinimized ? 'justify-center px-2' : 'space-x-3 px-4'} py-3 rounded-xl transition-all duration-200 ${
                currentPage === item.id
                  ? 'bg-white text-blue-700 shadow-lg font-semibold'
                  : 'text-blue-100 hover:bg-white/10'
              }`}
            >
              <span className="flex-shrink-0">{item.icon}</span>
              {!isMinimized && <span>{item.label}</span>}
            </button>
          ))}
        </div>
      </nav>

      {/* User Profile & Logout */}
      <div className={`${isMinimized ? 'p-2' : 'p-4'} border-t border-blue-500/30`}>
        {!isMinimized ? (
          <div className="bg-blue-500/30 rounded-xl p-4 mb-3">
            <div className="flex items-center space-x-3">
              <div className="w-10 h-10 bg-gradient-to-br from-blue-400 to-blue-600 rounded-full flex items-center justify-center text-white font-bold shadow-lg">
                {username ? username.charAt(0).toUpperCase() : 'U'}
              </div>
              <div className="flex-1 min-w-0">
                <p className="font-semibold text-sm truncate">{username || 'User'}</p>
                <p className="text-blue-200 text-xs">Administrator</p>
              </div>
            </div>
          </div>
        ) : (
          <div className="flex justify-center mb-3">
            <div className="w-10 h-10 bg-gradient-to-br from-blue-400 to-blue-600 rounded-full flex items-center justify-center text-white font-bold shadow-lg">
              {username ? username.charAt(0).toUpperCase() : 'U'}
            </div>
          </div>
        )}
        <button
          onClick={onLogout}
          title={isMinimized ? 'Keluar' : ''}
          className={`w-full flex items-center justify-center ${isMinimized ? 'px-2' : 'space-x-2 px-4'} py-3 bg-red-600 text-red-200 rounded-xl hover:bg-red-600/50 transition-all duration-200`}
        >
          <svg className="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
          </svg>
          {!isMinimized && <span>Keluar</span>}
        </button>
      </div>
    </>
  );

  return (
    <>
      {/* Mobile hamburger button */}
      <button
        onClick={() => setMobileOpen(true)}
        className="lg:hidden fixed top-4 left-4 z-50 p-2 bg-blue-600 text-white rounded-lg shadow-lg"
      >
        <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 6h16M4 12h16M4 18h16" />
        </svg>
      </button>

      {/* Mobile overlay */}
      {mobileOpen && (
        <div
          className="lg:hidden fixed inset-0 bg-black/50 z-50"
          onClick={() => setMobileOpen(false)}
        />
      )}

      {/* Mobile sidebar drawer */}
      <aside
        className={`lg:hidden fixed left-0 top-0 h-screen w-64 bg-gradient-to-b from-blue-600 via-blue-700 to-blue-800 text-white flex flex-col shadow-xl z-50 transition-transform duration-300 ${
          mobileOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        {/* Close button */}
        <button
          onClick={() => setMobileOpen(false)}
          className="absolute top-4 right-4 p-1 text-blue-200 hover:text-white"
        >
          <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
        {sidebarContent}
      </aside>

      {/* Desktop sidebar */}
      <aside className={`hidden lg:flex fixed left-0 top-0 h-screen ${isMinimized ? 'w-20' : 'w-64'} bg-gradient-to-b from-blue-600 via-blue-700 to-blue-800 text-white flex-col shadow-xl z-50 transition-all duration-300`}>
        {/* Toggle Button */}
        <button
          onClick={() => setIsMinimized(!isMinimized)}
          className="absolute -right-3 top-8 w-6 h-6 bg-white rounded-full shadow-lg flex items-center justify-center text-blue-600 hover:bg-blue-50 transition-colors"
        >
          <svg className={`w-4 h-4 transition-transform duration-300 ${isMinimized ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M15 19l-7-7 7-7" />
          </svg>
        </button>
        {sidebarContent}
      </aside>
    </>
  );
};
