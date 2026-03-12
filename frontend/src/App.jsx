import React from 'react';
import { Login } from './pages/Login';
import { Dashboard } from './pages/Dashboard';
import { ManageData } from './pages/ManageData';
import { PrediksiHarga } from './pages/PrediksiHarga';
import { Sidebar } from './components/Sidebar';
import { authService } from './services/authService';

function App() {
  const [isAuthenticated, setIsAuthenticated] = React.useState(
    authService.isAuthenticated()
  );
  const [currentPage, setCurrentPage] = React.useState('dashboard');
  const [isSidebarMinimized, setIsSidebarMinimized] = React.useState(false);

  const handleLogin = () => {
    setIsAuthenticated(true);
  };

  const handleLogout = () => {
    authService.logout();
    setIsAuthenticated(false);
    setCurrentPage('dashboard');
  };

  if (!isAuthenticated) {
    return <Login onLogin={handleLogin} />;
  }

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Sidebar */}
      <Sidebar
        currentPage={currentPage}
        setCurrentPage={setCurrentPage}
        onLogout={handleLogout}
        isMinimized={isSidebarMinimized}
        setIsMinimized={setIsSidebarMinimized}
      />

      {/* Main Content */}
      <main className={`transition-all duration-300 ${isSidebarMinimized ? 'lg:ml-20' : 'lg:ml-64'}`}>
        {currentPage === 'dashboard' && <Dashboard />}
        {currentPage === 'manage-data' && <ManageData />}
        {currentPage === 'prediksi-harga' && <PrediksiHarga />}
        {currentPage === 'settings' && (
          <div className="p-8">
            <h1 className="text-2xl font-bold text-gray-800">Pengaturan</h1>
            <p className="text-gray-600 mt-2">Halaman pengaturan dalam pengembangan.</p>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
