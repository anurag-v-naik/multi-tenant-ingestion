import React, { useState, useEffect, createContext, useContext } from 'react';
import {
  User,
  Settings,
  Database,
  Play,
  Pause,
  CheckCircle,
  XCircle,
  Clock,
  AlertTriangle,
  Plus,
  Edit,
  Trash2,
  Eye,
  BarChart3,
  Activity,
  Building,
  LogOut,
  Menu,
  X
} from 'lucide-react';

// Types
interface User {
  id: string;
  email: string;
  first_name: string;
  last_name: string;
  role: string;
  tenant_memberships: TenantMembership[];
}

interface TenantMembership {
  tenant_id: string;
  role: string;
  permissions: string[];
}

interface Pipeline {
  id: string;
  name: string;
  description: string;
  pipeline_type: string;
  status: string;
  source_connection_id: string;
  target_connection_id: string;
  created_at: string;
  current_version: string;
  last_execution?: {
    id: string;
    status: string;
    started_at: string;
    completed_at?: string;
  };
}

interface Connection {
  id: string;
  name: string;
  connection_type: string;
  status: string;
  description: string;
  last_tested_at: string;
  is_valid: boolean;
}

// Auth Context
const AuthContext = createContext<{
  user: User | null;
  currentTenant: string | null;
  login: (credentials: any) => Promise<void>;
  logout: () => void;
  setCurrentTenant: (tenantId: string) => void;
}>({
  user: null,
  currentTenant: null,
  login: async () => {},
  logout: () => {},
  setCurrentTenant: () => {}
});

// API Client
class ApiClient {
  private baseUrl = '/api/v1';
  private token: string | null = null;
  private tenantId: string | null = null;

  setAuth(token: string, tenantId?: string) {
    this.token = token;
    this.tenantId = tenantId;
  }

  private getHeaders() {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    if (this.tenantId) {
      headers['X-Tenant-ID'] = this.tenantId;
    }

    return headers;
  }

  async request(endpoint: string, options: RequestInit = {}) {
    const url = `${this.baseUrl}${endpoint}`;
    const response = await fetch(url, {
      ...options,
      headers: {
        ...this.getHeaders(),
        ...options.headers,
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return response.json();
  }

  // Auth endpoints
  async login(credentials: { email: string; password: string; tenant_id?: string }) {
    return this.request('/auth/login', {
      method: 'POST',
      body: JSON.stringify(credentials),
    });
  }

  async getCurrentUser() {
    return this.request('/auth/me');
  }

  // Pipeline endpoints
  async getPipelines() {
    return this.request('/pipelines');
  }

  async createPipeline(pipeline: any) {
    return this.request('/pipelines', {
      method: 'POST',
      body: JSON.stringify(pipeline),
    });
  }

  async executePipeline(pipelineId: string) {
    return this.request(`/pipelines/${pipelineId}/execute`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
  }

  // Connection endpoints
  async getConnections() {
    return this.request('/connections');
  }

  async testConnection(connectionData: any) {
    return this.request('/connectors/test-connection', {
      method: 'POST',
      body: JSON.stringify(connectionData),
    });
  }

  async getConnectorTypes() {
    return this.request('/connectors');
  }
}

const apiClient = new ApiClient();

// Auth Provider
function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [currentTenant, setCurrentTenantState] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const token = localStorage.getItem('auth_token');
    const tenantId = localStorage.getItem('current_tenant');

    if (token) {
      apiClient.setAuth(token, tenantId || undefined);
      fetchCurrentUser();
    } else {
      setLoading(false);
    }
  }, []);

  const fetchCurrentUser = async () => {
    try {
      const userData = await apiClient.getCurrentUser();
      setUser(userData);

      if (!currentTenant && userData.tenant_memberships.length > 0) {
        const defaultTenant = userData.tenant_memberships[0].tenant_id;
        setCurrentTenantState(defaultTenant);
        localStorage.setItem('current_tenant', defaultTenant);
        apiClient.setAuth(localStorage.getItem('auth_token')!, defaultTenant);
      }
    } catch (error) {
      console.error('Failed to fetch user:', error);
      logout();
    } finally {
      setLoading(false);
    }
  };

  const login = async (credentials: { email: string; password: string; tenant_id?: string }) => {
    const response = await apiClient.login(credentials);

    localStorage.setItem('auth_token', response.access_token);
    if (response.tenant?.id) {
      localStorage.setItem('current_tenant', response.tenant.id);
      setCurrentTenantState(response.tenant.id);
    }

    apiClient.setAuth(response.access_token, response.tenant?.id);
    setUser(response.user);
  };

  const logout = () => {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('current_tenant');
    setUser(null);
    setCurrentTenantState(null);
  };

  const setCurrentTenant = (tenantId: string) => {
    setCurrentTenantState(tenantId);
    localStorage.setItem('current_tenant', tenantId);
    apiClient.setAuth(localStorage.getItem('auth_token')!, tenantId);
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  return (
    <AuthContext.Provider value={{ user, currentTenant, login, logout, setCurrentTenant }}>
      {children}
    </AuthContext.Provider>
  );
}

// Login Component
function LoginForm() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [tenantId, setTenantId] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const { login } = useContext(AuthContext);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      await login({ email, password, tenant_id: tenantId || undefined });
    } catch (error) {
      setError('Login failed. Please check your credentials.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col justify-center py-12 sm:px-6 lg:px-8">
      <div className="sm:mx-auto sm:w-full sm:max-w-md">
        <div className="text-center">
          <Database className="mx-auto h-12 w-12 text-blue-600" />
          <h2 className="mt-6 text-3xl font-bold text-gray-900">
            Multi-Tenant Data Platform
          </h2>
          <p className="mt-2 text-sm text-gray-600">
            Sign in to your account
          </p>
        </div>
      </div>

      <div className="mt-8 sm:mx-auto sm:w-full sm:max-w-md">
        <div className="bg-white py-8 px-4 shadow sm:rounded-lg sm:px-10">
          <form className="space-y-6" onSubmit={handleSubmit}>
            {error && (
              <div className="bg-red-50 border border-red-200 text-red-600 px-4 py-3 rounded">
                {error}
              </div>
            )}

            <div>
              <label htmlFor="email" className="block text-sm font-medium text-gray-700">
                Email address
              </label>
              <input
                id="email"
                name="email"
                type="email"
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="mt-1 appearance-none relative block w-full px-3 py-2 border border-gray-300 placeholder-gray-500 text-gray-900 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                placeholder="Enter your email"
              />
            </div>

            <div>
              <label htmlFor="password" className="block text-sm font-medium text-gray-700">
                Password
              </label>
              <input
                id="password"
                name="password"
                type="password"
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="mt-1 appearance-none relative block w-full px-3 py-2 border border-gray-300 placeholder-gray-500 text-gray-900 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                placeholder="Enter your password"
              />
            </div>

            <div>
              <label htmlFor="tenantId" className="block text-sm font-medium text-gray-700">
                Organization (Optional)
              </label>
              <input
                id="tenantId"
                name="tenantId"
                type="text"
                value={tenantId}
                onChange={(e) => setTenantId(e.target.value)}
                className="mt-1 appearance-none relative block w-full px-3 py-2 border border-gray-300 placeholder-gray-500 text-gray-900 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                placeholder="Enter organization ID"
              />
            </div>

            <div>
              <button
                type="submit"
                disabled={loading}
                className="group relative w-full flex justify-center py-2 px-4 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
              >
                {loading ? 'Signing in...' : 'Sign in'}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}

// Header Component
function Header() {
  const { user, currentTenant, logout, setCurrentTenant } = useContext(AuthContext);
  const [showUserMenu, setShowUserMenu] = useState(false);

  const currentMembership = user?.tenant_memberships.find(
    tm => tm.tenant_id === currentTenant
  );

  return (
    <header className="bg-white shadow border-b border-gray-200">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16">
          <div className="flex items-center">
            <Database className="h-8 w-8 text-blue-600" />
            <span className="ml-2 text-xl font-bold text-gray-900">
              Data Platform
            </span>
          </div>

          <div className="flex items-center space-x-4">
            {user?.tenant_memberships && user.tenant_memberships.length > 1 && (
              <select
                value={currentTenant || ''}
                onChange={(e) => setCurrentTenant(e.target.value)}
                className="border border-gray-300 rounded-md px-3 py-1 text-sm"
              >
                {user.tenant_memberships.map((tm) => (
                  <option key={tm.tenant_id} value={tm.tenant_id}>
                    {tm.tenant_id} ({tm.role})
                  </option>
                ))}
              </select>
            )}

            <div className="relative">
              <button
                onClick={() => setShowUserMenu(!showUserMenu)}
                className="flex items-center space-x-2 text-sm text-gray-700 hover:text-gray-900"
              >
                <User className="h-5 w-5" />
                <span>{user?.first_name} {user?.last_name}</span>
                <span className="text-xs text-gray-500">
                  ({currentMembership?.role})
                </span>
              </button>

              {showUserMenu && (
                <div className="absolute right-0 mt-2 w-48 bg-white rounded-md shadow-lg border border-gray-200 z-10">
                  <div className="py-1">
                    <button
                      onClick={logout}
                      className="flex items-center px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 w-full text-left"
                    >
                      <LogOut className="h-4 w-4 mr-2" />
                      Sign out
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}

// Status Badge Component
function StatusBadge({ status }: { status: string }) {
  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'active':
      case 'success':
        return 'bg-green-100 text-green-800';
      case 'running':
      case 'pending':
        return 'bg-yellow-100 text-yellow-800';
      case 'failed':
      case 'error':
        return 'bg-red-100 text-red-800';
      case 'paused':
      case 'disabled':
        return 'bg-gray-100 text-gray-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status.toLowerCase()) {
      case 'active':
      case 'success':
        return <CheckCircle className="h-4 w-4" />;
      case 'running':
      case 'pending':
        return <Clock className="h-4 w-4" />;
      case 'failed':
      case 'error':
        return <XCircle className="h-4 w-4" />;
      case 'paused':
        return <Pause className="h-4 w-4" />;
      default:
        return <AlertTriangle className="h-4 w-4" />;
    }
  };

  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusColor(status)}`}>
      {getStatusIcon(status)}
      <span className="ml-1">{status}</span>
    </span>
  );
}

// Dashboard Component
function Dashboard() {
  const [stats, setStats] = useState({
    totalPipelines: 0,
    activePipelines: 0,
    totalConnections: 0,
    recentExecutions: []
  });

  useEffect(() => {
    setStats({
      totalPipelines: 12,
      activePipelines: 8,
      totalConnections: 6,
      recentExecutions: [
        { id: '1', pipeline_name: 'Sales Data ETL', status: 'success', started_at: '2024-01-15T10:30:00Z' },
        { id: '2', pipeline_name: 'Customer Analytics', status: 'running', started_at: '2024-01-15T11:00:00Z' },
        { id: '3', pipeline_name: 'Inventory Sync', status: 'failed', started_at: '2024-01-15T09:45:00Z' },
      ]
    });
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <p className="mt-1 text-sm text-gray-500">
          Overview of your data pipelines and connections
        </p>
      </div>

      <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <Activity className="h-6 w-6 text-gray-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Total Pipelines
                  </dt>
                  <dd className="text-lg font-medium text-gray-900">
                    {stats.totalPipelines}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <CheckCircle className="h-6 w-6 text-green-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Active Pipelines
                  </dt>
                  <dd className="text-lg font-medium text-gray-900">
                    {stats.activePipelines}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <Database className="h-6 w-6 text-blue-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Connections
                  </dt>
                  <dd className="text-lg font-medium text-gray-900">
                    {stats.totalConnections}
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <BarChart3 className="h-6 w-6 text-purple-400" />
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Success Rate
                  </dt>
                  <dd className="text-lg font-medium text-gray-900">
                    94%
                  </dd>
                </dl>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="bg-white shadow rounded-lg">
        <div className="px-4 py-5 sm:p-6">
          <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
            Recent Pipeline Executions
          </h3>
          <div className="space-y-3">
            {stats.recentExecutions.map((execution) => (
              <div key={execution.id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                <div>
                  <p className="text-sm font-medium text-gray-900">{execution.pipeline_name}</p>
                  <p className="text-xs text-gray-500">
                    {new Date(execution.started_at).toLocaleString()}
                  </p>
                </div>
                <StatusBadge status={execution.status} />
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// Sidebar Component
function Sidebar({ currentPage, setCurrentPage }: { currentPage: string; setCurrentPage: (page: string) => void }) {
  const [isOpen, setIsOpen] = useState(true);

  const navigation = [
    { name: 'Dashboard', id: 'dashboard', icon: BarChart3 },
    { name: 'Pipelines', id: 'pipelines', icon: Activity },
    { name: 'Connections', id: 'connections', icon: Database },
    { name: 'Settings', id: 'settings', icon: Settings },
  ];

  return (
    <>
      <div className="lg:hidden fixed top-4 left-4 z-20">
        <button
          onClick={() => setIsOpen(!isOpen)}
          className="p-2 rounded-md text-gray-400 hover:text-gray-500 hover:bg-gray-100"
        >
          {isOpen ? <X className="h-6 w-6" /> : <Menu className="h-6 w-6" />}
        </button>
      </div>

      <div className={`fixed inset-y-0 left-0 z-10 w-64 bg-gray-50 border-r border-gray-200 transition-transform duration-300 ease-in-out ${
        isOpen ? 'translate-x-0' : '-translate-x-full'
      } lg:translate-x-0`}>
        <div className="flex flex-col h-full">
          <div className="flex-1 flex flex-col pt-20 lg:pt-5 pb-4 overflow-y-auto">
            <nav className="mt-5 flex-1 px-2 space-y-1">
              {navigation.map((item) => {
                const Icon = item.icon;
                return (
                  <button
                    key={item.id}
                    onClick={() => {
                      setCurrentPage(item.id);
                      setIsOpen(false);
                    }}
                    className={`${
                      currentPage === item.id
                        ? 'bg-blue-100 text-blue-900'
                        : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
                    } group flex items-center px-2 py-2 text-sm font-medium rounded-md w-full text-left`}
                  >
                    <Icon className="mr-3 h-5 w-5" />
                    {item.name}
                  </button>
                );
              })}
            </nav>
          </div>
        </div>
      </div>
    </>
  );
}

// Simple pages for demo
function Pipelines() {
  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Pipelines</h1>
          <p className="mt-1 text-sm text-gray-500">
            Manage your data processing pipelines
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700">
          <Plus className="h-4 w-4 mr-2" />
          Create Pipeline
        </button>
      </div>
      <div className="bg-white shadow rounded-lg p-6">
        <p className="text-gray-500">Pipeline management interface will be implemented here.</p>
      </div>
    </div>
  );
}

function Connections() {
  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Connections</h1>
          <p className="mt-1 text-sm text-gray-500">
            Manage your data source and destination connections
          </p>
        </div>
        <button className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700">
          <Plus className="h-4 w-4 mr-2" />
          Add Connection
        </button>
      </div>
      <div className="bg-white shadow rounded-lg p-6">
        <p className="text-gray-500">Connection management interface will be implemented here.</p>
      </div>
    </div>
  );
}

function Settings() {
  const { user } = useContext(AuthContext);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Settings</h1>
        <p className="mt-1 text-sm text-gray-500">
          Manage your account and preferences
        </p>
      </div>

      <div className="bg-white shadow rounded-lg">
        <div className="px-4 py-5 sm:p-6">
          <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
            Profile Information
          </h3>
          <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
            <div>
              <label className="block text-sm font-medium text-gray-700">
                First Name
              </label>
              <input
                type="text"
                value={user?.first_name || ''}
                readOnly
                className="mt-1 block w-full border-gray-300 rounded-md shadow-sm bg-gray-50"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">
                Last Name
              </label>
              <input
                type="text"
                value={user?.last_name || ''}
                readOnly
                className="mt-1 block w-full border-gray-300 rounded-md shadow-sm bg-gray-50"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">
                Email
              </label>
              <input
                type="email"
                value={user?.email || ''}
                readOnly
                className="mt-1 block w-full border-gray-300 rounded-md shadow-sm bg-gray-50"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">
                Role
              </label>
              <input
                type="text"
                value={user?.role || ''}
                readOnly
                className="mt-1 block w-full border-gray-300 rounded-md shadow-sm bg-gray-50"
              />
            </div>
          </div>
        </div>
      </div>

      <div className="bg-white shadow rounded-lg">
        <div className="px-4 py-5 sm:p-6">
          <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
            Organization Memberships
          </h3>
          <div className="space-y-3">
            {user?.tenant_memberships.map((membership, index) => (
              <div key={index} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                <div className="flex items-center space-x-3">
                  <Building className="h-5 w-5 text-gray-400" />
                  <div>
                    <p className="text-sm font-medium text-gray-900">
                      {membership.tenant_id}
                    </p>
                    <p className="text-xs text-gray-500">
                      Role: {membership.role}
                    </p>
                  </div>
                </div>
                <div className="flex flex-wrap gap-1">
                  {membership.permissions.slice(0, 3).map((permission, idx) => (
                    <span key={idx} className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800">
                      {permission}
                    </span>
                  ))}
                  {membership.permissions.length > 3 && (
                    <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-800">
                      +{membership.permissions.length - 3} more
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// Main App Component
function App() {
  const { user } = useContext(AuthContext);
  const [currentPage, setCurrentPage] = useState('dashboard');

  if (!user) {
    return <LoginForm />;
  }

  const renderCurrentPage = () => {
    switch (currentPage) {
      case 'dashboard':
        return <Dashboard />;
      case 'pipelines':
        return <Pipelines />;
      case 'connections':
        return <Connections />;
      case 'settings':
        return <Settings />;
      default:
        return <Dashboard />;
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <Header />
      <div className="flex">
        <Sidebar currentPage={currentPage} setCurrentPage={setCurrentPage} />
        <main className="flex-1 lg:ml-64">
          <div className="max-w-7xl mx-auto py-6 px-4 sm:px-6 lg:px-8">
            {renderCurrentPage()}
          </div>
        </main>
      </div>
    </div>
  );
}

// Root App with Providers
//export default function MultiTenantDataPlatform() {
//  return (
//    <AuthProvider>
//      <App />
//    </AuthProvider>
//  );
//}

// At the bottom, change to:
export default function App() {
  return (
    <AuthProvider>
      <MultiTenantDataPlatform />
    </AuthProvider>
  );
}