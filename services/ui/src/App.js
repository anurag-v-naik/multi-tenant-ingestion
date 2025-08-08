// Multi-Tenant Data Ingestion Dashboard
// Main React application component

import React, { useState, useEffect, useContext } from 'react';
import {
  BrowserRouter as Router,
  Routes,
  Route,
  Navigate,
  useNavigate,
  useLocation
} from 'react-router-dom';
import {
  AppBar,
  Toolbar,
  Typography,
  Button,
  Container,
  Box,
  CssBaseline,
  ThemeProvider,
  createTheme,
  Drawer,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Divider,
  Avatar,
  Menu,
  MenuItem,
  Alert,
  Snackbar,
  CircularProgress,
  Paper,
  Grid,
  Card,
  CardContent,
  CardActions,
  Chip,
  IconButton
} from '@mui/material';
import {
  Dashboard as DashboardIcon,
  Pipeline as PipelineIcon,
  Storage as StorageIcon,
  Link as ConnectorIcon,
  Assessment as QualityIcon,
  Settings as SettingsIcon,
  AccountCircle,
  Menu as MenuIcon,
  Logout,
  Add as AddIcon,
  PlayArrow as RunIcon,
  Stop as StopIcon,
  Refresh as RefreshIcon
} from '@mui/icons-material';
import axios from 'axios';

// Create theme
const theme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
    background: {
      default: '#f5f5f5',
    },
  },
});

// Auth Context
const AuthContext = React.createContext();

// Auth Provider
const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [organization, setOrganization] = useState(null);
  const [loading, setLoading] = useState(true);
  const [token, setToken] = useState(localStorage.getItem('token'));

  useEffect(() => {
    const storedUser = localStorage.getItem('user');
    const storedOrg = localStorage.getItem('organization');

    if (storedUser && storedOrg && token) {
      setUser(JSON.parse(storedUser));
      setOrganization(JSON.parse(storedOrg));
    }
    setLoading(false);
  }, [token]);

  const login = async (username, password, organizationId) => {
    try {
      const response = await axios.post('/api/v1/auth/login', {
        username,
        password,
        organization_id: organizationId
      });

      const { token: newToken, user: userData, organization: orgData } = response.data;

      setToken(newToken);
      setUser(userData);
      setOrganization(orgData);

      localStorage.setItem('token', newToken);
      localStorage.setItem('user', JSON.stringify(userData));
      localStorage.setItem('organization', JSON.stringify(orgData));

      // Set default axios headers
      axios.defaults.headers.common['Authorization'] = `Bearer ${newToken}`;
      axios.defaults.headers.common['X-Organization-ID'] = organizationId;

      return true;
    } catch (error) {
      console.error('Login failed:', error);
      return false;
    }
  };

  const logout = () => {
    setUser(null);
    setOrganization(null);
    setToken(null);

    localStorage.removeItem('token');
    localStorage.removeItem('user');
    localStorage.removeItem('organization');

    delete axios.defaults.headers.common['Authorization'];
    delete axios.defaults.headers.common['X-Organization-ID'];
  };

  const value = {
    user,
    organization,
    token,
    login,
    logout,
    loading
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

// Hook to use auth context
const useAuth = () => {
  return useContext(AuthContext);
};

// Protected Route Component
const ProtectedRoute = ({ children }) => {
  const { user, loading } = useAuth();

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="100vh">
        <CircularProgress />
      </Box>
    );
  }

  return user ? children : <Navigate to="/login" />;
};

// Login Component
const LoginPage = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [organizationId, setOrganizationId] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const { login } = useAuth();
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    const success = await login(username, password, organizationId);

    if (success) {
      navigate('/dashboard');
    } else {
      setError('Login failed. Please check your credentials.');
    }

    setLoading(false);
  };

  return (
    <Container maxWidth="sm">
      <Box
        display="flex"
        flexDirection="column"
        alignItems="center"
        justifyContent="center"
        minHeight="100vh"
      >
        <Paper elevation={3} sx={{ p: 4, width: '100%' }}>
          <Typography variant="h4" align="center" gutterBottom>
            Multi-Tenant Ingestion
          </Typography>
          <Typography variant="subtitle1" align="center" color="text.secondary" gutterBottom>
            Sign in to your organization
          </Typography>

          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}

          <Box component="form" onSubmit={handleSubmit}>
            <Box mb={2}>
              <input
                type="text"
                placeholder="Username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                required
                style={{
                  width: '100%',
                  padding: '12px',
                  border: '1px solid #ddd',
                  borderRadius: '4px',
                  fontSize: '16px'
                }}
              />
            </Box>

            <Box mb={2}>
              <input
                type="password"
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                style={{
                  width: '100%',
                  padding: '12px',
                  border: '1px solid #ddd',
                  borderRadius: '4px',
                  fontSize: '16px'
                }}
              />
            </Box>

            <Box mb={3}>
              <select
                value={organizationId}
                onChange={(e) => setOrganizationId(e.target.value)}
                required
                style={{
                  width: '100%',
                  padding: '12px',
                  border: '1px solid #ddd',
                  borderRadius: '4px',
                  fontSize: '16px'
                }}
              >
                <option value="">Select Organization</option>
                <option value="finance">Finance Department</option>
                <option value="retail">Retail Division</option>
                <option value="healthcare">Healthcare Unit</option>
              </select>
            </Box>

            <Button
              type="submit"
              fullWidth
              variant="contained"
              size="large"
              disabled={loading}
            >
              {loading ? <CircularProgress size={24} /> : 'Sign In'}
            </Button>
          </Box>
        </Paper>
      </Box>
    </Container>
  );
};

// Navigation Drawer
const NavigationDrawer = ({ open, onClose }) => {
  const navigate = useNavigate();
  const location = useLocation();

  const menuItems = [
    { text: 'Dashboard', icon: <DashboardIcon />, path: '/dashboard' },
    { text: 'Pipelines', icon: <PipelineIcon />, path: '/pipelines' },
    { text: 'Catalogs', icon: <StorageIcon />, path: '/catalogs' },
    { text: 'Connectors', icon: <ConnectorIcon />, path: '/connectors' },
    { text: 'Data Quality', icon: <QualityIcon />, path: '/data-quality' },
    { text: 'Settings', icon: <SettingsIcon />, path: '/settings' },
  ];

  return (
    <Drawer anchor="left" open={open} onClose={onClose}>
      <Box sx={{ width: 250 }} role="presentation">
        <Toolbar>
          <Typography variant="h6">Navigation</Typography>
        </Toolbar>
        <Divider />
        <List>
          {menuItems.map((item) => (
            <ListItem
              button
              key={item.text}
              selected={location.pathname === item.path}
              onClick={() => {
                navigate(item.path);
                onClose();
              }}
            >
              <ListItemIcon>{item.icon}</ListItemIcon>
              <ListItemText primary={item.text} />
            </ListItem>
          ))}
        </List>
      </Box>
    </Drawer>
  );
};

// Main Layout Component
const Layout = ({ children }) => {
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [anchorEl, setAnchorEl] = useState(null);
  const { user, organization, logout } = useAuth();

  const handleProfileMenuOpen = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleProfileMenuClose = () => {
    setAnchorEl(null);
  };

  const handleLogout = () => {
    logout();
    handleProfileMenuClose();
  };

  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar position="fixed">
        <Toolbar>
          <IconButton
            edge="start"
            color="inherit"
            onClick={() => setDrawerOpen(true)}
            sx={{ mr: 2 }}
          >
            <MenuIcon />
          </IconButton>

          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            Multi-Tenant Ingestion - {organization?.display_name}
          </Typography>

          <Button
            color="inherit"
            onClick={handleProfileMenuOpen}
            startIcon={<AccountCircle />}
          >
            {user?.username}
          </Button>

          <Menu
            anchorEl={anchorEl}
            open={Boolean(anchorEl)}
            onClose={handleProfileMenuClose}
          >
            <MenuItem onClick={handleProfileMenuClose}>Profile</MenuItem>
            <MenuItem onClick={handleLogout}>
              <Logout sx={{ mr: 1 }} />
              Logout
            </MenuItem>
          </Menu>
        </Toolbar>
      </AppBar>

      <NavigationDrawer
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
      />

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          p: 3,
          mt: 8,
        }}
      >
        {children}
      </Box>
    </Box>
  );
};

// Dashboard Component
const Dashboard = () => {
  const [stats, setStats] = useState({
    totalPipelines: 0,
    activePipelines: 0,
    totalConnectors: 0,
    dataQualityScore: 0,
  });
  const [recentPipelines, setRecentPipelines] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchDashboardData = async () => {
      try {
        // Fetch dashboard statistics
        const [pipelinesRes, connectorsRes] = await Promise.all([
          axios.get('/api/v1/pipelines'),
          axios.get('/api/v1/connectors'),
        ]);

        const pipelines = pipelinesRes.data;
        const connectors = connectorsRes.data;

        setStats({
          totalPipelines: pipelines.length,
          activePipelines: pipelines.filter(p => p.is_active).length,
          totalConnectors: connectors.length,
          dataQualityScore: 95, // Mock score
        });

        setRecentPipelines(pipelines.slice(0, 5));
      } catch (error) {
        console.error('Failed to fetch dashboard data:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchDashboardData();
  }, []);

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Container maxWidth="lg">
      <Typography variant="h4" gutterBottom>
        Dashboard
      </Typography>

      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Total Pipelines
              </Typography>
              <Typography variant="h4">
                {stats.totalPipelines}
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Active Pipelines
              </Typography>
              <Typography variant="h4">
                {stats.activePipelines}
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Total Connectors
              </Typography>
              <Typography variant="h4">
                {stats.totalConnectors}
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Data Quality Score
              </Typography>
              <Typography variant="h4">
                {stats.dataQualityScore}%
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Recent Pipelines
          </Typography>
          <List>
            {recentPipelines.map((pipeline) => (
              <ListItem key={pipeline.id} divider>
                <ListItemText
                  primary={pipeline.name}
                  secondary={`Last run: ${pipeline.last_run_at ? new Date(pipeline.last_run_at).toLocaleString() : 'Never'}`}
                />
                <Chip
                  label={pipeline.status}
                  color={pipeline.status === 'completed' ? 'success' : pipeline.status === 'failed' ? 'error' : 'default'}
                  size="small"
                />
              </ListItem>
            ))}
          </List>
        </CardContent>
      </Card>
    </Container>
  );
};

// Pipelines Component
const Pipelines = () => {
  const [pipelines, setPipelines] = useState([]);
  const [loading, setLoading] = useState(true);
  const [snackbar, setSnackbar] = useState({ open: false, message: '', severity: 'info' });

  useEffect(() => {
    fetchPipelines();
  }, []);

  const fetchPipelines = async () => {
    try {
      const response = await axios.get('/api/v1/pipelines');
      setPipelines(response.data);
    } catch (error) {
      console.error('Failed to fetch pipelines:', error);
      showSnackbar('Failed to fetch pipelines', 'error');
    } finally {
      setLoading(false);
    }
  };

  const showSnackbar = (message, severity = 'info') => {
    setSnackbar({ open: true, message, severity });
  };

  const handleCloseSnackbar = () => {
    setSnackbar({ ...snackbar, open: false });
  };

  const runPipeline = async (pipelineId) => {
    try {
      await axios.post(`/api/v1/pipelines/${pipelineId}/run`);
      showSnackbar('Pipeline started successfully', 'success');
      fetchPipelines(); // Refresh the list
    } catch (error) {
      console.error('Failed to run pipeline:', error);
      showSnackbar('Failed to start pipeline', 'error');
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Container maxWidth="lg">
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4">
          Pipelines
        </Typography>
        <Box>
          <Button
            variant="outlined"
            startIcon={<RefreshIcon />}
            onClick={fetchPipelines}
            sx={{ mr: 2 }}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
          >
            Create Pipeline
          </Button>
        </Box>
      </Box>

      <Grid container spacing={3}>
        {pipelines.map((pipeline) => (
          <Grid item xs={12} md={6} lg={4} key={pipeline.id}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  {pipeline.name}
                </Typography>
                <Typography color="text.secondary" gutterBottom>
                  {pipeline.source_type} → {pipeline.destination_type}
                </Typography>
                <Box display="flex" alignItems="center" mb={2}>
                  <Chip
                    label={pipeline.status}
                    color={pipeline.status === 'completed' ? 'success' : pipeline.status === 'failed' ? 'error' : 'default'}
                    size="small"
                  />
                  <Chip
                    label={pipeline.is_active ? 'Active' : 'Inactive'}
                    color={pipeline.is_active ? 'primary' : 'default'}
                    size="small"
                    sx={{ ml: 1 }}
                  />
                </Box>
                <Typography variant="body2" color="text.secondary">
                  Last run: {pipeline.last_run_at ? new Date(pipeline.last_run_at).toLocaleString() : 'Never'}
                </Typography>
              </CardContent>
              <CardActions>
                <Button
                  size="small"
                  startIcon={<RunIcon />}
                  onClick={() => runPipeline(pipeline.id)}
                  disabled={!pipeline.is_active}
                >
                  Run
                </Button>
                <Button size="small">
                  View Details
                </Button>
              </CardActions>
            </Card>
          </Grid>
        ))}
      </Grid>

      <Snackbar
        open={snackbar.open}
        autoHideDuration={6000}
        onClose={handleCloseSnackbar}
      >
        <Alert
          onClose={handleCloseSnackbar}
          severity={snackbar.severity}
          sx={{ width: '100%' }}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Container>
  );
};

// Connectors Component
const Connectors = () => {
  const [connectors, setConnectors] = useState([]);
  const [templates, setTemplates] = useState([]);
  const [loading, setLoading] = useState(true);
  const [snackbar, setSnackbar] = useState({ open: false, message: '', severity: 'info' });

  useEffect(() => {
    fetchConnectors();
    fetchTemplates();
  }, []);

  const fetchConnectors = async () => {
    try {
      const response = await axios.get('/api/v1/connectors');
      setConnectors(response.data);
    } catch (error) {
      console.error('Failed to fetch connectors:', error);
      showSnackbar('Failed to fetch connectors', 'error');
    }
  };

  const fetchTemplates = async () => {
    try {
      const response = await axios.get('/api/v1/connector-templates');
      setTemplates(response.data);
    } catch (error) {
      console.error('Failed to fetch templates:', error);
    } finally {
      setLoading(false);
    }
  };

  const showSnackbar = (message, severity = 'info') => {
    setSnackbar({ open: true, message, severity });
  };

  const handleCloseSnackbar = () => {
    setSnackbar({ ...snackbar, open: false });
  };

  const testConnection = async (connectorId) => {
    try {
      const response = await axios.post('/api/v1/connectors/test', {
        connector_id: connectorId
      });

      if (response.data.success) {
        showSnackbar('Connection test successful', 'success');
      } else {
        showSnackbar('Connection test failed', 'error');
      }
    } catch (error) {
      console.error('Connection test failed:', error);
      showSnackbar('Connection test failed', 'error');
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Container maxWidth="lg">
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4">
          Connectors
        </Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
        >
          Create Connector
        </Button>
      </Box>

      <Grid container spacing={3}>
        {connectors.map((connector) => (
          <Grid item xs={12} md={6} lg={4} key={connector.id}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  {connector.name}
                </Typography>
                <Typography color="text.secondary" gutterBottom>
                  Type: {connector.connector_type}
                </Typography>
                <Box display="flex" alignItems="center" mb={2}>
                  <Chip
                    label={connector.is_active ? 'Active' : 'Inactive'}
                    color={connector.is_active ? 'primary' : 'default'}
                    size="small"
                  />
                  {connector.test_status && (
                    <Chip
                      label={connector.test_status}
                      color={connector.test_status === 'success' ? 'success' : 'error'}
                      size="small"
                      sx={{ ml: 1 }}
                    />
                  )}
                </Box>
                <Typography variant="body2" color="text.secondary">
                  Last tested: {connector.last_tested ? new Date(connector.last_tested).toLocaleString() : 'Never'}
                </Typography>
              </CardContent>
              <CardActions>
                <Button
                  size="small"
                  onClick={() => testConnection(connector.id)}
                >
                  Test Connection
                </Button>
                <Button size="small">
                  Edit
                </Button>
              </CardActions>
            </Card>
          </Grid>
        ))}
      </Grid>

      <Typography variant="h5" sx={{ mt: 4, mb: 2 }}>
        Available Templates
      </Typography>

      <Grid container spacing={3}>
        {templates.map((template) => (
          <Grid item xs={12} md={6} lg={4} key={template.id}>
            <Card variant="outlined">
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  {template.name}
                </Typography>
                <Typography color="text.secondary" gutterBottom>
                  Category: {template.category}
                </Typography>
                <Typography variant="body2">
                  {template.description}
                </Typography>
              </CardContent>
              <CardActions>
                <Button size="small" variant="outlined">
                  Use Template
                </Button>
              </CardActions>
            </Card>
          </Grid>
        ))}
      </Grid>

      <Snackbar
        open={snackbar.open}
        autoHideDuration={6000}
        onClose={handleCloseSnackbar}
      >
        <Alert
          onClose={handleCloseSnackbar}
          severity={snackbar.severity}
          sx={{ width: '100%' }}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Container>
  );
};

// Catalogs Component
const Catalogs = () => {
  const [catalogs, setCatalogs] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchCatalogs();
  }, []);

  const fetchCatalogs = async () => {
    try {
      const response = await axios.get('/api/v1/catalogs');
      setCatalogs(response.data);
    } catch (error) {
      console.error('Failed to fetch catalogs:', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Container maxWidth="lg">
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4">
          Data Catalogs
        </Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
        >
          Create Catalog
        </Button>
      </Box>

      <Grid container spacing={3}>
        {catalogs.map((catalog) => (
          <Grid item xs={12} md={6} lg={4} key={catalog.id}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  {catalog.name}
                </Typography>
                <Typography color="text.secondary" gutterBottom>
                  Unity Catalog: {catalog.unity_catalog_id}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Storage: {catalog.storage_location}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Created: {new Date(catalog.created_at).toLocaleDateString()}
                </Typography>
              </CardContent>
              <CardActions>
                <Button size="small">
                  View Schemas
                </Button>
                <Button size="small">
                  Manage
                </Button>
              </CardActions>
            </Card>
          </Grid>
        ))}
      </Grid>
    </Container>
  );
};

// Data Quality Component
const DataQuality = () => {
  return (
    <Container maxWidth="lg">
      <Typography variant="h4" gutterBottom>
        Data Quality
      </Typography>
      <Paper sx={{ p: 3 }}>
        <Typography variant="h6" gutterBottom>
          Quality Rules & Metrics
        </Typography>
        <Typography>
          Data quality monitoring and validation rules will be displayed here.
        </Typography>
      </Paper>
    </Container>
  );
};

// Settings Component
const Settings = () => {
  const { organization } = useAuth();

  return (
    <Container maxWidth="lg">
      <Typography variant="h4" gutterBottom>
        Settings
      </Typography>

      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Organization Settings
              </Typography>
              <Typography variant="body2" gutterBottom>
                Name: {organization?.display_name}
              </Typography>
              <Typography variant="body2" gutterBottom>
                ID: {organization?.name}
              </Typography>
              <Typography variant="body2" gutterBottom>
                Cost Center: {organization?.cost_center}
              </Typography>
              <Typography variant="body2">
                Compliance Level: {organization?.compliance_level}
              </Typography>
            </CardContent>
            <CardActions>
              <Button size="small">
                Edit Settings
              </Button>
            </CardActions>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Resource Quotas
              </Typography>
              <Typography variant="body2" gutterBottom>
                Max DBU/Hour: {organization?.resource_quotas?.max_dbu_per_hour}
              </Typography>
              <Typography variant="body2" gutterBottom>
                Max Storage GB: {organization?.resource_quotas?.max_storage_gb}
              </Typography>
              <Typography variant="body2">
                Max API Calls/Min: {organization?.resource_quotas?.max_api_calls_per_minute}
              </Typography>
            </CardContent>
            <CardActions>
              <Button size="small">
                Request Quota Increase
              </Button>
            </CardActions>
          </Card>
        </Grid>
      </Grid>
    </Container>
  );
};

// Main App Component
const App = () => {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <AuthProvider>
        <Router>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route
              path="/*"
              element={
                <ProtectedRoute>
                  <Layout>
                    <Routes>
                      <Route path="/" element={<Navigate to="/dashboard" />} />
                      <Route path="/dashboard" element={<Dashboard />} />
                      <Route path="/pipelines" element={<Pipelines />} />
                      <Route path="/connectors" element={<Connectors />} />
                      <Route path="/catalogs" element={<Catalogs />} />
                      <Route path="/data-quality" element={<DataQuality />} />
                      <Route path="/settings" element={<Settings />} />
                    </Routes>
                  </Layout>
                </ProtectedRoute>
              }
            />
          </Routes>
        </Router>
      </AuthProvider>
    </ThemeProvider>
  );
};

export default App;