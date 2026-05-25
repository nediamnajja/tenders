// src/App.jsx
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { AuthProvider, useAuth } from './lib/auth'
import Layout       from './components/layout/Layout'
import Login        from './pages/Login'
import Today        from './pages/Today'
import Tenders      from './pages/Tenders'
import TenderDetail from './pages/TenderDetail'
import Decisions    from './pages/Decisions'
import Dashboard    from './pages/Dashboard'

function PrivateRoute({ children }) {
  const { user } = useAuth()
  if (!user) return <Navigate to="/login" replace />
  return children
}

function PublicRoute({ children }) {
  const { user } = useAuth()
  if (user) return <Navigate to="/today" replace />
  return children
}

export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={
            <PublicRoute><Login /></PublicRoute>
          } />
          <Route path="/" element={
            <PrivateRoute><Layout /></PrivateRoute>
          }>
            <Route index              element={<Navigate to="/today" replace />} />
            <Route path="today"       element={<Today />}        />
            <Route path="tenders"     element={<Tenders />}      />
            <Route path="tenders/:id" element={<TenderDetail />} />
            <Route path="decisions"   element={<Decisions />}    />
            <Route path="dashboard"   element={<Dashboard />}    />
          </Route>
          <Route path="*" element={<Navigate to="/today" replace />} />
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  )
}