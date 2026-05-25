// src/components/layout/Layout.jsx
import { Outlet } from 'react-router-dom'
import Navbar from './Navbar'

export default function Layout() {
  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      <Navbar />
      <main className="flex-1">
        <div className="max-w-screen-xl mx-auto px-6 py-6">
          <Outlet />
        </div>
      </main>
    </div>
  )
}