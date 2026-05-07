// import { useState } from 'react'
// import { useNavigate } from 'react-router-dom'
// import { useAuth } from '../lib/auth'
// import { Spinner } from '../components/ui'

// export default function Login() {
//   const { login, loading } = useAuth()
//   const navigate = useNavigate()
//   const [email,    setEmail]    = useState('')
//   const [password, setPassword] = useState('')
//   const [error,    setError]    = useState('')

//   async function handleSubmit(e) {
//     e.preventDefault()
//     setError('')
//     const result = await login(email, password)
//     if (result.ok) {
//       navigate('/today')
//     } else {
//       setError(result.error)
//     }
//   }

//   return (
//     <div className="min-h-screen bg-kpmg-blue flex items-center justify-center p-4">
//       <div className="w-full max-w-sm">
//         <div className="text-center mb-8">
//           <div className="text-white text-4xl font-bold tracking-tight mb-1">KPMG</div>
//           <div className="text-blue-300 text-sm">Tender Intelligence Platform</div>
//         </div>

//         <div className="bg-white rounded-2xl shadow-2xl p-8">
//           <h2 className="text-xl font-semibold text-gray-900 mb-6">Sign in</h2>
//           <form onSubmit={handleSubmit} className="space-y-4">
//             <div>
//               <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
//               <input
//                 type="email"
//                 value={email}
//                 onChange={e => setEmail(e.target.value)}
//                 required
//                 autoFocus
//                 placeholder="you@kpmg.com"
//                 className="block w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
//                            focus:border-kpmg-blue focus:outline-none focus:ring-1 focus:ring-kpmg-blue"
//               />
//             </div>
//             <div>
//               <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
//               <input
//                 type="password"
//                 value={password}
//                 onChange={e => setPassword(e.target.value)}
//                 required
//                 placeholder="••••••••"
//                 className="block w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
//                            focus:border-kpmg-blue focus:outline-none focus:ring-1 focus:ring-kpmg-blue"
//               />
//             </div>

//             {error && (
//               <div className="text-red-600 text-sm bg-red-50 border border-red-200 rounded-lg px-3 py-2">
//                 {error}
//               </div>
//             )}

//             <button
//               type="submit"
//               disabled={loading}
//               className="w-full btn-primary py-2.5 flex items-center justify-center gap-2"
//             >
//               {loading ? <Spinner size="sm" /> : null}
//               {loading ? 'Signing in…' : 'Sign in'}
//             </button>
//           </form>
//         </div>

//         <p className="text-center text-blue-300 text-xs mt-6">
//           © {new Date().getFullYear()} KPMG — Confidential
//         </p>
//       </div>
//     </div>
//   )
// }


// import { useState, useEffect, useRef } from 'react'
// import { useNavigate } from 'react-router-dom'
// import { useAuth } from '../lib/auth'
// import { Eye, EyeOff, AlertCircle } from 'lucide-react'

// // ── Animated Canvas Background ────────────────────────────────────────────────

// function GeometricBackground() {
//   const canvasRef = useRef(null)

//   useEffect(() => {
//     const canvas = canvasRef.current
//     const ctx    = canvas.getContext('2d')
//     let animId

//     const resize = () => {
//       canvas.width  = canvas.offsetWidth
//       canvas.height = canvas.offsetHeight
//     }
//     resize()
//     window.addEventListener('resize', resize)

//     const colors = [
//       'rgba(0, 51, 141, 0.6)',
//       'rgba(0, 94, 184, 0.5)',
//       'rgba(0, 145, 218, 0.4)',
//       'rgba(72, 54, 152, 0.4)',
//       'rgba(0, 163, 161, 0.3)',
//     ]

//     const shapes = Array.from({ length: 22 }, (_, i) => ({
//       x:        Math.random() * canvas.width,
//       y:        Math.random() * canvas.height,
//       size:     Math.random() * 60 + 20,
//       color:    colors[i % colors.length],
//       speedX:   (Math.random() - 0.5) * 0.4,
//       speedY:   (Math.random() - 0.5) * 0.4,
//       type:     ['circle', 'triangle', 'square', 'diamond'][i % 4],
//       opacity:  Math.random() * 0.5 + 0.2,
//       rotation: Math.random() * Math.PI * 2,
//       rotSpeed: (Math.random() - 0.5) * 0.01,
//     }))

//     function drawLine(x1, y1, x2, y2, opacity) {
//       ctx.beginPath()
//       ctx.moveTo(x1, y1)
//       ctx.lineTo(x2, y2)
//       ctx.strokeStyle = `rgba(0, 145, 218, ${opacity})`
//       ctx.lineWidth = 0.5
//       ctx.stroke()
//     }

//     function drawShape(s) {
//       ctx.save()
//       ctx.translate(s.x, s.y)
//       ctx.rotate(s.rotation)
//       ctx.globalAlpha = s.opacity
//       ctx.fillStyle = s.color

//       if (s.type === 'circle') {
//         ctx.beginPath()
//         ctx.arc(0, 0, s.size / 2, 0, Math.PI * 2)
//         ctx.fill()
//       } else if (s.type === 'triangle') {
//         ctx.beginPath()
//         ctx.moveTo(0, -s.size / 2)
//         ctx.lineTo(s.size / 2, s.size / 2)
//         ctx.lineTo(-s.size / 2, s.size / 2)
//         ctx.closePath()
//         ctx.fill()
//       } else if (s.type === 'square') {
//         ctx.fillRect(-s.size / 2, -s.size / 2, s.size, s.size)
//       } else if (s.type === 'diamond') {
//         ctx.beginPath()
//         ctx.moveTo(0, -s.size / 2)
//         ctx.lineTo(s.size / 2, 0)
//         ctx.lineTo(0, s.size / 2)
//         ctx.lineTo(-s.size / 2, 0)
//         ctx.closePath()
//         ctx.fill()
//       }
//       ctx.restore()
//     }

//     function animate() {
//       ctx.clearRect(0, 0, canvas.width, canvas.height)

//       const grad = ctx.createLinearGradient(0, 0, canvas.width, canvas.height)
//       grad.addColorStop(0, '#00205B')
//       grad.addColorStop(0.5, '#00338D')
//       grad.addColorStop(1, '#001F5C')
//       ctx.fillStyle = grad
//       ctx.fillRect(0, 0, canvas.width, canvas.height)

//       for (let i = 0; i < shapes.length; i++) {
//         for (let j = i + 1; j < shapes.length; j++) {
//           const dx   = shapes[i].x - shapes[j].x
//           const dy   = shapes[i].y - shapes[j].y
//           const dist = Math.sqrt(dx * dx + dy * dy)
//           if (dist < 180) {
//             drawLine(shapes[i].x, shapes[i].y, shapes[j].x, shapes[j].y, (1 - dist / 180) * 0.15)
//           }
//         }
//       }

//       shapes.forEach(s => {
//         drawShape(s)
//         s.x        += s.speedX
//         s.y        += s.speedY
//         s.rotation += s.rotSpeed
//         if (s.x < -s.size) s.x = canvas.width + s.size
//         if (s.x > canvas.width + s.size) s.x = -s.size
//         if (s.y < -s.size) s.y = canvas.height + s.size
//         if (s.y > canvas.height + s.size) s.y = -s.size
//       })

//       animId = requestAnimationFrame(animate)
//     }

//     animate()
//     return () => {
//       cancelAnimationFrame(animId)
//       window.removeEventListener('resize', resize)
//     }
//   }, [])

//   return <canvas ref={canvasRef} className="absolute inset-0 w-full h-full" />
// }

// // ── Avatar color helper (exported for use in Sidebar) ────────────────────────

// const AVATAR_COLORS = [
//   'bg-blue-500', 'bg-purple-500', 'bg-green-500', 'bg-orange-500',
//   'bg-pink-500',  'bg-teal-500',  'bg-red-500',   'bg-indigo-500',
// ]

// export function getAvatarColor(email = '') {
//   let hash = 0
//   for (let i = 0; i < email.length; i++) hash = email.charCodeAt(i) + ((hash << 5) - hash)
//   return AVATAR_COLORS[Math.abs(hash) % AVATAR_COLORS.length]
// }

// export function Avatar({ email, name, size = 'md' }) {
//   const letter = (name || email || '?')[0].toUpperCase()
//   const color  = getAvatarColor(email)
//   const sizes  = { sm: 'h-7 w-7 text-xs', md: 'h-9 w-9 text-sm', lg: 'h-12 w-12 text-lg' }
//   return (
//     <div className={`${sizes[size]} ${color} rounded-full flex items-center justify-center text-white font-bold flex-shrink-0`}>
//       {letter}
//     </div>
//   )
// }

// // ── Main Login Page ───────────────────────────────────────────────────────────

// export default function Login() {
//   const { login, loading } = useAuth()
//   const navigate = useNavigate()

//   const [email,      setEmail]      = useState('')
//   const [password,   setPassword]   = useState('')
//   const [showPass,   setShowPass]   = useState(false)
//   const [error,      setError]      = useState('')
//   const [emailError, setEmailError] = useState('')

//   function validateEmail(val) {
//     if (val && !val.toLowerCase().endsWith('@kpmg.com')) {
//       setEmailError('Only @kpmg.com email addresses are allowed')
//     } else {
//       setEmailError('')
//     }
//   }

//   async function handleSubmit(e) {
//     e.preventDefault()
//     setError('')
//     if (!email.toLowerCase().endsWith('@kpmg.com')) {
//       setEmailError('Only @kpmg.com email addresses are allowed')
//       return
//     }
//     const result = await login(email, password)
//     if (result.ok) {
//       navigate('/today')
//     } else {
//       setError(result.error)
//     }
//   }

//   return (
//     <div className="min-h-screen flex">

//       {/* LEFT — Animated background */}
//       <div className="hidden lg:flex lg:w-3/5 relative overflow-hidden flex-col">
//         <GeometricBackground />
//         <div className="relative z-10 flex flex-col h-full p-12">

//           {/* Logo */}
//           <div className="flex items-center gap-3">
//             <div className="bg-white rounded-lg px-3 py-2">
//               <div className="text-kpmg-blue font-black text-2xl tracking-tighter leading-none">KPMG</div>
//             </div>
//           </div>

//           {/* Center content */}
//           <div className="flex-1 flex flex-col items-center justify-center text-center px-8">
//             <div className="inline-flex items-center gap-2 bg-white/10 border border-white/20
//                             rounded-full px-4 py-1.5 text-blue-200 text-xs font-medium mb-8">
//               <span className="h-1.5 w-1.5 rounded-full bg-kpmg-teal animate-pulse" />
//               Tender Intelligence Platform
//             </div>

//             <h1 className="text-4xl font-bold text-white leading-tight mb-4">
//               Cutting through<br />
//               <span className="text-kpmg-cobalt">complexity.</span>
//             </h1>
//             <p className="text-blue-200 text-base max-w-sm leading-relaxed">
//               AI-powered tender intelligence to help KPMG teams identify,
//               evaluate and prioritize the right opportunities — faster.
//             </p>

//             <div className="flex flex-wrap gap-2 justify-center mt-10">
//               {['Daily GO recommendations', 'Multi-portal coverage', 'LLM-powered summaries', 'Team decisions'].map(f => (
//                 <span key={f} className="bg-white/10 border border-white/20 text-blue-100 text-xs rounded-full px-3 py-1">
//                   {f}
//                 </span>
//               ))}
//             </div>
//           </div>

//           <div className="text-blue-300 text-xs">
//             © {new Date().getFullYear()} KPMG International — Confidential
//           </div>
//         </div>
//       </div>

//       {/* RIGHT — Login form */}
//       <div className="w-full lg:w-2/5 flex items-center justify-center bg-white p-8">
//         <div className="w-full max-w-sm">

//           {/* Mobile logo */}
//           <div className="lg:hidden mb-8 text-center">
//             <div className="inline-flex bg-kpmg-blue rounded-lg px-3 py-2 mb-3">
//               <div className="text-white font-black text-2xl tracking-tighter">KPMG</div>
//             </div>
//             <p className="text-gray-500 text-sm">Tender Intelligence Platform</p>
//           </div>

//           <div className="mb-8">
//             <h2 className="text-2xl font-bold text-gray-900">Welcome back</h2>
//             <p className="text-gray-500 text-sm mt-1">Sign in to your KPMG account</p>
//           </div>

//           <form onSubmit={handleSubmit} className="space-y-5">

//             {/* Email */}
//             <div>
//               <label className="block text-sm font-medium text-gray-700 mb-1.5">Email address</label>
//               <input
//                 type="email"
//                 value={email}
//                 onChange={e => { setEmail(e.target.value); validateEmail(e.target.value) }}
//                 onBlur={e => validateEmail(e.target.value)}
//                 required
//                 autoFocus
//                 placeholder="firstname.lastname@kpmg.com"
//                 className={`block w-full rounded-xl border px-4 py-3 text-sm transition-colors
//                   placeholder-gray-400 focus:outline-none focus:ring-2
//                   ${emailError
//                     ? 'border-red-300 focus:border-red-400 focus:ring-red-100'
//                     : 'border-gray-200 focus:border-kpmg-blue focus:ring-blue-100'}`}
//               />
//               {emailError && (
//                 <p className="mt-1.5 text-xs text-red-600 flex items-center gap-1">
//                   <AlertCircle className="h-3 w-3" /> {emailError}
//                 </p>
//               )}
//             </div>

//             {/* Password */}
//             <div>
//               <label className="block text-sm font-medium text-gray-700 mb-1.5">Password</label>
//               <div className="relative">
//                 <input
//                   type={showPass ? 'text' : 'password'}
//                   value={password}
//                   onChange={e => setPassword(e.target.value)}
//                   required
//                   placeholder="••••••••••"
//                   className="block w-full rounded-xl border border-gray-200 px-4 py-3 pr-11
//                              text-sm placeholder-gray-400 focus:outline-none focus:ring-2
//                              focus:border-kpmg-blue focus:ring-blue-100 transition-colors"
//                 />
//                 <button
//                   type="button"
//                   onClick={() => setShowPass(!showPass)}
//                   className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
//                 >
//                   {showPass ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
//                 </button>
//               </div>
//             </div>

//             {/* Error */}
//             {error && (
//               <div className="flex items-center gap-2 text-red-600 bg-red-50 border border-red-200 rounded-xl px-4 py-3 text-sm">
//                 <AlertCircle className="h-4 w-4 flex-shrink-0" /> {error}
//               </div>
//             )}

//             {/* Submit */}
//             <button
//               type="submit"
//               disabled={loading || !!emailError}
//               className="w-full bg-kpmg-blue text-white rounded-xl py-3 text-sm font-semibold
//                          hover:bg-kpmg-lightblue transition-colors duration-150
//                          disabled:opacity-50 disabled:cursor-not-allowed
//                          flex items-center justify-center gap-2 mt-2"
//             >
//               {loading ? (
//                 <>
//                   <div className="h-4 w-4 animate-spin rounded-full border-2 border-white/30 border-t-white" />
//                   Signing in…
//                 </>
//               ) : 'Sign in'}
//             </button>
//           </form>

//           <p className="mt-8 text-center text-xs text-gray-400">
//             Access restricted to KPMG personnel only.<br />
//             Contact your administrator to request access.
//           </p>
//         </div>
//       </div>
//     </div>
//   )
// }

import { useState, useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '../lib/auth'
import { Eye, EyeOff, AlertCircle, Globe, ChevronDown } from 'lucide-react'

const ALLOWED_EMAILS = ['nediamnajja.tbs@gmail.com']
function isEmailAllowed(email) {
  const lower = email.toLowerCase().trim()
  return lower.endsWith('@kpmg.com') || ALLOWED_EMAILS.includes(lower)
}

// ─────────────────────────────────────────────────────────────────────────────
//  WORLD MAP — precise grid-based continent outlines
//  Each point is [longitude, latitude] placed on a 4° grid
// ─────────────────────────────────────────────────────────────────────────────
const CONTINENT_DOTS = [
  // ── NORTH AMERICA ────────────────────────────────────────────────────────
  [-124,48],[-120,48],[-116,48],[-112,48],[-108,48],[-104,48],[-100,48],[-96,48],[-92,48],[-88,48],[-84,46],[-80,44],[-76,44],[-72,44],[-68,46],[-64,44],
  [-124,44],[-120,44],[-116,42],[-112,40],[-108,40],[-104,38],[-100,38],[-96,36],[-92,36],[-88,34],[-84,34],[-80,32],[-76,34],[-72,42],[-68,44],
  [-120,40],[-116,38],[-112,36],[-108,34],[-104,32],[-100,30],[-96,28],[-92,28],[-88,30],[-84,30],[-80,28],
  [-116,32],[-112,30],[-108,28],[-104,26],[-100,24],[-96,22],[-92,20],[-88,18],[-84,12],[-80,10],[-76,8],[-72,10],[-68,12],
  [-128,50],[-124,52],[-120,52],[-116,52],[-112,52],[-108,52],[-104,52],[-100,52],[-96,52],[-92,52],[-88,50],[-84,48],
  [-132,56],[-128,56],[-124,56],[-120,56],[-116,56],[-112,56],[-108,56],[-104,56],[-100,56],[-96,54],
  [-136,60],[-132,60],[-128,60],[-124,60],[-120,58],[-116,58],[-112,58],[-108,58],[-104,58],[-100,58],
  [-140,64],[-136,64],[-132,64],[-128,62],[-124,62],[-120,62],[-116,62],[-112,62],
  [-148,64],[-144,62],[-140,60],[-144,60],[-148,60],[-152,60],[-156,58],[-160,58],[-164,60],[-168,64],
  // Greenland
  [-52,72],[-48,72],[-44,70],[-40,68],[-44,66],[-48,66],[-52,68],[-56,68],[-52,64],[-48,64],[-44,64],[-40,64],[-36,66],[-32,68],
  // ── SOUTH AMERICA ─────────────────────────────────────────────────────────
  [-76,10],[-72,10],[-68,8],[-64,8],[-60,8],[-56,8],[-52,6],[-48,4],[-44,4],
  [-76,6],[-72,4],[-68,4],[-64,4],[-60,4],[-56,4],[-52,4],[-48,2],[-44,2],[-40,0],
  [-76,2],[-72,0],[-68,0],[-64,0],[-60,0],[-56,0],[-52,-2],[-48,-2],[-44,-2],[-40,-4],
  [-72,-4],[-68,-4],[-64,-4],[-60,-4],[-56,-4],[-52,-6],[-48,-6],[-44,-6],[-40,-6],
  [-70,-8],[-66,-10],[-62,-10],[-58,-10],[-54,-10],[-50,-10],[-46,-10],[-42,-10],
  [-68,-14],[-64,-14],[-60,-14],[-56,-14],[-52,-14],[-48,-14],[-44,-14],[-40,-14],
  [-66,-18],[-62,-18],[-58,-18],[-54,-18],[-50,-18],[-46,-18],[-42,-18],
  [-64,-22],[-60,-22],[-56,-22],[-52,-22],[-48,-22],[-44,-22],
  [-60,-26],[-56,-26],[-52,-26],[-48,-26],[-44,-26],
  [-60,-30],[-56,-30],[-52,-30],[-48,-30],
  [-60,-34],[-56,-34],[-52,-34],
  [-58,-38],[-54,-38],
  [-56,-42],
  // ── EUROPE ────────────────────────────────────────────────────────────────
  [-8,36],[-4,36],[0,36],[4,36],[8,38],[12,38],[16,38],[20,38],[24,38],[28,38],
  [-8,40],[-4,40],[0,40],[4,40],[8,42],[12,42],[16,42],[20,42],[24,42],[28,42],[32,42],
  [-8,44],[-4,44],[0,44],[4,44],[8,46],[12,46],[16,46],[20,46],[24,46],[28,46],[32,46],
  [-4,48],[0,48],[4,48],[8,50],[12,50],[16,50],[20,50],[24,50],[28,50],[32,50],
  [-4,52],[0,52],[4,52],[8,54],[12,54],[16,54],[20,54],[24,54],[28,54],
  [4,56],[8,58],[12,58],[16,58],[20,58],[24,58],[28,58],
  [8,62],[12,62],[16,62],[20,62],[24,62],[28,62],
  [12,66],[16,66],[20,66],[24,66],[28,66],
  [16,70],[20,70],[24,70],[28,70],
  [-8,44],[-8,48],[-8,52],[-4,56],[0,56],
  [28,42],[32,44],[36,42],[28,46],[32,46],[36,46],
  [20,36],[24,36],[28,36],[32,36],[20,40],[24,40],
  // ── AFRICA ────────────────────────────────────────────────────────────────
  [-16,20],[-12,20],[-8,20],[-4,20],[0,20],[4,20],[8,20],[12,20],[16,20],[20,20],[24,20],[28,20],[32,20],[36,20],[40,20],[44,16],[48,12],
  [-16,16],[-12,16],[-8,16],[-4,16],[0,16],[4,16],[8,16],[12,16],[16,16],[20,16],[24,16],[28,16],[32,16],[36,16],[40,16],[44,12],
  [-16,12],[-12,12],[-8,12],[-4,12],[0,12],[4,12],[8,12],[12,12],[16,12],[20,12],[24,12],[28,12],[32,12],[36,12],[40,12],[44,8],
  [-12,8],[-8,8],[-4,8],[0,8],[4,8],[8,8],[12,8],[16,8],[20,8],[24,8],[28,8],[32,8],[36,8],[40,8],[44,4],
  [-8,4],[-4,4],[0,4],[4,4],[8,4],[12,4],[16,4],[20,4],[24,4],[28,4],[32,4],[36,4],[40,4],
  [-4,0],[0,0],[4,0],[8,0],[12,0],[16,0],[20,0],[24,0],[28,0],[32,0],[36,0],[40,0],
  [0,-4],[4,-4],[8,-4],[12,-4],[16,-4],[20,-4],[24,-4],[28,-4],[32,-4],[36,-4],[40,-4],
  [4,-8],[8,-8],[12,-8],[16,-8],[20,-8],[24,-8],[28,-8],[32,-8],[36,-8],[40,-8],
  [8,-12],[12,-12],[16,-12],[20,-12],[24,-12],[28,-12],[32,-12],[36,-12],
  [12,-16],[16,-16],[20,-16],[24,-16],[28,-16],[32,-16],
  [16,-20],[20,-20],[24,-20],[28,-20],[32,-20],
  [16,-24],[20,-24],[24,-24],[28,-24],
  [20,-28],[24,-28],[28,-28],
  [20,-32],[24,-32],[28,-32],
  [20,-36],[24,-36],
  [-16,24],[-12,24],[-8,24],[-4,24],[0,24],[4,24],[8,24],[12,24],[16,24],[20,24],[24,24],[28,24],[32,24],[36,24],[40,24],[44,20],
  [-12,28],[-8,28],[-4,28],[0,28],[4,28],[8,28],[12,28],[16,28],[20,28],[24,28],[28,28],[32,28],[36,28],[40,28],
  [-8,32],[-4,32],[0,32],[4,32],[8,32],[12,32],[16,32],[20,32],[24,32],[28,32],[32,32],[36,32],
  [32,4],[36,4],[40,4],[44,4],[32,8],[36,8],[40,8],[44,8],[32,12],[36,12],[40,12],
  // ── MIDDLE EAST ───────────────────────────────────────────────────────────
  [36,32],[40,32],[44,32],[48,28],[52,28],[56,24],[60,20],
  [36,36],[40,36],[44,36],[48,32],[52,32],[56,28],[60,24],[64,20],
  [40,40],[44,40],[48,36],[52,36],[56,32],[60,28],[64,24],
  [44,28],[48,24],[52,24],[56,20],[60,16],[64,16],
  [48,20],[52,20],[56,16],[60,12],[64,12],
  // ── CENTRAL & SOUTH ASIA ──────────────────────────────────────────────────
  [60,36],[64,36],[68,36],[72,36],[76,32],[80,28],[84,24],[88,24],[92,24],
  [64,40],[68,40],[72,40],[76,36],[80,32],[84,28],[88,28],[92,28],
  [68,44],[72,44],[76,40],[80,36],[84,32],[88,28],[92,28],
  [72,48],[76,44],[80,40],[84,36],[88,32],[92,28],
  [60,32],[64,28],[68,24],[72,20],[76,16],[80,12],[84,12],[88,20],[92,20],
  [64,24],[68,20],[72,16],[76,12],[80,8],[84,8],[88,16],[92,16],
  // ── EAST & SOUTHEAST ASIA ─────────────────────────────────────────────────
  [96,28],[100,24],[104,20],[108,16],[112,12],[116,8],[120,8],[124,8],[128,12],[132,32],[136,36],[140,40],
  [100,32],[104,28],[108,24],[112,20],[116,16],[120,12],[124,16],[128,20],[132,36],[136,40],[140,44],
  [104,36],[108,32],[112,28],[116,24],[120,20],[124,24],[128,28],[132,32],[136,36],[140,40],
  [108,40],[112,36],[116,32],[120,28],[124,32],[128,36],[132,40],[136,44],
  [112,44],[116,40],[120,36],[124,40],[128,44],[132,48],[136,48],
  [116,48],[120,44],[124,44],[128,48],[132,52],
  [120,52],[124,52],[128,52],[132,56],[136,52],
  [124,56],[128,56],[132,60],[136,60],[140,56],[144,56],[148,56],[152,52],
  [128,60],[132,64],[136,64],[140,64],[144,60],[148,60],
  [136,68],[140,68],[144,64],[148,64],[152,60],[156,56],[160,56],[164,56],[168,56],
  [104,4],[108,0],[112,-4],[116,-4],[120,-2],[124,2],
  [108,-4],[112,-8],[116,-8],[120,-4],[124,0],
  [112,-8],[116,-12],[120,-8],[124,-4],
  [100,4],[104,8],[108,12],[96,16],[100,20],[104,24],
  // ── AUSTRALIA ─────────────────────────────────────────────────────────────
  [116,-20],[120,-20],[124,-20],[128,-20],[132,-20],[136,-20],[140,-20],[144,-20],[148,-20],
  [116,-24],[120,-24],[124,-24],[128,-24],[132,-24],[136,-24],[140,-24],[144,-24],[148,-24],[152,-24],
  [120,-28],[124,-28],[128,-28],[132,-28],[136,-28],[140,-28],[144,-28],[148,-28],[152,-28],
  [124,-32],[128,-32],[132,-32],[136,-32],[140,-32],[144,-32],[148,-32],
  [128,-36],[132,-36],[136,-36],[140,-36],[144,-36],
  [132,-40],[136,-40],[140,-40],
  [136,-44],[140,-44],
  [152,-28],[156,-32],[160,-36],[164,-36],[168,-36],[172,-36],[176,-40],
  // ── RUSSIA & NORTH ASIA ───────────────────────────────────────────────────
  [40,52],[44,52],[48,52],[52,52],[56,52],[60,52],[64,52],[68,52],[72,52],[76,52],[80,52],[84,52],[88,52],[92,52],[96,52],[100,52],[104,52],[108,52],[112,52],[116,52],[120,52],[124,52],[128,52],[132,52],[136,52],[140,52],[144,52],[148,52],[152,52],[156,52],[160,52],[164,52],[168,52],[172,52],[176,52],
  [40,56],[44,56],[48,56],[52,56],[56,56],[60,56],[64,56],[68,56],[72,56],[76,56],[80,56],[84,56],[88,56],[92,56],[96,56],[100,56],[104,56],[108,56],[112,56],[116,56],[120,56],[124,56],[128,56],[132,56],[136,56],[140,56],[144,56],[148,56],[152,56],[156,56],[160,56],[164,56],[168,56],
  [40,60],[44,60],[48,60],[52,60],[56,60],[60,60],[64,60],[68,60],[72,60],[76,60],[80,60],[84,60],[88,60],[92,60],[96,60],[100,60],[104,60],[108,60],[112,60],[116,60],[120,60],[124,60],[128,60],[132,60],[136,60],[140,60],[144,60],[148,60],[152,60],[156,60],[160,60],
  [40,64],[44,64],[48,64],[52,64],[56,64],[60,64],[64,64],[68,64],[72,64],[76,64],[80,64],[84,64],[88,64],[92,64],[96,64],[100,64],[104,64],[108,64],[112,64],[116,64],[120,64],[124,64],[128,64],[132,64],[136,64],[140,64],[144,64],[148,64],[152,64],[156,64],
  [40,68],[44,68],[48,68],[52,68],[56,68],[60,68],[64,68],[68,68],[72,68],[76,68],[80,68],[84,68],[88,68],[92,68],[96,68],[100,68],[104,68],[108,68],[112,68],[116,68],[120,68],[124,68],[128,68],[132,68],[136,68],[140,68],[144,68],[148,68],
  [60,72],[64,72],[68,72],[72,72],[76,72],[80,72],[84,72],[88,72],[92,72],[96,72],[100,72],[104,72],[108,72],[112,72],[116,72],[120,72],[124,72],[128,72],[132,72],[136,72],[140,72],
  [80,76],[84,76],[88,76],[92,76],[96,76],[100,76],[104,76],[108,76],[112,76],[116,76],[120,76],
  // ── JAPAN & KOREA ─────────────────────────────────────────────────────────
  [130,34],[134,34],[138,36],[142,40],[138,44],[134,42],[130,38],
  [126,34],[128,36],[130,38],[126,36],[128,34],
  [140,36],[144,40],[140,44],
]

// ── Hotspots — well-distributed across continents, KPMG blue palette only ────
const HOTSPOTS = [
  // Africa (most dense — primary market)
  { lng: 3.4,  lat: 6.5,  },{ lng: 36.8, lat: -1.3, },{ lng: 31.2, lat: 30.1 },
  { lng: 13.5, lat: 12.4, },{ lng: -1.7, lat: 12.4, },{ lng: 38.7, lat: 9.0  },
  { lng: 7.5,  lat: 9.1,  },{ lng: -16.6,lat: 13.5, },{ lng: 32.5, lat: 15.6 },
  { lng: 29.4, lat: -3.4, },{ lng: -4.0, lat: 5.4,  },{ lng: 20.5, lat: -4.3 },
  { lng: 34.8, lat: -6.0, },{ lng: 18.6, lat: 4.4,  },{ lng: 28.0, lat:-26.2 },
  { lng:-8.0,  lat: 11.9, },{ lng: 2.1,  lat: 13.5, },{ lng: 15.0, lat: -4.0 },
  // Middle East
  { lng: 44.4, lat: 33.3, },{ lng: 35.9, lat: 31.9, },{ lng: 51.4, lat: 25.3 },
  { lng: 47.5, lat: 8.0,  },{ lng: 55.3, lat: 25.3, },
  // Asia
  { lng: 77.2, lat: 28.6, },{ lng:104.9, lat: 11.6, },{ lng: 90.4, lat: 23.7 },
  { lng:106.8, lat: 10.8, },{ lng:121.5, lat: 25.0, },
  // Europe
  { lng: 2.3,  lat: 48.9, },{ lng: 13.4, lat: 52.5, },{ lng: 10.0, lat: 53.6 },
  // Latin America
  { lng:-74.1, lat: 4.7,  },{ lng:-77.0, lat:-12.0, },{ lng:-47.9, lat:-15.8 },
  { lng:-58.4, lat:-34.6, },
  // North America
  { lng:-87.6, lat: 41.9, },{ lng:-73.9, lat: 40.7, },
]

function WorldMapCanvas() {
  const canvasRef = useRef(null)

  useEffect(() => {
    const canvas = canvasRef.current
    const ctx    = canvas.getContext('2d')
    let animId

    const resize = () => {
      canvas.width  = canvas.offsetWidth
      canvas.height = canvas.offsetHeight
    }
    resize()
    window.addEventListener('resize', resize)

    function project(lng, lat, w, h) {
      const x = (lng + 180) / 360 * w
      const y = (90 - lat) / 180 * h
      return { x, y }
    }

    // Each hotspot has fully independent random timing
    // phase offset + random cycle length creates natural async feel
    const spots = HOTSPOTS.map((h, i) => ({
      ...h,
      // Spread phases so they never sync
      phase:    Math.random() * Math.PI * 2,
      // Vary cycle speed: some pulse every ~2s, some every ~4s
      speed:    0.005 + Math.random() * 0.012,
      // Random initial delay so first appearance is staggered
      delay:    Math.random() * 200,
      // Color: KPMG blue family only
      color:    ['#005EB8','#0091DA','#00338D','#483698','#00A3A1'][i % 5],
      active:   false,
      frame:    0,
    }))

    function animate() {
      const w = canvas.width
      const h = canvas.height
      ctx.clearRect(0, 0, w, h)

      // Background — very light blue-white
      ctx.fillStyle = '#F7F9FF'
      ctx.fillRect(0, 0, w, h)

      // Subtle gradient wash — adds depth without darkening
      const wash = ctx.createLinearGradient(0, 0, w, h)
      wash.addColorStop(0,   'rgba(0, 51, 141, 0.025)')
      wash.addColorStop(0.5, 'rgba(0, 94, 184, 0.018)')
      wash.addColorStop(1,   'rgba(0, 145, 218, 0.025)')
      ctx.fillStyle = wash
      ctx.fillRect(0, 0, w, h)

      // ── World map dots ─────────────────────────────────────────────────────
      CONTINENT_DOTS.forEach(([lng, lat]) => {
        const { x, y } = project(lng, lat, w, h)
        ctx.beginPath()
        ctx.arc(x, y, 1.6, 0, Math.PI * 2)
        ctx.fillStyle = 'rgba(0, 94, 184, 0.18)'
        ctx.fill()
      })

      // ── Animated hotspots — independent timing ─────────────────────────────
      spots.forEach(s => {
        s.phase += s.speed
        // alpha: 0→1→0 per cycle (sin wave, only positive half)
        const raw   = Math.sin(s.phase)
        const alpha = Math.max(0, raw)  // only show on positive phase

        if (alpha < 0.01) return  // skip invisible dots

        const { x, y } = project(s.lng, s.lat, w, h)

        // Parse hex color to rgb
        const r = parseInt(s.color.slice(1,3),16)
        const g = parseInt(s.color.slice(3,5),16)
        const b = parseInt(s.color.slice(5,7),16)

        // Outer glow — very soft
        const grd = ctx.createRadialGradient(x, y, 0, x, y, 16)
        grd.addColorStop(0, `rgba(${r},${g},${b},${alpha * 0.18})`)
        grd.addColorStop(1, `rgba(${r},${g},${b},0)`)
        ctx.beginPath()
        ctx.arc(x, y, 16, 0, Math.PI * 2)
        ctx.fillStyle = grd
        ctx.fill()

        // Pulse ring — appears as alpha grows
        ctx.beginPath()
        ctx.arc(x, y, 5 + alpha * 3, 0, Math.PI * 2)
        ctx.strokeStyle = `rgba(${r},${g},${b},${alpha * 0.35})`
        ctx.lineWidth = 0.8
        ctx.stroke()

        // Core dot
        ctx.beginPath()
        ctx.arc(x, y, 2.5, 0, Math.PI * 2)
        ctx.fillStyle = `rgba(${r},${g},${b},${0.5 + alpha * 0.5})`
        ctx.fill()
      })

      animId = requestAnimationFrame(animate)
    }

    animate()
    return () => {
      cancelAnimationFrame(animId)
      window.removeEventListener('resize', resize)
    }
  }, [])

  return <canvas ref={canvasRef} className="absolute inset-0 w-full h-full" />
}

// ── Avatar helpers ────────────────────────────────────────────────────────────
const AVATAR_COLORS = ['bg-blue-500','bg-purple-500','bg-green-500','bg-orange-500','bg-pink-500','bg-teal-500','bg-red-500','bg-indigo-500']
export function getAvatarColor(email=''){let h=0;for(let i=0;i<email.length;i++)h=email.charCodeAt(i)+((h<<5)-h);return AVATAR_COLORS[Math.abs(h)%AVATAR_COLORS.length]}
export function Avatar({email,name,size='md'}){const letter=(name||email||'?')[0].toUpperCase();const color=getAvatarColor(email);const sizes={sm:'h-7 w-7 text-xs',md:'h-9 w-9 text-sm',lg:'h-12 w-12 text-lg'};return(<div className={`${sizes[size]} ${color} rounded-full flex items-center justify-center text-white font-bold flex-shrink-0`}>{letter}</div>)}

// ── Main Login Page ───────────────────────────────────────────────────────────
export default function Login() {
  const { login, loading } = useAuth()
  const navigate = useNavigate()
  const [email,      setEmail]      = useState('')
  const [password,   setPassword]   = useState('')
  const [showPass,   setShowPass]   = useState(false)
  const [error,      setError]      = useState('')
  const [emailError, setEmailError] = useState('')
  const [showCreate, setShowCreate] = useState(false)

  function validateEmail(val) {
    if (val && !isEmailAllowed(val)) setEmailError('Only @kpmg.com email addresses are allowed')
    else setEmailError('')
  }
  async function handleSubmit(e) {
    e.preventDefault(); setError('')
    if (!isEmailAllowed(email)) { setEmailError('Only @kpmg.com email addresses are allowed'); return }
    const result = await login(email, password)
    if (result.ok) navigate('/today')
    else setError(result.error)
  }

  return (
    <div className="min-h-screen relative overflow-y-auto flex flex-col" style={{background:'#F7F9FF'}}>
      <WorldMapCanvas />

      {/* ── Top nav bar ──────────────────────────────────────────────────── */}
      <header className="relative z-10 bg-white/80 backdrop-blur-md border-b border-gray-100 sticky top-0">
        <div className="max-w-7xl mx-auto px-6 h-14 flex items-center justify-between">
          {/* Logo */}
          <div className="flex items-center gap-3">
            <img src="/kpmg-logo-blue.png" alt="KPMG" className="h-6 object-contain"
              onError={e=>{e.target.style.display='none';e.target.nextSibling.style.display='block'}}/>
            <span style={{display:'none'}} className="font-black text-kpmg-blue text-lg tracking-tighter">KPMG</span>
          </div>

          {/* Nav actions */}
          <div className="flex items-center gap-2">
            <button
              onClick={() => setShowCreate(false)}
              className="text-sm text-gray-600 hover:text-kpmg-blue px-3 py-1.5 rounded-lg
                         hover:bg-blue-50 transition-colors font-medium">
              Sign in
            </button>
            <button
              onClick={() => setShowCreate(true)}
              className="text-sm font-medium px-4 py-1.5 rounded-lg border border-kpmg-blue
                         text-kpmg-blue hover:bg-kpmg-blue hover:text-white transition-colors">
              Create account
            </button>
            <div className="flex items-center gap-1 ml-2 text-gray-400 text-sm cursor-pointer hover:text-gray-600">
              <Globe className="h-3.5 w-3.5" />
              <span className="text-xs">EN</span>
              <ChevronDown className="h-3 w-3" />
            </div>
          </div>
        </div>
      </header>

      {/* ── Hero section ─────────────────────────────────────────────────── */}
      <section className="relative z-10 flex-1 flex items-center justify-center min-h-[calc(100vh-56px)] px-6 py-16">
        <div className="w-full max-w-7xl flex items-center gap-16">

          {/* Left hero text */}
          <div className="hidden lg:flex flex-col flex-1 max-w-xl">
            <div className="flex items-center gap-2 mb-6">
              <span className="h-1.5 w-1.5 rounded-full bg-green-500 animate-pulse" />
              <span className="text-xs font-medium text-gray-500 tracking-wide">Live global data monitoring</span>
            </div>
            <h1 className="text-4xl font-bold text-gray-900 leading-snug mb-4 tracking-tight">
              Global Tender<br />
              <span className="text-kpmg-blue">Intelligence Platform</span>
            </h1>
            <p className="text-gray-500 text-base leading-relaxed mb-8 max-w-md">
              Supporting KPMG professionals with real-time tender intelligence
              to identify and act on global opportunities with confidence.
            </p>

            {/* Portal tags */}
            <div className="flex flex-wrap gap-2 mb-10">
              {[
                {l:'AfDB',bg:'bg-blue-50',t:'text-blue-700',b:'border-blue-100'},
                {l:'World Bank',bg:'bg-indigo-50',t:'text-indigo-700',b:'border-indigo-100'},
                {l:'UNDP',bg:'bg-teal-50',t:'text-teal-700',b:'border-teal-100'},
                {l:'UNGM',bg:'bg-purple-50',t:'text-purple-700',b:'border-purple-100'},
              ].map(p=>(
                <span key={p.l} className={`${p.bg} ${p.t} border ${p.b} text-xs font-medium px-3 py-1.5 rounded-full`}>
                  {p.l}
                </span>
              ))}
            </div>

            {/* Stats row */}
            <div className="flex gap-8 pt-8 border-t border-gray-100">
              {[
                {v:'4',    l:'Portals monitored'},
                {v:'Daily',l:'Automated scoring'},
                {v:'AI',   l:'Powered insights'},
              ].map(s=>(
                <div key={s.l}>
                  <div className="text-xl font-bold text-kpmg-blue">{s.v}</div>
                  <div className="text-xs text-gray-400 mt-0.5">{s.l}</div>
                </div>
              ))}
            </div>
          </div>

          {/* Right — floating login card */}
          <div className="w-full max-w-[360px] flex-shrink-0 mx-auto lg:mx-0">
            <div className="bg-white/90 backdrop-blur-sm rounded-2xl border border-gray-100 p-7"
                 style={{boxShadow:'0 20px 60px rgba(0,51,141,0.08),0 4px 16px rgba(0,0,0,0.05)'}}>

              {!showCreate ? (
                <>
                  <div className="mb-6">
                    <h2 className="text-lg font-bold text-gray-900">Welcome back</h2>
                    <p className="text-gray-400 text-sm mt-0.5">Sign in to your KPMG account</p>
                  </div>
                  <form onSubmit={handleSubmit} className="space-y-4">
                    <div>
                      <label className="block text-xs font-semibold text-gray-600 mb-1.5 uppercase tracking-wide">
                        Email
                      </label>
                      <input type="email" value={email}
                        onChange={e=>{setEmail(e.target.value);validateEmail(e.target.value)}}
                        onBlur={e=>validateEmail(e.target.value)}
                        required autoFocus placeholder="firstname.lastname@kpmg.com"
                        className={`block w-full rounded-xl border px-3.5 py-2.5 text-sm
                          placeholder-gray-300 outline-none transition-all focus:ring-2
                          ${emailError
                            ? 'border-red-300 focus:border-red-400 focus:ring-red-50 bg-red-50'
                            : 'border-gray-200 focus:border-kpmg-blue focus:ring-blue-50 bg-white'}`}
                      />
                      {emailError && (
                        <p className="mt-1 text-xs text-red-500 flex items-center gap-1">
                          <AlertCircle className="h-3 w-3 flex-shrink-0"/>{emailError}
                        </p>
                      )}
                    </div>
                    <div>
                      <div className="flex items-center justify-between mb-1.5">
                        <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wide">
                          Password
                        </label>
                        <a href="#" className="text-xs text-kpmg-blue hover:underline">Forgot?</a>
                      </div>
                      <div className="relative">
                        <input type={showPass?'text':'password'} value={password}
                          onChange={e=>setPassword(e.target.value)} required placeholder="••••••••"
                          className="block w-full rounded-xl border border-gray-200 px-3.5 py-2.5 pr-10
                                     text-sm placeholder-gray-300 bg-white outline-none transition-all
                                     focus:ring-2 focus:border-kpmg-blue focus:ring-blue-50"/>
                        <button type="button" onClick={()=>setShowPass(!showPass)}
                          className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600">
                          {showPass?<EyeOff className="h-4 w-4"/>:<Eye className="h-4 w-4"/>}
                        </button>
                      </div>
                    </div>
                    {error && (
                      <div className="flex items-center gap-2 text-red-600 bg-red-50 border border-red-200 rounded-xl px-3.5 py-2.5 text-sm">
                        <AlertCircle className="h-4 w-4 flex-shrink-0"/>{error}
                      </div>
                    )}
                    <button type="submit" disabled={loading||!!emailError}
                      className="w-full rounded-xl py-2.5 text-sm font-semibold text-white mt-1
                                 transition-all duration-150 disabled:opacity-50 disabled:cursor-not-allowed
                                 flex items-center justify-center gap-2"
                      style={{background:'linear-gradient(135deg,#005EB8 0%,#00338D 100%)',boxShadow:'0 4px 14px rgba(0,51,141,0.3)'}}>
                      {loading?(<><div className="h-4 w-4 animate-spin rounded-full border-2 border-white/30 border-t-white"/>Signing in…</>):'Sign in'}
                    </button>
                  </form>
                  <div className="mt-5 pt-4 border-t border-gray-50 space-y-3">
                    <p className="text-center text-sm text-gray-400">
                      No account?{' '}
                      <button onClick={()=>setShowCreate(true)} className="text-kpmg-blue font-medium hover:underline">
                        Request access
                      </button>
                    </p>
                    <div className="flex items-center justify-center gap-1.5">
                      <svg className="h-3 w-3 text-gray-300" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M5 9V7a5 5 0 0110 0v2a2 2 0 012 2v5a2 2 0 01-2 2H5a2 2 0 01-2-2v-5a2 2 0 012-2zm8-2v2H7V7a3 3 0 016 0z" clipRule="evenodd"/>
                      </svg>
                      <span className="text-xs text-gray-300">Access restricted to authorized KPMG personnel</span>
                    </div>
                  </div>
                </>
              ) : (
                <>
                  <div className="mb-6">
                    <h2 className="text-lg font-bold text-gray-900">Request access</h2>
                    <p className="text-gray-400 text-sm mt-0.5">Contact your KPMG administrator</p>
                  </div>
                  <div className="bg-blue-50 border border-blue-100 rounded-xl p-4 mb-4">
                    <p className="text-sm text-blue-700 leading-relaxed">
                      This platform is restricted to authorized KPMG personnel.
                      To request an account, please contact your team administrator
                      or send an email to <span className="font-semibold">it-support@kpmg.com</span>
                    </p>
                  </div>
                  <button onClick={()=>setShowCreate(false)}
                    className="w-full rounded-xl py-2.5 text-sm font-semibold text-kpmg-blue border
                               border-kpmg-blue hover:bg-kpmg-blue hover:text-white transition-colors">
                    ← Back to sign in
                  </button>
                </>
              )}
            </div>
          </div>
        </div>
      </section>

      {/* ── Scrollable content ────────────────────────────────────────────── */}
      <section className="relative z-10 bg-white border-t border-gray-100 py-16 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-2xl font-bold text-gray-900 mb-3">A unified approach to tender intelligence</h2>
            <p className="text-gray-500 max-w-lg mx-auto">Bringing together global data, AI insights, and expert analysis.</p>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
            {[
              {icon:'🎯', title:'Identify Opportunities',   desc:'Continuous monitoring across all major development portals.'},
              {icon:'📊', title:'Evaluate Strategic Fit',   desc:'AI-powered scoring aligned with KPMG sector expertise.'},
              {icon:'🌍', title:'Global Coverage',          desc:'Supporting teams worldwide with continuous monitoring.'},
              {icon:'🔒', title:'Enterprise Security',      desc:'Access restricted to authorized KPMG personnel only.'},
            ].map(c=>(
              <div key={c.title} className="bg-gray-50 rounded-2xl p-6 border border-gray-100">
                <div className="text-2xl mb-3">{c.icon}</div>
                <div className="font-semibold text-gray-900 text-sm mb-2">{c.title}</div>
                <div className="text-gray-400 text-xs leading-relaxed">{c.desc}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Footer ───────────────────────────────────────────────────────── */}
      <footer className="relative z-10 bg-white border-t border-gray-100 px-6 py-4">
        <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-2">
          <div className="flex items-center gap-3 text-gray-400 text-xs">
            <img src="/kpmg-logo-blue.png" alt="KPMG" className="h-4 object-contain opacity-40"
              onError={e=>e.target.style.display='none'}/>
            <span>KPMG Tunisia</span>
            <span>·</span>
            <span>© {new Date().getFullYear()} KPMG International</span>
            <span>·</span>
            <span className="text-gray-300">Platform v1.0</span>
          </div>
          <div className="flex gap-4">
            {['Privacy Policy','Terms','Contact'].map(l=>(
              <a key={l} href="#" className="text-gray-400 text-xs hover:text-gray-600 transition-colors">{l}</a>
            ))}
          </div>
        </div>
      </footer>
    </div>
  )
}