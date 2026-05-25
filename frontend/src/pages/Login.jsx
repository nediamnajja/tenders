// import { useState, useEffect, useRef } from 'react'
// import { useNavigate } from 'react-router-dom'
// import { useAuth } from '../lib/auth'
// import { Eye, EyeOff, AlertCircle, Globe, ChevronDown } from 'lucide-react'

// const ALLOWED_EMAILS = ['nediamnajja.tbs@gmail.com']
// function isEmailAllowed(email) {
//   const lower = email.toLowerCase().trim()
//   return lower.endsWith('@kpmg.com') || ALLOWED_EMAILS.includes(lower)
// }

// function WorldMapCanvas() {
//   const canvasRef = useRef(null)
//   useEffect(() => {
//     const canvas = canvasRef.current
//     const ctx = canvas.getContext('2d')
//     let animId, maskData = null, maskReady = false
//     const MASK_W = 1000, MASK_H = 500
//     const resize = () => { canvas.width = canvas.offsetWidth; canvas.height = canvas.offsetHeight }
//     resize()
//     window.addEventListener('resize', resize)
//     const img = new Image()
//     img.crossOrigin = 'anonymous'
//     img.src = '/world-map-mask.png'
//     img.onload = () => {
//       const off = document.createElement('canvas')
//       off.width = MASK_W; off.height = MASK_H
//       const oc = off.getContext('2d')
//       oc.drawImage(img, 0, 0, MASK_W, MASK_H)
//       maskData = oc.getImageData(0, 0, MASK_W, MASK_H)
//       maskReady = true
//     }
//     img.onerror = () => { maskReady = true }
//     function isLand(sx, sy, w, h) {
//       if (!maskData) return false
//       const mx = Math.round((sx / w) * MASK_W)
//       const my = Math.round((sy / h) * MASK_H)
//       if (mx < 0 || mx >= MASK_W || my < 0 || my >= MASK_H) return false
//       return maskData.data[(my * MASK_W + mx) * 4] > 128
//     }
//     const HOTSPOTS = [
//       { lng:3.4,   lat:6.5  }, { lng:36.8,  lat:-1.3  }, { lng:13.5, lat:12.4 },
//       { lng:38.7,  lat:9.0  }, { lng:29.4,  lat:-3.4  }, { lng:20.5, lat:-4.3 },
//       { lng:-4.0,  lat:5.4  }, { lng:18.6,  lat:4.4   }, { lng:7.5,  lat:9.1  },
//       { lng:-8.0,  lat:11.9 }, { lng:2.1,   lat:13.5  }, { lng:15.0, lat:-4.0 },
//       { lng:32.5,  lat:15.6 }, { lng:28.0,  lat:-26.2 }, { lng:31.2, lat:30.1 },
//       { lng:-16.6, lat:13.5 }, { lng:34.8,  lat:-6.0  }, { lng:-1.7, lat:12.4 },
//       { lng:9.0,   lat:4.0  }, { lng:23.0,  lat:-3.0  },
//       { lng:44.4,  lat:33.3 }, { lng:35.9,  lat:31.9  }, { lng:51.4, lat:25.3 },
//       { lng:2.3,   lat:48.9 }, { lng:13.4,  lat:52.5  }, { lng:12.5, lat:41.9 },
//       { lng:77.2,  lat:28.6 },
//     ]
//     const spots = HOTSPOTS.map((h, i) => ({
//       ...h,
//       phase: (i / HOTSPOTS.length) * Math.PI * 2 + Math.random() * 1.5,
//       speed: 0.003 + Math.random() * 0.006,
//     }))
//     function lngLatToXY(lng, lat, w, h) {
//       return { x: ((lng + 180) / 360) * w, y: ((90 - lat) / 180) * h }
//     }
//     function animate() {
//       const w = canvas.width, h = canvas.height
//       ctx.clearRect(0, 0, w, h)
//       if (maskReady) {
//         ctx.fillStyle = 'rgba(160, 185, 220, 0.18)'
//         for (let px = 0; px < w; px += 5)
//           for (let py = 0; py < h; py += 5)
//             if (isLand(px, py, w, h)) { ctx.beginPath(); ctx.arc(px, py, 1.5, 0, Math.PI * 2); ctx.fill() }
//       }
//       spots.forEach(s => {
//         s.phase += s.speed
//         const alpha = Math.max(0, Math.sin(s.phase))
//         if (alpha < 0.02) return
//         const { x, y } = lngLatToXY(s.lng, s.lat, w, h)
//         ctx.beginPath(); ctx.arc(x, y, 5 + alpha * 3, 0, Math.PI * 2)
//         ctx.strokeStyle = `rgba(100, 150, 210, ${alpha * 0.22})`
//         ctx.lineWidth = 0.8; ctx.stroke()
//         ctx.beginPath(); ctx.arc(x, y, 2, 0, Math.PI * 2)
//         ctx.fillStyle = `rgba(80, 130, 200, ${0.25 + alpha * 0.30})`
//         ctx.fill()
//       })
//       animId = requestAnimationFrame(animate)
//     }
//     animate()
//     return () => { cancelAnimationFrame(animId); window.removeEventListener('resize', resize) }
//   }, [])
//   return <canvas ref={canvasRef} className="absolute inset-0 w-full h-full" />
// }

// const AVATAR_COLORS = ['bg-blue-500','bg-purple-500','bg-green-500','bg-orange-500','bg-pink-500','bg-teal-500','bg-red-500','bg-indigo-500']
// export function getAvatarColor(email=''){let h=0;for(let i=0;i<email.length;i++)h=email.charCodeAt(i)+((h<<5)-h);return AVATAR_COLORS[Math.abs(h)%AVATAR_COLORS.length]}
// export function Avatar({email,name,size='md'}){const letter=(name||email||'?')[0].toUpperCase();const color=getAvatarColor(email);const sizes={sm:'h-7 w-7 text-xs',md:'h-9 w-9 text-sm',lg:'h-12 w-12 text-lg'};return(<div className={`${sizes[size]} ${color} rounded-full flex items-center justify-center text-white font-bold flex-shrink-0`}>{letter}</div>)}

// export default function Login() {
//   const { login, loading } = useAuth()
//   const navigate = useNavigate()
//   const [email,      setEmail]      = useState('')
//   const [password,   setPassword]   = useState('')
//   const [showPass,   setShowPass]   = useState(false)
//   const [error,      setError]      = useState('')
//   const [emailError, setEmailError] = useState('')
//   const [showCreate, setShowCreate] = useState(false)

//   function validateEmail(val) {
//     if (val && !isEmailAllowed(val)) setEmailError('Only @kpmg.com email addresses are allowed')
//     else setEmailError('')
//   }
//   async function handleSubmit(e) {
//     e.preventDefault(); setError('')
//     if (!isEmailAllowed(email)) { setEmailError('Only @kpmg.com email addresses are allowed'); return }
//     const result = await login(email, password)
//     if (result.ok) navigate('/today')
//     else setError(result.error)
//   }

//   return (
//     <div className="min-h-screen relative flex flex-col" style={{background:'#F7F9FF'}}>
//       <WorldMapCanvas />

//       <header className="relative z-10 bg-white/80 backdrop-blur-md border-b border-gray-100 sticky top-0">
//         <div className="max-w-7xl mx-auto px-6 h-14 flex items-center justify-between">
//           <div className="flex items-center gap-3">
//             <img src="/kpmg-logo-blue.svg" alt="KPMG" className="h-6 object-contain mix-blend-multiply"
//               onError={e=>{e.target.style.display='none';e.target.nextSibling.style.display='block'}}/>
//             <span style={{display:'none'}} className="font-black text-kpmg-blue text-lg tracking-tighter">KPMG</span>
//           </div>
//           <div className="flex items-center gap-2">
//             <button onClick={() => setShowCreate(false)}
//               className="text-sm text-gray-600 hover:text-kpmg-blue px-3 py-1.5 rounded-lg hover:bg-blue-50 transition-colors font-medium">
//               Sign in
//             </button>
//             <button onClick={() => setShowCreate(true)}
//               className="text-sm font-medium px-4 py-1.5 rounded-lg border border-kpmg-blue text-kpmg-blue hover:bg-kpmg-blue hover:text-white transition-colors">
//               Create account
//             </button>
//             <div className="flex items-center gap-1 ml-2 text-gray-400 text-sm cursor-pointer hover:text-gray-600">
//               <Globe className="h-3.5 w-3.5" /><span className="text-xs">EN</span><ChevronDown className="h-3 w-3" />
//             </div>
//           </div>
//         </div>
//       </header>

//       <section className="relative z-10 flex-1 flex items-center justify-center px-6 py-10">
//         <div className="w-full max-w-7xl flex items-center gap-16">

//           <div className="hidden lg:flex flex-col flex-1 max-w-xl relative">
//             {/* Soft white gradient so text stays crisp over dots */}
//             <div className="absolute inset-0 -left-8 -right-4 rounded-2xl pointer-events-none"
//                  style={{background:'linear-gradient(to right, rgba(247,249,255,0.96) 65%, rgba(247,249,255,0))'}} />
//             <div className="relative z-10">
//               <div className="flex items-center gap-2 mb-5">
//                 <span className="h-1.5 w-1.5 rounded-full bg-green-500 animate-pulse" />
//                 <span className="text-xs font-medium text-gray-500 tracking-wide">Live global data monitoring</span>
//               </div>
//               <h1 className="text-3xl font-bold text-gray-900 leading-snug mb-4 tracking-tight">
//                 Global Tender<br />
//                 <span className="text-kpmg-blue">Intelligence Platform</span>
//               </h1>
//               <p className="text-gray-500 text-sm leading-relaxed mb-6 max-w-md">
//                 Supporting KPMG professionals with real-time insights
//                 to identify                 and act on global opportunities with confidence.
//               </p>
//               <div className="flex gap-8 pt-6 border-t border-gray-100">
//                 {[{v:'500+',l:'Tenders scored'},{v:'Daily',l:'Fresh opportunities'},{v:'AI',l:'Bid/no-bid engine'}].map(s=>(
//                   <div key={s.l}>
//                     <div className="text-lg font-bold text-kpmg-blue">{s.v}</div>
//                     <div className="text-xs text-gray-400 mt-0.5">{s.l}</div>
//                   </div>
//                 ))}
//               </div>
//             </div>
//           </div>

//           <div className="w-full max-w-[360px] flex-shrink-0 mx-auto lg:mx-0">
//             <div className="bg-white/95 backdrop-blur-sm rounded-2xl border border-gray-100 p-7"
//                  style={{boxShadow:'0 20px 60px rgba(0,51,141,0.08),0 4px 16px rgba(0,0,0,0.05)'}}>
//               {!showCreate ? (
//                 <>
//                   <div className="mb-6">
//                     <h2 className="text-lg font-bold text-gray-900">Welcome back</h2>
//                     <p className="text-gray-400 text-sm mt-0.5">Sign in to your KPMG account</p>
//                   </div>
//                   <form onSubmit={handleSubmit} className="space-y-4">
//                     <div>
//                       <label className="block text-xs font-semibold text-gray-600 mb-1.5 uppercase tracking-wide">Email</label>
//                       <input type="email" value={email}
//                         onChange={e=>{setEmail(e.target.value);validateEmail(e.target.value)}}
//                         onBlur={e=>validateEmail(e.target.value)}
//                         required autoFocus placeholder="firstname.lastname@kpmg.com"
//                         className={`block w-full rounded-xl border px-3.5 py-2.5 text-sm placeholder-gray-300 outline-none transition-all focus:ring-2 ${emailError ? 'border-red-300 focus:border-red-400 focus:ring-red-50 bg-red-50' : 'border-gray-200 focus:border-kpmg-blue focus:ring-blue-50 bg-white'}`}
//                       />
//                       {emailError && <p className="mt-1 text-xs text-red-500 flex items-center gap-1"><AlertCircle className="h-3 w-3 flex-shrink-0"/>{emailError}</p>}
//                     </div>
//                     <div>
//                       <div className="flex items-center justify-between mb-1.5">
//                         <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wide">Password</label>
//                         <a href="#" className="text-xs text-kpmg-blue hover:underline">Forgot?</a>
//                       </div>
//                       <div className="relative">
//                         <input type={showPass?'text':'password'} value={password}
//                           onChange={e=>setPassword(e.target.value)} required placeholder="••••••••"
//                           className="block w-full rounded-xl border border-gray-200 px-3.5 py-2.5 pr-10 text-sm placeholder-gray-300 bg-white outline-none transition-all focus:ring-2 focus:border-kpmg-blue focus:ring-blue-50"/>
//                         <button type="button" onClick={()=>setShowPass(!showPass)}
//                           className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600">
//                           {showPass?<EyeOff className="h-4 w-4"/>:<Eye className="h-4 w-4"/>}
//                         </button>
//                       </div>
//                     </div>
//                     {error && <div className="flex items-center gap-2 text-red-600 bg-red-50 border border-red-200 rounded-xl px-3.5 py-2.5 text-sm"><AlertCircle className="h-4 w-4 flex-shrink-0"/>{error}</div>}
//                     <button type="submit" disabled={loading||!!emailError}
//                       className="w-full rounded-xl py-2.5 text-sm font-semibold text-white mt-1 transition-all duration-150 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
//                       style={{background:'linear-gradient(135deg,#005EB8 0%,#00338D 100%)',boxShadow:'0 4px 14px rgba(0,51,141,0.3)'}}>
//                       {loading?(<><div className="h-4 w-4 animate-spin rounded-full border-2 border-white/30 border-t-white"/>Signing in…</>):'Sign in'}
//                     </button>
//                   </form>
//                   <div className="mt-5 pt-4 border-t border-gray-50 space-y-3">
//                     <p className="text-center text-sm text-gray-400">No account?{' '}<button onClick={()=>setShowCreate(true)} className="text-kpmg-blue font-medium hover:underline">Request access</button></p>
//                     <div className="flex items-center justify-center gap-1.5">
//                       <svg className="h-3 w-3 text-gray-300" fill="currentColor" viewBox="0 0 20 20"><path fillRule="evenodd" d="M5 9V7a5 5 0 0110 0v2a2 2 0 012 2v5a2 2 0 01-2 2H5a2 2 0 01-2-2v-5a2 2 0 012-2zm8-2v2H7V7a3 3 0 016 0z" clipRule="evenodd"/></svg>
//                       <span className="text-xs text-gray-300">Access restricted to authorized KPMG personnel</span>
//                     </div>
//                   </div>
//                 </>
//               ) : (
//                 <>
//                   <div className="mb-6">
//                     <h2 className="text-lg font-bold text-gray-900">Request access</h2>
//                     <p className="text-gray-400 text-sm mt-0.5">Contact your KPMG administrator</p>
//                   </div>
//                   <div className="bg-blue-50 border border-blue-100 rounded-xl p-4 mb-4">
//                     <p className="text-sm text-blue-700 leading-relaxed">
//                       This platform is restricted to authorized KPMG personnel.
//                       Contact your team administrator or email <span className="font-semibold">it-support@kpmg.com</span>
//                     </p>
//                   </div>
//                   <button onClick={()=>setShowCreate(false)}
//                     className="w-full rounded-xl py-2.5 text-sm font-semibold text-kpmg-blue border border-kpmg-blue hover:bg-kpmg-blue hover:text-white transition-colors">
//                     ← Back to sign in
//                   </button>
//                 </>
//               )}
//             </div>
//           </div>
//         </div>
//       </section>

//       <footer className="relative z-10 bg-white/60 backdrop-blur-sm border-t border-gray-100 px-6 py-3">
//         <div className="max-w-7xl mx-auto flex items-center justify-between">
//           <p className="text-gray-400 text-xs">© {new Date().getFullYear()} KPMG International — Confidential</p>
//           <div className="flex gap-4">
//             {['Privacy','Terms','Contact'].map(l=>(
//               <a key={l} href="#" className="text-gray-400 text-xs hover:text-gray-600 transition-colors">{l}</a>
//             ))}
//           </div>
//         </div>
//       </footer>
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

function WorldMapCanvas() {
  const canvasRef = useRef(null)
  useEffect(() => {
    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')
    let animId, maskData = null, maskReady = false
    const MASK_W = 1000, MASK_H = 500
    const resize = () => { canvas.width = canvas.offsetWidth; canvas.height = canvas.offsetHeight }
    resize()
    window.addEventListener('resize', resize)
    const img = new Image()
    img.crossOrigin = 'anonymous'
    img.src = '/world-map-mask.png'
    img.onload = () => {
      const off = document.createElement('canvas')
      off.width = MASK_W; off.height = MASK_H
      const oc = off.getContext('2d')
      oc.drawImage(img, 0, 0, MASK_W, MASK_H)
      maskData = oc.getImageData(0, 0, MASK_W, MASK_H)
      maskReady = true
    }
    img.onerror = () => { maskReady = true }
    function isLand(sx, sy, w, h) {
      if (!maskData) return false
      const mx = Math.round((sx / w) * MASK_W)
      const my = Math.round((sy / h) * MASK_H)
      if (mx < 0 || mx >= MASK_W || my < 0 || my >= MASK_H) return false
      return maskData.data[(my * MASK_W + mx) * 4] > 128
    }
    const HOTSPOTS = [
      { lng:3.4,   lat:6.5  }, { lng:36.8,  lat:-1.3  }, { lng:13.5, lat:12.4 },
      { lng:38.7,  lat:9.0  }, { lng:29.4,  lat:-3.4  }, { lng:20.5, lat:-4.3 },
      { lng:-4.0,  lat:5.4  }, { lng:18.6,  lat:4.4   }, { lng:7.5,  lat:9.1  },
      { lng:-8.0,  lat:11.9 }, { lng:2.1,   lat:13.5  }, { lng:15.0, lat:-4.0 },
      { lng:32.5,  lat:15.6 }, { lng:28.0,  lat:-26.2 }, { lng:31.2, lat:30.1 },
      { lng:-16.6, lat:13.5 }, { lng:34.8,  lat:-6.0  }, { lng:-1.7, lat:12.4 },
      { lng:9.0,   lat:4.0  }, { lng:23.0,  lat:-3.0  },
      { lng:44.4,  lat:33.3 }, { lng:35.9,  lat:31.9  }, { lng:51.4, lat:25.3 },
      { lng:2.3,   lat:48.9 }, { lng:13.4,  lat:52.5  }, { lng:12.5, lat:41.9 },
      { lng:77.2,  lat:28.6 },
    ]
    const spots = HOTSPOTS.map((h, i) => ({
      ...h,
      phase: (i / HOTSPOTS.length) * Math.PI * 2 + Math.random() * 1.5,
      speed: 0.003 + Math.random() * 0.006,
    }))
    function lngLatToXY(lng, lat, w, h) {
      return { x: ((lng + 180) / 360) * w, y: ((90 - lat) / 180) * h }
    }
    function animate() {
      const w = canvas.width, h = canvas.height
      ctx.clearRect(0, 0, w, h)
      if (maskReady) {
        ctx.fillStyle = 'rgba(160, 185, 220, 0.18)'
        for (let px = 0; px < w; px += 5)
          for (let py = 0; py < h; py += 5)
            if (isLand(px, py, w, h)) { ctx.beginPath(); ctx.arc(px, py, 1.5, 0, Math.PI * 2); ctx.fill() }
      }
      spots.forEach(s => {
        s.phase += s.speed
        const alpha = Math.max(0, Math.sin(s.phase))
        if (alpha < 0.02) return
        const { x, y } = lngLatToXY(s.lng, s.lat, w, h)
        ctx.beginPath(); ctx.arc(x, y, 5 + alpha * 3, 0, Math.PI * 2)
        ctx.strokeStyle = `rgba(60, 110, 190, ${alpha * 0.35})`
        ctx.lineWidth = 0.8; ctx.stroke()
        ctx.beginPath(); ctx.arc(x, y, 2, 0, Math.PI * 2)
        ctx.fillStyle = `rgba(40, 100, 180, ${0.40 + alpha * 0.40})`
        ctx.fill()
      })
      animId = requestAnimationFrame(animate)
    }
    animate()
    return () => { cancelAnimationFrame(animId); window.removeEventListener('resize', resize) }
  }, [])
  return <canvas ref={canvasRef} className="absolute inset-0 w-full h-full" />
}

const AVATAR_COLORS = ['bg-blue-500','bg-purple-500','bg-green-500','bg-orange-500','bg-pink-500','bg-teal-500','bg-red-500','bg-indigo-500']
export function getAvatarColor(email=''){let h=0;for(let i=0;i<email.length;i++)h=email.charCodeAt(i)+((h<<5)-h);return AVATAR_COLORS[Math.abs(h)%AVATAR_COLORS.length]}
export function Avatar({email,name,size='md'}){const letter=(name||email||'?')[0].toUpperCase();const color=getAvatarColor(email);const sizes={sm:'h-7 w-7 text-xs',md:'h-9 w-9 text-sm',lg:'h-12 w-12 text-lg'};return(<div className={`${sizes[size]} ${color} rounded-full flex items-center justify-center text-white font-bold flex-shrink-0`}>{letter}</div>)}

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
    <div className="min-h-screen relative flex flex-col" style={{background:'#F7F9FF'}}>
      <WorldMapCanvas />

      <header className="relative z-10 bg-white/80 backdrop-blur-md border-b border-gray-100 sticky top-0">
        <div className="max-w-7xl mx-auto px-6 h-14 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <img src="/kpmg-logo-blue.svg" alt="KPMG" className="h-6 object-contain mix-blend-multiply"
              onError={e=>{e.target.style.display='none';e.target.nextSibling.style.display='block'}}/>
            <span style={{display:'none'}} className="font-black text-kpmg-blue text-lg tracking-tighter">KPMG</span>
          </div>
          <div className="flex items-center gap-2">
            <button onClick={() => setShowCreate(false)}
              className="text-sm text-gray-600 hover:text-kpmg-blue px-3 py-1.5 rounded-lg hover:bg-blue-50 transition-colors font-medium">
              Sign in
            </button>
            <button onClick={() => setShowCreate(true)}
              className="text-sm font-medium px-4 py-1.5 rounded-lg border border-kpmg-blue text-kpmg-blue hover:bg-kpmg-blue hover:text-white transition-colors">
              Create account
            </button>
            <div className="flex items-center gap-1 ml-2 text-gray-400">
              <Globe className="h-3.5 w-3.5" />
              <span className="text-xs font-medium">EN</span>
            </div>
          </div>
        </div>
      </header>

      <section className="relative z-10 flex-1 flex items-center justify-center px-6 py-10">
        <div className="w-full max-w-7xl flex items-center gap-16">

          <div className="hidden lg:flex flex-col flex-1 max-w-xl relative">
            {/* Soft white gradient so text stays crisp over dots */}
            <div className="absolute inset-0 -left-8 -right-4 rounded-2xl pointer-events-none"
                 style={{background:'linear-gradient(to right, rgba(247,249,255,0.96) 65%, rgba(247,249,255,0))'}} />
            <div className="relative z-10">
              <div className="flex items-center gap-2 mb-5">
                <span className="h-2 w-2 rounded-full bg-green-500 animate-pulse" style={{boxShadow:'0 0 6px #22c55e'}} />
                <span className="text-xs font-medium text-gray-500 tracking-wide">Live global data monitoring</span>
              </div>
              <h1 className="text-3xl font-bold text-gray-900 leading-snug mb-4 tracking-tight">
                Global Procurement<br />
                <span className="text-kpmg-blue">Intelligence Platform</span>
              </h1>
              <p className="text-gray-500 text-sm leading-relaxed mb-6 max-w-md">
                Supporting KPMG professionals with real-time insights<br />
                to identify and act on global opportunities with confidence.
              </p>
              <div className="flex gap-8 pt-6 border-t border-gray-100">
                {[{v:'500+',l:'Tenders scored'},{v:'Daily',l:'Fresh opportunities'},{v:'AI',l:'Opportunity Insights'}].map(s=>(
                  <div key={s.l}>
                    <div className="text-lg font-bold text-kpmg-blue">{s.v}</div>
                    <div className="text-xs text-gray-400 mt-0.5">{s.l}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="w-full max-w-[360px] flex-shrink-0 mx-auto lg:mx-0">
            <div className="bg-white/95 backdrop-blur-sm rounded-2xl border border-gray-100 p-7"
                 style={{boxShadow:'0 20px 60px rgba(0,51,141,0.08),0 4px 16px rgba(0,0,0,0.05)'}}>
              {!showCreate ? (
                <>
                  <div className="mb-6">
                    <h2 className="text-lg font-bold text-gray-900">Welcome back</h2>
                    <p className="text-gray-400 text-sm mt-0.5">Sign in to your KPMG account</p>
                  </div>
                  <form onSubmit={handleSubmit} className="space-y-4">
                    <div>
                      <label className="block text-xs font-semibold text-gray-600 mb-1.5 uppercase tracking-wide">Email</label>
                      <input type="email" value={email}
                        onChange={e=>{setEmail(e.target.value);validateEmail(e.target.value)}}
                        onBlur={e=>validateEmail(e.target.value)}
                        required autoFocus placeholder="firstname.lastname@kpmg.com"
                        className={`block w-full rounded-xl border px-3.5 py-2.5 text-sm placeholder-gray-300 outline-none transition-all focus:ring-2 ${emailError ? 'border-red-300 focus:border-red-400 focus:ring-red-50 bg-red-50' : 'border-gray-200 focus:border-kpmg-blue focus:ring-blue-50 bg-white'}`}
                      />
                      {emailError && <p className="mt-1 text-xs text-red-500 flex items-center gap-1"><AlertCircle className="h-3 w-3 flex-shrink-0"/>{emailError}</p>}
                    </div>
                    <div>
                      <div className="flex items-center justify-between mb-1.5">
                        <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wide">Password</label>
                        <a href="#" className="text-xs text-kpmg-blue hover:underline">Forgot?</a>
                      </div>
                      <div className="relative">
                        <input type={showPass?'text':'password'} value={password}
                          onChange={e=>setPassword(e.target.value)} required placeholder="••••••••"
                          className="block w-full rounded-xl border border-gray-200 px-3.5 py-2.5 pr-10 text-sm placeholder-gray-300 bg-white outline-none transition-all focus:ring-2 focus:border-kpmg-blue focus:ring-blue-50"/>
                        <button type="button" onClick={()=>setShowPass(!showPass)}
                          className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600">
                          {showPass?<EyeOff className="h-4 w-4"/>:<Eye className="h-4 w-4"/>}
                        </button>
                      </div>
                    </div>
                    {error && <div className="flex items-center gap-2 text-red-600 bg-red-50 border border-red-200 rounded-xl px-3.5 py-2.5 text-sm"><AlertCircle className="h-4 w-4 flex-shrink-0"/>{error}</div>}
                    <button type="submit" disabled={loading||!!emailError}
                      className="w-full rounded-xl py-2.5 text-sm font-semibold text-white mt-1 transition-all duration-150 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                      style={{background:'linear-gradient(135deg,#005EB8 0%,#00338D 100%)',boxShadow:'0 4px 14px rgba(0,51,141,0.3)'}}>
                      {loading?(<><div className="h-4 w-4 animate-spin rounded-full border-2 border-white/30 border-t-white"/>Signing in…</>):'Sign in'}
                    </button>
                  </form>
                  <div className="mt-5 pt-4 border-t border-gray-50 space-y-3">
                    <p className="text-center text-sm text-gray-400">No account?{' '}<button onClick={()=>setShowCreate(true)} className="text-kpmg-blue font-medium hover:underline">Request access</button></p>
                    <div className="flex items-center justify-center gap-1.5">
                      <svg className="h-3 w-3 text-gray-300" fill="currentColor" viewBox="0 0 20 20"><path fillRule="evenodd" d="M5 9V7a5 5 0 0110 0v2a2 2 0 012 2v5a2 2 0 01-2 2H5a2 2 0 01-2-2v-5a2 2 0 012-2zm8-2v2H7V7a3 3 0 016 0z" clipRule="evenodd"/></svg>
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
                      Contact your team administrator or email <span className="font-semibold">it-support@kpmg.com</span>
                    </p>
                  </div>
                  <button onClick={()=>setShowCreate(false)}
                    className="w-full rounded-xl py-2.5 text-sm font-semibold text-kpmg-blue border border-kpmg-blue hover:bg-kpmg-blue hover:text-white transition-colors">
                    ← Back to sign in
                  </button>
                </>
              )}
            </div>
          </div>
        </div>
      </section>

      <footer className="relative z-10 bg-white/60 backdrop-blur-sm border-t border-gray-100 px-6 py-3">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <p className="text-gray-400 text-xs">© {new Date().getFullYear()} KPMG International — Confidential</p>
          <div className="flex gap-4">
            {['Privacy','Terms','Contact'].map(l=>(
              <a key={l} href="#" className="text-gray-400 text-xs hover:text-gray-600 transition-colors">{l}</a>
            ))}
          </div>
        </div>
      </footer>
    </div>
  )
}