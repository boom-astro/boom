import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import api from '@/lib/api';
import LandingSky from '@/components/LandingSky';

export default function Landing() {
  const token = api.getTokenRecord();
  const loggedIn = !!token;
  const BG_KEY = 'landing_bg_mode';
  const modes = ['sky', 'blob', 'none'] as const;
  type Mode = typeof modes[number];
  const [bgMode, setBgMode] = useState<Mode>(() => {
    try {
      const v = localStorage.getItem(BG_KEY);
      if (v && modes.includes(v as Mode)) return v as Mode;
    } catch {}
    return 'blob';
  });

  useEffect(() => {
    try {
      localStorage.setItem(BG_KEY, bgMode);
    } catch {}
  }, [bgMode]);

  return (
    <div className="w-full min-h-screen flex items-center justify-center bg-gradient-to-b from-slate-950 to-slate-900 text-white">
      <style>{`
        .hero-blob { animation: blob 18s infinite; opacity: 0.18 }
        @keyframes blob {
          0% { transform: translate(0px,0px) scale(1); }
          33% { transform: translate(30px,-20px) scale(1.05); }
          66% { transform: translate(-20px,20px) scale(0.95); }
          100% { transform: translate(0px,0px) scale(1); }
        }
        .fade-in-up { transform: translateY(8px); opacity: 0; animation: fadeUp .6s ease-out .12s forwards; }
        @keyframes fadeUp { to { transform: translateY(0); opacity: 1; } }
      `}</style>

      {/* Background variants rendered as full-viewport fixed layers */}
      {bgMode === 'sky' && (
        <div className="fixed inset-0 pointer-events-none z-0">
          <LandingSky />
        </div>
      )}
      {bgMode === 'blob' && (
        <div className="fixed inset-0 flex items-center justify-center pointer-events-none z-0">
          <svg className="hero-blob w-[60%] h-[60%]" viewBox="0 0 600 600" xmlns="http://www.w3.org/2000/svg">
            <g transform="translate(300,300)">
              <path d="M120,-172C159,-146,196,-111,210,-70C224,-29,215,18,193,59C171,100,135,135,92,165C49,195,-2,220,-42,205C-82,191,-113,137,-154,96C-196,55,-248,26,-259,-17C-270,-60,-241,-116,-203,-150C-164,-184,-116,-197,-67,-209C-18,-221,31,-233,72,-217C113,-201,82,-198,120,-172Z" fill="#ffb86b" />
            </g>
          </svg>
        </div>
      )}

      <div className="relative max-w-6xl w-full px-6 lg:px-12 py-12 md:py-20">

        <div className="relative z-10 max-w-3xl mx-auto text-center">
          {/* <h1 className="text-4xl md:text-5xl font-extrabold leading-tight tracking-tight mb-4 fade-in-up">BabamulA real-time & multi-survey alert broker</h1> */}
          {/*  let's break this down into 2 lines */}
            <h1 className="text-5xl md:text-8xl font-extrabold leading-tight tracking-tight mb-4 fade-in-up">Babamul</h1>
            <h2 className="text-3xl md:text-5xl font-semibold leading-tight tracking-tight mb-6 fade-in-up">A real-time multi-survey alert broker for the LSST era.</h2>
          <p className="text-lg md:text-xl text-slate-200 mb-8 fade-in-up">Ingest, filter and stream optical transient alerts from ZTF and LSST at scale. Subscribe with Kafka, query with a flexible API, and build real-time alerting systems.</p>

          <div className="flex flex-col sm:flex-row items-center justify-center gap-3 sm:gap-4 fade-in-up">
            <Link to="/query">
              <Button className="px-6 py-3 hover:from-amber-600 hover:to-orange-500 text-slate-900 shadow-md hover:shadow-xl transform transition duration-150 ease-out hover:-translate-y-0.5">Search for alerts</Button>
            </Link>
            <Link to="/babamul/docs">
              <Button variant="outline" className="px-6 py-3 transform transition duration-150 ease-out hover:-translate-y-0.5 hover:shadow-md">Kafka docs</Button>
            </Link>
            <Link to="/api-docs">
              <Button variant="outline" className="px-6 py-3 transform transition duration-150 ease-out hover:-translate-y-0.5 hover:shadow-md">API docs</Button>
            </Link>
          </div>

          <div className="mt-10 grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="p-4 bg-slate-800/30 rounded-lg border border-slate-700">
              <h3 className="font-semibold mb-2">Realtime</h3>
              <p className="text-sm text-slate-300">Low-latency streams via Kafka — connect consumers and react to alerts instantly.</p>
            </div>
            <div className="p-4 bg-slate-800/30 rounded-lg border border-slate-700">
              <h3 className="font-semibold mb-2">Multi-survey</h3>
              <p className="text-sm text-slate-300">Native support for ZTF and LSST alerts with band-aware photometry and crossmatches.</p>
            </div>
            <div className="p-4 bg-slate-800/30 rounded-lg border border-slate-700">
              <h3 className="font-semibold mb-2">At Scale</h3>
              <p className="text-sm text-slate-300">Designed for high throughput and efficient filtering at observatory scale.</p>
            </div>
          </div>

          {loggedIn ? (
            <p className="mt-8 text-sm text-slate-400">You're signed in — try running a query on the <Link to="/query" className="underline">Query page</Link>.</p>
          ) : (
            <p className="mt-8 text-sm text-slate-400">Want to get started quickly? <Link to="/login" className="underline">Log in</Link> or <Link to="/signup" className="underline">create an account</Link>.</p>
          )}
        </div>
        {/* background toggle - sticky top-right */}
        <div className="fixed top-4 right-4 z-50">
          <div className="flex items-center gap-2">
            <button
              title="Cycle background"
              aria-label="Cycle landing background"
              onClick={() => {
                const idx = modes.indexOf(bgMode);
                const next = modes[(idx + 1) % modes.length];
                setBgMode(next);
              }}
              className="bg-slate-800/60 text-slate-100 hover:bg-slate-800/80 backdrop-blur-sm rounded-md px-3 py-2 shadow-md border border-slate-700"
            >
              {bgMode === 'sky' ? 'Sky' : bgMode === 'blob' ? 'Blob' : 'None'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
