import { Link } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import api from '@/lib/api';
import Kilonova from '@/components/Kilonova';
import { IconNews } from '@tabler/icons-react';
import { toast } from 'sonner';

// Release mode flag - set VITE_PRERELEASE_MODE=true at build time to restrict app to landing page only
const PRERELEASE_MODE = import.meta.env.VITE_PRERELEASE_MODE === 'true';

export default function Landing() {
  const token = api.getTokenRecord();
  const loggedIn = !!token;

  const handleRestrictedNavigation = (e: React.MouseEvent) => {
    e.preventDefault();
    toast.info('This feature will become available when LSST alerts are released publicly');
  };

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

      {/* Kilonova animation background */}
      <div className="fixed inset-0 pointer-events-none z-0">
        <Kilonova />
      </div>

      {/* Paper link in top right - hidden on mobile */}
      <a 
        href="https://arxiv.org/abs/2511.00164" 
        target="_blank" 
        rel="noopener noreferrer"
        className="hidden md:flex fixed top-6 right-6 z-20 px-4 py-2 bg-slate-800/80 backdrop-blur-sm border border-slate-600 rounded-lg text-slate-100 hover:bg-slate-700/90 hover:text-white hover:border-slate-500 transition-all duration-200 shadow-lg hover:shadow-xl items-center gap-2 fade-in-up"
      >
        <IconNews className="w-5 h-5" />
        Read our paper  
      </a>

      <div className="relative max-w-6xl w-full px-6 lg:px-12 py-12 md:py-20">

        <div className="relative z-10 max-w-3xl mx-auto text-center">
          {/* <h1 className="text-4xl md:text-5xl font-extrabold leading-tight tracking-tight mb-4 fade-in-up">BabamulA real-time & multi-survey alert broker</h1> */}
          {/*  let's break this down into 2 lines */}
            <div className="fade-in-up mb-4 relative">
              <div className="flex items-center justify-center gap-1 text-2xl md:text-3xl opacity-20 m-0 p-0 gap-10 mb-2 md:-mb-2">
                <span className="text-slate-200">𒁀</span>
                <span className="text-slate-400">·</span>
                <span className="text-slate-200">𒁀</span>
                <span className="text-slate-400">·</span>
                <span className="text-slate-200">𒀯</span>
              </div>
              <h1 className="text-6xl md:text-8xl font-extrabold leading-tight tracking-tight relative z-10 mb-2">
                Babamul
              </h1>
            </div>
            <h2 className="text-3xl md:text-5xl font-semibold leading-tight tracking-tight mb-6 fade-in-up">A real-time multi-survey alert broker for the LSST era.</h2>
          <p className="text-lg md:text-xl text-slate-200 mb-8 fade-in-up">Ingest, filter and stream optical transient alerts from ZTF and LSST at scale. Subscribe with Kafka, query with a flexible API, and build real-time alerting systems.</p>

          <div className="grid grid-cols-2 md:flex md:flex-row items-center justify-center gap-3 md:gap-4 fade-in-up">
            {PRERELEASE_MODE ? (
              <Button 
                onClick={handleRestrictedNavigation}
                className="col-span-2 md:col-span-1 px-6 py-3 bg-white/90 hover:bg-white text-slate-900 shadow-md hover:shadow-xl transform transition duration-150 ease-out hover:-translate-y-0.5"
              >
                Search for alerts
              </Button>
            ) : (
              <Link to="/query" className="col-span-2 md:col-span-1">
                <Button className="w-full px-6 py-3 bg-white/90 hover:bg-white text-slate-900 shadow-md hover:shadow-xl transform transition duration-150 ease-out hover:-translate-y-0.5">Search for alerts</Button>
              </Link>
            )}
            {PRERELEASE_MODE ? (
              <Button 
                onClick={handleRestrictedNavigation}
                variant="outline" 
                className="col-span-1 px-6 py-3 bg-slate-800/40 border-slate-600 text-slate-100 hover:bg-slate-700/60 hover:text-white transform transition duration-150 ease-out hover:-translate-y-0.5 hover:shadow-md"
              >
                Kafka docs
              </Button>
            ) : (
              <Link to="/docs/kafka" className="col-span-1">
                <Button variant="outline" className="w-full px-6 py-3 bg-slate-800/40 border-slate-600 text-slate-100 hover:bg-slate-700/60 hover:text-white transform transition duration-150 ease-out hover:-translate-y-0.5 hover:shadow-md">Kafka docs</Button>
              </Link>
            )}
            {PRERELEASE_MODE ? (
              <Button 
                onClick={handleRestrictedNavigation}
                variant="outline" 
                className="col-span-1 px-6 py-3 bg-slate-800/40 border-slate-600 text-slate-100 hover:bg-slate-700/60 hover:text-white transform transition duration-150 ease-out hover:-translate-y-0.5 hover:shadow-md"
              >
                API docs
              </Button>
            ) : (
              <Link to="/docs/api" state={{ from: '/' }} className="col-span-1">
                <Button variant="outline" className="w-full px-6 py-3 bg-slate-800/40 border-slate-600 text-slate-100 hover:bg-slate-700/60 hover:text-white transform transition duration-150 ease-out hover:-translate-y-0.5 hover:shadow-md">API docs</Button>
              </Link>
            )}
            <a href="https://arxiv.org/abs/2511.00164" target="_blank" rel="noopener noreferrer" className="col-span-2 md:hidden">
              <Button variant="outline" className="w-full px-6 py-3 bg-slate-800/40 border-slate-600 text-slate-100 hover:bg-slate-700/60 hover:text-white transform transition duration-150 ease-out hover:-translate-y-0.5 hover:shadow-md flex items-center justify-center gap-2">
                <IconNews className="w-4 h-4" />
                Read our paper
              </Button>
            </a>
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

          {!PRERELEASE_MODE && (loggedIn ? (
            <p className="mt-8 text-sm text-slate-400">You're signed in — try running a query on the <Link to="/query" className="underline">Query page</Link>.</p>
          ) : (
            <p className="mt-8 text-sm text-slate-400">Want to get started quickly? <Link to="/login" className="underline">Log in</Link> or <Link to="/signup" className="underline">create an account</Link>.</p>
          ))}
        </div>
      </div>
    </div>
  );
}
