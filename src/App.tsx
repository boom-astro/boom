import { BrowserRouter, Routes, Route, useNavigate, Navigate, useLocation } from "react-router-dom";
import api from "@/lib/api";
import useAppStore, { ensureProfileLoaded } from "@/lib/store";
import { Suspense, lazy, useEffect } from "react";
import { AppSidebar } from "@/components/app-sidebar"
import { SiteHeader } from "@/components/site-header"
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"
import { ThemeProvider } from "@/components/theme-provider"
import { Toaster } from "sonner";

const Query = lazy(() => import("@/pages/Query"));
const KafkaDocs = lazy(() => import("@/pages/KafkaDocs"));
const ApiDocs = lazy(() => import("@/pages/ApiDocs"));
const Login = lazy(() => import("@/pages/Login"));
const ObjectPage = lazy(() => import("@/pages/ObjectPage"));
const SignupPage = lazy(() => import("@/pages/Signup"));
const Landing = lazy(() => import("@/pages/Landing"));
const Profile = lazy(() => import("@/pages/Profile"))

// Release mode flag - set VITE_PRERELEASE_MODE=true at build time to restrict app to landing page only
const PRERELEASE_MODE = import.meta.env.VITE_PRERELEASE_MODE === 'true';

// Home removed — landing page is the main entrypoint. Use `/query` for app search.

function LoginPageWrapper() {
  const navigate = useNavigate();
  // after login, redirect to /query or to the page the user originally
  // wanted to visit (minus login/signup/landing/root)
  const location = useLocation();

  return <Login onLoginSuccess={() => {
    let from = (location.state as any)?.from?.pathname || '/query';
    if (from === '/login' || from === '/signup' || from === '/landing' || from === '/') {
      from = '/query';
    }
    navigate(from, { replace: true });
  }} />;
}

export default function App() {
  return (
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      <SidebarProvider defaultOpen={false}>
        <BrowserRouter>
          {/* Layout wrapper to conditionally show header/sidebar on non-landing routes */}
          <LayoutRoutes />
        </BrowserRouter>
      </SidebarProvider>
    </ThemeProvider>
  )
}

function ProtectedRoute({ children }: { children: React.ReactElement }) {
  const location = useLocation();
  const token = api.getTokenRecord();
  const profile = useAppStore((s) => s.profile);
  const loading = !!token && !profile;

  useEffect(() => {
    if (!token) return;
    if (profile?.username) return;
    let mounted = true;
    ensureProfileLoaded().catch((err) => { if (mounted) console.error(err) });
    return () => { mounted = false };
  }, [token, profile]);

  if (!token) {
    return <Navigate to="/login" replace state={{ from: location }} />;
  }
  if (loading) {
    return <div className="px-4 py-6">Loading...</div>;
  }
  return children;
}

function LayoutRoutes() {
  const location = useLocation();
  const path = location.pathname || '/';
  const isLanding = path === '/' || path === '/landing';

  return (
    <>
      {!isLanding && <AppSidebar variant="inset" />}
      <SidebarInset>
        {!isLanding && <SiteHeader />}
        <Toaster />
        <div className="flex flex-1 flex-col">
          <div className="@container/main flex flex-1 flex-col gap-2">
            <div className={`flex flex-col gap-4 ${isLanding ? '' : 'py-4 md:gap-6 md:py-6'}`}>
              <Suspense fallback={<div className="px-4 py-6">Loading...</div>}>
                <Routes>
                  <Route path="/" element={<Landing />} />
                  <Route path="/landing" element={<Landing />} />
                  <Route path="/login" element={<LoginPageWrapper />} />
                  {!PRERELEASE_MODE && <Route path="/signup" element={<SignupPage />} />}
                  <Route path="/query" element={<ProtectedRoute><Query /></ProtectedRoute>} />
                  <Route path="/profile" element={<ProtectedRoute><Profile /></ProtectedRoute>} />
                  <Route path="/docs/kafka" element={<ProtectedRoute><KafkaDocs /></ProtectedRoute>} />
                  <Route path="/docs/api" element={<ApiDocs />} />
                  <Route path="/objects/:survey/:objectId" element={<ProtectedRoute><ObjectPage /></ProtectedRoute>} />
                </Routes>
              </Suspense>
            </div>
          </div>
        </div>
      </SidebarInset>
    </>
  );
}
