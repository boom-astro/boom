import { BrowserRouter, Routes, Route, useNavigate, Navigate, useLocation } from "react-router-dom";
import api from "@/lib/api";
import useAppStore, { ensureProfileLoaded } from "@/lib/store";
import { Suspense, lazy, useEffect, useState } from "react";
import { AppSidebar } from "@/components/app-sidebar"
import { SiteHeader } from "@/components/site-header"
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"
import { ThemeProvider } from "@/components/theme-provider"
import { Toaster } from "@/components/ui/sonner";
import { Loader } from "@/components/ui/loader";

const Query = lazy(() => import("@/pages/Query"));
const Admin = lazy(() => import("@/pages/Admin"));
const ApiDocs = lazy(() => import("@/pages/ApiDocs"));
const KafkaDocs = lazy(() => import("@/pages/KafkaDocs"));
const KafkaAccessGuide = lazy(() => import("@/pages/KafkaAccessGuide"));
const Login = lazy(() => import("@/pages/Login"));
const OAuthCallback = lazy(() => import("@/pages/OAuthCallback"));
const OAuthComplete = lazy(() => import("@/pages/OAuthComplete"));
const ObjectPage = lazy(() => import("@/pages/ObjectPage"));
const SignupPage = lazy(() => import("@/pages/Signup"));
const Landing = lazy(() => import("@/pages/Landing"));
const Profile = lazy(() => import("@/pages/Profile"))
const ForgotPassword = lazy(() => import("@/pages/ForgotPassword"))
const ResetPassword = lazy(() => import("@/pages/ResetPassword"))
const Help = lazy(() => import("@/pages/Help"))
const Acknowledgments = lazy(() => import("@/pages/Acknowledgments"))
const Dashboard = lazy(() => import("@/pages/Dashboard"))
const Telescopes = lazy(() => import("@/pages/Telescopes"))
const Embeddings = lazy(() => import("@/pages/Embeddings"))

// Release mode flag - set VITE_PRERELEASE_MODE=true at build time to restrict app to landing page only
const PRERELEASE_MODE = import.meta.env.VITE_PRERELEASE_MODE === 'true';

// Home removed — landing page is the main entrypoint. Use `/query` for app search.

function LoginPageWrapper() {
  const navigate = useNavigate();
  // after login, redirect to /query or to the page the user originally
  // wanted to visit (minus login/signup/landing/root)
  const location = useLocation();

  return <Login onLoginSuccess={() => {
    let from = (location.state as { from: Location })?.from?.pathname || '/query';
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
  if (loading) return <Loader />;

  return children;
}

/**
 * Route guard for the admin section.
 *
 * Hiding the page is a convenience, not the control: every admin route checks
 * `is_admin` server-side, so a non-admin who navigates here directly gets a
 * 403 from the API rather than a working page.
 */
function AdminRoute({ children }: { children: React.ReactElement }) {
  // Wrapped rather than guarded first: `ProtectedRoute` is what loads the
  // profile, so checking `is_admin` ahead of it would hold a loader on a page
  // whose profile nothing had asked for.
  return <ProtectedRoute><AdminOnly>{children}</AdminOnly></ProtectedRoute>;
}

/**
 * Inside `ProtectedRoute`, so a profile is loaded -- but not necessarily one
 * that is still true.
 *
 * The store persists the profile to localStorage, so `is_admin` read out of it
 * is a hint and not a fact: it survives a reload, it can be edited there by
 * hand, and it goes stale the moment somebody's admin status is revoked
 * elsewhere. Nothing leaks either way, because every route behind this is
 * enforced server-side and answers 403 -- but a page that renders in full and
 * then fails every request on it is a worse answer than the redirect, and a
 * guard that cannot tell is not really a guard.
 *
 * So the profile is re-read from the API before deciding. The admin page did
 * already force a re-read, from inside one card on it; a route guard's
 * correctness should not depend on which of its children happen to be mounted.
 */
function AdminOnly({ children }: { children: React.ReactElement }) {
  const profile = useAppStore((s) => s.profile);
  const [checked, setChecked] = useState(false);

  useEffect(() => {
    let mounted = true;
    ensureProfileLoaded({ force: true }).finally(() => {
      if (mounted) setChecked(true);
    });
    return () => { mounted = false };
  }, []);

  if (!checked) return <Loader />;
  // `/query` rather than `/`: the landing page sends a signed-in user straight
  // back to `/query` anyway, so routing a denied admin through it bought a
  // flash of the marketing page and a second redirect.
  if (!profile?.is_admin) return <Navigate to="/query" replace />;
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
              <Suspense fallback={<Loader/>}>
                <Routes>
                  <Route path="/" element={<Landing />} />
                  <Route path="/landing" element={<Landing />} />
                  <Route path="/login" element={<LoginPageWrapper />} />
                  <Route path="/oauth/callback" element={<OAuthCallback />} />
                  <Route path="/oauth/complete" element={<OAuthComplete />} />
                  <Route path="/forgot-password" element={<ForgotPassword />} />
                  <Route path="/reset-password" element={<ResetPassword />} />
                  {!PRERELEASE_MODE && <Route path="/signup" element={<SignupPage />} />}
                  {!PRERELEASE_MODE && <Route path="/activate" element={<SignupPage />} />}
                  <Route path="/query" element={<ProtectedRoute><Query /></ProtectedRoute>} />
                  <Route path="/profile" element={<ProtectedRoute><Profile /></ProtectedRoute>} />
                  <Route path="/admin" element={<AdminRoute><Admin /></AdminRoute>} />
                  <Route path="/dashboard" element={<Dashboard />} />
                  <Route path="/telescopes" element={<Telescopes />} />
                  <Route path="/embeddings" element={<ProtectedRoute><Embeddings /></ProtectedRoute>} />
                  <Route path="/docs/kafka" element={<KafkaDocs />} />
                  <Route path="/docs/kafka/access-guide" element={<KafkaAccessGuide />} />
                  <Route path="/docs/api" element={<ApiDocs />} />
                  <Route path="/acknowledgments" element={<Acknowledgments />} />
                  <Route path="/help" element={<Help />} />
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
