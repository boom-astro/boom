import { BrowserRouter, Routes, Route, useNavigate, Navigate, useLocation } from "react-router-dom";
import api from "@/lib/api";
import useAppStore, { ensureProfileLoaded } from "@/lib/store";
import { useEffect } from "react";
import Query from "@/pages/Query";
import BabamulDocs from "@/pages/BabamulDocs";
import { AppSidebar } from "@/components/app-sidebar"
import { SiteHeader } from "@/components/site-header"
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"
import { ThemeProvider } from "@/components/theme-provider"
import { Toaster } from "sonner";
import Login from "@/pages/Login";
import ObjectPage from "@/pages/ObjectPage";

function Home() {
  const navigate = useNavigate();

  return (
    <div className="px-4 lg:px-6">
      <h2 className="text-lg font-medium mb-2">Welcome</h2>
      <p className="mb-4">Search for an object by survey and object id.</p>
      <div className="mb-4">
        <button className="btn" onClick={() => navigate('/query')}>Open Query</button>
      </div>
    </div>
  )
}

function LoginPageWrapper() {
  const navigate = useNavigate();
  return <Login onLoginSuccess={() => navigate('/')} />;
}

export default function App() {
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

  return (
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      <SidebarProvider>
        <BrowserRouter>
          <AppSidebar variant="inset" />
          <SidebarInset>
            <SiteHeader />
            <Toaster />
            <div className="flex flex-1 flex-col">
              <div className="@container/main flex flex-1 flex-col gap-2">
                <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6">
                  <Routes>
                    <Route path="/login" element={<LoginPageWrapper />} />
                    <Route path="/" element={<ProtectedRoute><Home /></ProtectedRoute>} />
                    <Route path="/query" element={<ProtectedRoute><Query /></ProtectedRoute>} />
                    <Route path="/babamul/docs" element={<ProtectedRoute><BabamulDocs /></ProtectedRoute>} />
                    <Route path="/objects/:survey/:objectId" element={<ProtectedRoute><ObjectPage /></ProtectedRoute>} />
                  </Routes>
                </div>
              </div>
            </div>
          </SidebarInset>
        </BrowserRouter>
      </SidebarProvider>
    </ThemeProvider>
  )
}
