import { useState, useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import api from "@/lib/api";
import { LoginForm } from "@/components/login-form";
import * as analytics from "@/lib/analytics";

type Props = {
  onLoginSuccess: () => void;
};

export default function Login({ onLoginSuccess }: Props) {
  const location = useLocation();
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [prefilledFromSignup, setPrefilledFromSignup] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Credentials are handed off from signup via in-memory navigation state
    // (never persisted to web storage). Consume them once and clear the state
    // so they don't survive a refresh or back-navigation.
    const state = location.state as { prefillEmail?: string; prefillPassword?: string } | null;
    if (state?.prefillEmail || state?.prefillPassword) {
      if (state.prefillEmail) setEmail(state.prefillEmail);
      if (state.prefillPassword) setPassword(state.prefillPassword);
      setPrefilledFromSignup(true);
      navigate(location.pathname, { replace: true, state: null });
    }
  }, [location, navigate]);

  async function submit(e?: React.FormEvent) {
    e?.preventDefault();
    setLoading(true);
    setError(null);
    try {
      await api.login(email, password);
      const profile = await api.fetchProfile().catch(() => null);
      const identifiedEmail = profile?.email ?? email;
      const identifiedUserId = profile?.id ?? profile?.username ?? identifiedEmail;
      analytics.identifyUser(identifiedUserId, identifiedEmail);
      analytics.trackLoginSuccess({ email });
      onLoginSuccess();
    } catch (err: unknown) {
      const msg = err && typeof err === 'object' && 'message' in err ? String((err as { message?: unknown }).message) : String(err);
      setError(msg);
      analytics.trackError('login', err, { email });
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="w-full max-w-lg mx-auto p-4">
      {prefilledFromSignup && (
        <div className="mb-4 rounded-md bg-muted p-3 text-sm flex items-center justify-between">
          <div className="text-sm">Credentials were pre-filled from account activation.</div>
          <button
            className="ml-4 text-sm text-primary hover:underline"
            onClick={() => setPrefilledFromSignup(false)}
          >
            Dismiss
          </button>
        </div>
      )}
      <LoginForm
        email={email}
        password={password}
        onEmailChange={(e) => setEmail(e.target.value)}
        onPasswordChange={(e) => setPassword(e.target.value)}
        onSubmit={submit}
        loading={loading}
        error={error}
      />
    </div>
  );
}
