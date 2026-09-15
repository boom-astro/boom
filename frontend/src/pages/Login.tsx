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
    // Consume once: navigation state left in place is replayed by a refresh or a back-navigation.
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
      // No id to identify by: staying anonymous lets the next identify merge this in.
      if (profile) analytics.identifyProfile(profile);
      analytics.trackLoginSuccess();
      onLoginSuccess();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
      analytics.trackError('login', err);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="w-full max-w-lg mx-auto p-4">
      {prefilledFromSignup && (
        <div className="mb-4 rounded-md bg-muted p-3 text-sm flex items-center justify-between">
          <span>Credentials were pre-filled from account activation.</span>
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
