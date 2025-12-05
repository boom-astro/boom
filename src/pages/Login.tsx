import { useState, useEffect } from "react";
import api from "@/lib/api";
import { LoginForm } from "@/components/login-form";

type Props = {
  onLoginSuccess: () => void;
};

export default function Login({ onLoginSuccess }: Props) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [prefilledFromSignup, setPrefilledFromSignup] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    try {
      const raw = localStorage.getItem('signup_prefill');
      if (raw) {
        const parsed = JSON.parse(raw) as { email?: string; password?: string };
        if (parsed.email) setEmail(parsed.email);
        if (parsed.password) setPassword(parsed.password);
        // remember that we applied prefill so we can show a banner
        setPrefilledFromSignup(true);
        localStorage.removeItem('signup_prefill');
      }
    } catch {
      // ignore
    }
  }, []);

  async function submit(e?: React.FormEvent) {
    e?.preventDefault();
    setLoading(true);
    setError(null);
    try {
      await api.login(email, password);
      onLoginSuccess();
    } catch (err: unknown) {
      const msg = err && typeof err === 'object' && 'message' in err ? String((err as { message?: unknown }).message) : String(err);
      setError(msg);
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
