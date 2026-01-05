import { useState, useEffect, useRef } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Alert, AlertTitle, AlertDescription } from '@/components/ui/alert';
// Use same-origin proxy; prod nginx should route /api to backend
const API_BASE = '/api/babamul';

export default function SignupPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const autoActivatedRef = useRef(false);

  // If the activation link includes `email` and `activation_code`, prefill and auto-activate.
  useEffect(() => {
    try {
      const params = new URLSearchParams(location.search);
      const e = params.get('email');
      const code = params.get('activation_code') ?? params.get('code');
      if (e) setEmail(e);
      if (code) setActivationCode(code);
      if (e && code && !autoActivatedRef.current) {
        autoActivatedRef.current = true;
        setStep('code');
        // attempt activation automatically
        activateAccount(e, code);
      }
    } catch {
      // ignore malformed query
    }
  }, [location.search]);
  const [email, setEmail] = useState('');
  const [loading, setLoading] = useState(false);
  const [step, setStep] = useState<'email'|'code'|'done'>('email');
  const [message, setMessage] = useState<string | null>(null);
  const [activationCode, setActivationCode] = useState<string>('');
  
  const [password, setPassword] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const copyTimerRef = useRef<number | null>(null);

  async function submitEmail(e?: React.FormEvent) {
    e?.preventDefault();
    setError(null);
    setMessage(null);
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/signup`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email }),
      });
      if (!res.ok) {
        const txt = await res.text().catch(() => '');
        throw new Error(`Sign up failed: ${res.status} ${txt}`);
      }
      const body = await res.json().catch(() => ({}));
      // backend may return { activation_code } for dev; or just a message telling email was sent
      if (body && (body.activation_code || body.code)) {
        const code = (body.activation_code ?? body.code) as string;
        setActivationCode(code);
        setMessage('Activation code received (development mode). Enter it below to activate your account.');
        setStep('code');
      } else {
        setMessage('An email has been sent with an activation code. Check your inbox.');
        setStep('code');
      }
    } catch (err: unknown) {
      const msg = err && typeof err === 'object' && 'message' in err ? String((err as { message?: unknown }).message) : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
  }

  async function submitCode(e?: React.FormEvent) {
    e?.preventDefault();
    setError(null);
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/activate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, activation_code: activationCode }),
      });
      if (!res.ok) {
        const txt = await res.text().catch(() => '');
        throw new Error(`Activation failed: ${res.status} ${txt}`);
      }
      const body = await res.json().catch(() => ({}));
      // backend returns the password in the response (per spec)
      const pw = (body && (body.password ?? body.pw ?? body.pass)) as string | undefined;
      if (pw) {
        // show the generated password to the user and allow them to copy/save it
        setPassword(pw);
        setMessage('Account activated. Your password is shown below — save it now, it will not be shown again.');
        setStep('done');
      } else {
        setMessage('An account with this email is already activated.');
        setStep('done');
      }
    } catch (err: unknown) {
      const msg = err && typeof err === 'object' && 'message' in err ? String((err as { message?: unknown }).message) : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
  }

  // helper to activate using explicit values (used for auto-activation via URL)
  async function activateAccount(emailToUse: string, codeToUse: string) {
    setError(null);
    setMessage(null);
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/activate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: emailToUse, activation_code: codeToUse }),
      });
      if (!res.ok) {
        const txt = await res.text().catch(() => '');
        throw new Error(`Activation failed: ${res.status} ${txt}`);
      }
      const body = await res.json().catch(() => ({}));
      const pw = (body && (body.password ?? body.pw ?? body.pass)) as string | undefined;
      if (pw) {
        setPassword(pw);
        setMessage('Account activated. Your password is shown below — save it now, it will not be shown again.');
        setStep('done');
      } else {
        setMessage('An account with this email is already activated.');
        setStep('done');
      }
    } catch (err: unknown) {
      const msg = err && typeof err === 'object' && 'message' in err ? String((err as { message?: unknown }).message) : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
  }

  // copy handler for the password box with temporary visual feedback
  function handleCopyPassword() {
    if (!password) return;
    try {
      navigator.clipboard.writeText(password).then(() => {
        setCopied(true);
        if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current);
        copyTimerRef.current = window.setTimeout(() => setCopied(false), 2500);
      }).catch(() => {
        // ignore clipboard errors
      });
    } catch {
      // ignore
    }
  }

  // clear any pending timers when component unmounts
  useEffect(() => {
    return () => {
      if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current);
    };
  }, []);

  return (
    <div className="w-full max-w-lg mx-auto p-4">
      <div className="mb-4 text-center">
        <h1 className="text-2xl font-semibold">Create your Babamul account</h1>
        <p className="text-sm text-muted-foreground">Sign up with your email — we'll send an activation code to finish setup.</p>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Create an account</CardTitle>
          <CardDescription>Sign up with your email to receive an activation code.</CardDescription>
        </CardHeader>
        <CardContent>
          {step === 'email' && (
            <form onSubmit={submitEmail} className="grid gap-4">
              <div>
                <label className="text-sm font-medium">Email</label>
                <Input value={email} onChange={(e) => setEmail(e.target.value)} type="email" placeholder="you@example.com" required className="mt-1" />
              </div>
              <div className="flex gap-2">
                <Button type="submit" disabled={loading || !email}>{loading ? 'Sending…' : 'Send activation'}</Button>
              </div>
              {message && <div className="text-sm text-muted-foreground mt-2">{message}</div>}
              {error && <div className="text-sm text-red-600 mt-2">{error}</div>}
            </form>
          )}

          {step === 'code' && (
            <form onSubmit={submitCode} className="grid gap-4">
              <div>
                <label className="text-sm font-medium">Activation code</label>
                <Input value={activationCode} onChange={(e) => setActivationCode(e.target.value)} type="text" placeholder="Enter activation code" required className="mt-1" />
                <p className="text-sm text-muted-foreground mt-2">Enter the activation code you received by email.</p>
              </div>
              <div className="flex gap-2">
                <Button type="submit" disabled={loading || !activationCode}>{loading ? 'Activating…' : 'Activate account'}</Button>
              </div>
              {message && <div className="text-sm text-muted-foreground mt-2">{message}</div>}
              {error && <div className="text-sm text-red-600 mt-2">{error}</div>}
            </form>
          )}

          {step === 'done' && (
            <div className="grid gap-4">
              <div>
                {password ? (
                    <div className="mb-3">
                      <div className="flex items-center gap-3 rounded bg-green-50 border border-green-200 p-3">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" className="size-4 text-green-600" aria-hidden>
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.5" d="M5 13l4 4L19 7" />
                        </svg>
                        <div className="text-sm font-medium text-green-700">Account activated successfully</div>
                      </div>
                    </div>
                ) : null}
                <p className="text-sm">{message}</p>
                {password ? (
                    <Alert className="mt-1">
                      <AlertTitle className="flex items-center gap-2 mb-2">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="currentColor"
                            className="size-6 text-orange-500"
                            aria-hidden
                        >
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.5" d="M10.29 3.86L1.82 18a1.5 1.5 0 001.29 2.25h17.78a1.5 1.5 0 001.29-2.25L13.71 3.86a1.5 1.5 0 00-2.42 0z" />
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.5" d="M12 9v4" />
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.5" d="M12 17h.01" />
                        </svg>
                        <p className="text-orange-500 font-semibold">
                        Save this password — it will only be shown once
                        </p>
                      </AlertTitle>
                      <AlertDescription>
                            <div
                              role="button"
                              tabIndex={0}
                              onClick={handleCopyPassword}
                              onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); handleCopyPassword(); } }}
                              className={`font-mono text-base md:text-lg font-semibold inline-block w-full text-center whitespace-nowrap overflow-x-auto p-2 rounded border transition-colors duration-150 ease-out cursor-pointer ${copied ? 'border-green-500 text-green-700 bg-green-50' : 'border-orange-300 text-orange-700 bg-orange-50 hover:bg-orange-100 hover:border-orange-400 hover:text-orange-800'} focus:outline-none ${copied ? 'focus:ring-2 focus:ring-green-200' : 'focus:ring-2 focus:ring-orange-200'}`}
                            >
                              <span className="inline-block px-1">{password}</span>
                            </div>
                            <p className="mt-3 font-medium">This is the only time this password will be displayed. Copy or save it now.</p>
                            <div className="flex gap-2 mt-3">
                                <Button onClick={() => navigator.clipboard.writeText(password)}>Copy password</Button>
                                <Button onClick={async () => {
                                if (!password) return;
                                try {
                                  localStorage.setItem('signup_prefill', JSON.stringify({ email, password }));
                                } catch (err) {
                                  console.warn('Failed to persist signup prefill', err);
                                }
                                navigate('/login');
                                }}>Proceed to login</Button>
                            </div>
                      </AlertDescription>
                    </Alert>
                ) : null}
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
