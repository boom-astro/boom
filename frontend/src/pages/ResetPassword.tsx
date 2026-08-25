import { useState, useEffect } from 'react';
import { Link, useNavigate, useLocation } from 'react-router-dom';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field';
import api from '@/lib/api';

const rules = [
  { key: 'length',    label: 'At least 8 characters',       test: (p: string) => p.length >= 8 },
  { key: 'upper',     label: 'One uppercase letter (A–Z)',   test: (p: string) => /[A-Z]/.test(p) },
  { key: 'lower',     label: 'One lowercase letter (a–z)',   test: (p: string) => /[a-z]/.test(p) },
  { key: 'digit',     label: 'One digit (0–9)',              test: (p: string) => /[0-9]/.test(p) },
  { key: 'special',   label: 'One special character',        test: (p: string) => /[^A-Za-z0-9]/.test(p) },
];

function PasswordRules({ password, visible }: { password: string; visible: boolean }) {
  if (!visible) return null;
  return (
    <ul className="mt-2 flex flex-col gap-1">
      {rules.map(({ key, label, test }) => {
        const met = test(password);
        return (
          <li key={key} className={`flex items-center gap-2 text-xs ${met ? 'text-green-500' : 'text-muted-foreground'}`}>
            <span className="shrink-0 text-base leading-none">{met ? '✓' : '○'}</span>
            {label}
          </li>
        );
      })}
    </ul>
  );
}

export default function ResetPasswordPage() {
  const location = useLocation();
  const navigate = useNavigate();

  const [email, setEmail] = useState('');
  const [token, setToken] = useState('');
  const [password, setPassword] = useState('');
  const [confirm, setConfirm] = useState('');
  const [passwordFocused, setPasswordFocused] = useState(false);
  const [confirmFocused, setConfirmFocused] = useState(false);
  const [loading, setLoading] = useState(false);
  const [done, setDone] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const e = params.get('email');
    const t = params.get('token');
    if (e) setEmail(e);
    if (t) setToken(t);
  }, [location.search]);

  const allRulesMet = rules.every(({ test }) => test(password));

  async function handleSubmit(e?: React.FormEvent) {
    e?.preventDefault();
    setError(null);

    if (!allRulesMet) {
      setError('Please meet all password requirements.');
      return;
    }
    if (password !== confirm) {
      setError('Passwords do not match.');
      return;
    }

    setLoading(true);
    try {
      await api.resetPassword(email, token, password);
      setDone(true);
      // Redirect to login after a short delay
      setTimeout(() => navigate('/login', { replace: true }), 2500);
    } catch (err: unknown) {
      const msg =
        err && typeof err === 'object' && 'message' in err
          ? String((err as { message?: unknown }).message)
          : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
  }

  const missingParams = !email || !token;

  return (
    <div className="w-full max-w-lg mx-auto p-4">
      <div className="flex flex-col gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Reset your password</CardTitle>
            <CardDescription>
              {missingParams
                ? 'This link appears to be invalid or expired.'
                : `Enter a new password for ${email}.`}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {done ? (
              <div className="flex flex-col gap-4">
                <p className="text-sm text-muted-foreground">
                  Your password has been reset. Redirecting to login…
                </p>
                <Link to="/login" className="text-sm underline underline-offset-4">
                  Go to login
                </Link>
              </div>
            ) : missingParams ? (
              <div className="flex flex-col gap-4">
                <p className="text-sm text-muted-foreground">
                  Please use the link from the password reset email. If the link has expired, request a new one.
                </p>
                <Link to="/forgot-password" className="text-sm underline underline-offset-4">
                  Request a new reset link
                </Link>
              </div>
            ) : (
              <form onSubmit={handleSubmit}>
                <FieldGroup>
                  <Field>
                    <FieldLabel htmlFor="password">New password</FieldLabel>
                    <Input
                      id="password"
                      type="password"
                      placeholder="At least 8 characters"
                      required
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      onFocus={() => setPasswordFocused(true)}
                    />
                    <PasswordRules password={password} visible={passwordFocused || password.length > 0} />
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="confirm">Confirm new password</FieldLabel>
                    <Input
                      id="confirm"
                      type="password"
                      placeholder="Repeat your password"
                      required
                      value={confirm}
                      onChange={(e) => setConfirm(e.target.value)}
                      onFocus={() => setConfirmFocused(true)}
                    />
                    {(confirmFocused || confirm.length > 0) && (
                      <ul className="mt-2 flex flex-col gap-1">
                        <li className={`flex items-center gap-2 text-xs ${confirm.length > 0 && confirm === password ? 'text-green-500' : 'text-muted-foreground'}`}>
                          <span className="shrink-0 text-base leading-none">{confirm.length > 0 && confirm === password ? '✓' : '○'}</span>
                          Passwords match
                        </li>
                      </ul>
                    )}
                  </Field>
                  <Field>
                    <Button type="submit" disabled={loading}>
                      {loading ? 'Resetting…' : 'Reset password'}
                    </Button>
                  </Field>
                </FieldGroup>
                {error && <div className="text-red-600 mt-2 text-sm">{error}</div>}
              </form>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
