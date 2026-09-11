import { useEffect, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import api from "@/lib/api";
import { ensureProfileLoaded, useAppStore } from "@/lib/store";
import * as analytics from "@/lib/analytics";
import { safeNext } from "@/lib/oauth";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { SignInError } from "@/components/sign-in-error";

/** Parameters arrive in the URL fragment, which browsers never transmit. */
export default function OAuthComplete() {
  const navigate = useNavigate();
  const location = useLocation();

  const [ticket, setTicket] = useState("");
  const [providerName, setProviderName] = useState("your account");
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");
  const [step, setStep] = useState<"email" | "code">("email");
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const initialized = useRef(false);

  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;

    const fragment = new URLSearchParams(window.location.hash.replace(/^#/, ""));
    // Query string too: confirmation emails sent before the move to the fragment still carry one.
    const query = new URLSearchParams(location.search);

    setTicket(fragment.get("ticket") || query.get("ticket") || "");
    setProviderName(fragment.get("provider_name") || "your account");
    const suggested = fragment.get("suggested_email");
    if (suggested) setEmail(suggested);

    const codeParam = fragment.get("code") || query.get("code");
    if (codeParam) {
      setCode(codeParam);
      setStep("code");
    }

    if (window.location.hash || window.location.search) {
      window.history.replaceState(null, "", window.location.pathname);
    }
  }, [location.search]);

  async function submitEmail(e?: React.FormEvent) {
    e?.preventDefault();
    setError(null);
    setMessage(null);
    setLoading(true);
    try {
      await api.completeOAuthEmail(ticket, email);
      setMessage(`We sent a confirmation code to ${email}. Enter it below to finish signing in.`);
      setStep("code");
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
      analytics.trackError("oauth_complete_email", err, { email });
    } finally {
      setLoading(false);
    }
  }

  async function submitCode(e?: React.FormEvent) {
    e?.preventDefault();
    setError(null);
    setLoading(true);
    try {
      const { next } = await api.verifyOAuthEmail(ticket, code);
      // The store's five-minute freshness window would otherwise serve the previous account.
      useAppStore.getState().clearProfile();
      try {
        const profile = await ensureProfileLoaded({ force: true });
        if (profile) analytics.identifyProfile(profile);
        analytics.trackLoginSuccess({ email: profile?.email ?? email });
      } catch (profileErr) {
        console.error("OAuthComplete: could not load profile", profileErr);
      }
      navigate(safeNext(next), { replace: true });
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
      analytics.trackError("oauth_verify_email", err, { email });
    } finally {
      setLoading(false);
    }
  }

  if (!ticket) {
    return (
      <SignInError title="Sign-in request missing">
        This page needs a sign-in request to finish. Please start again from the login page.
      </SignInError>
    );
  }

  return (
    <div className="w-full max-w-lg mx-auto p-4">
      <Card>
        <CardHeader>
          <CardTitle>Confirm your email</CardTitle>
          <CardDescription>
            {step === "email"
              ? `${providerName} didn't share an email address with us. Add one so we can finish setting up your Babamul account.`
              : `Enter the code we emailed to ${email}.`}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {step === "email" ? (
            <form onSubmit={submitEmail} className="grid gap-4">
              <div>
                <label className="text-sm font-medium" htmlFor="oauth-email">
                  Email
                </label>
                <Input
                  id="oauth-email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="you@example.com"
                  required
                  className="mt-1"
                />
                <p className="text-sm text-muted-foreground mt-2">
                  If you already have a Babamul account with this address, confirming will link{" "}
                  {providerName} to it.
                </p>
              </div>
              <Button type="submit" className="justify-self-start" disabled={loading || !email}>
                {loading ? "Sending…" : "Send confirmation code"}
              </Button>
            </form>
          ) : (
            <form onSubmit={submitCode} className="grid gap-4">
              <div>
                <label className="text-sm font-medium" htmlFor="oauth-code">
                  Confirmation code
                </label>
                <Input
                  id="oauth-code"
                  value={code}
                  onChange={(e) => setCode(e.target.value)}
                  placeholder="Enter the code from your email"
                  required
                  className="mt-1 font-mono tracking-widest uppercase"
                />
              </div>
              <div className="flex gap-2">
                <Button type="submit" disabled={loading || !code}>
                  {loading ? "Confirming…" : "Confirm and sign in"}
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  disabled={loading}
                  onClick={() => {
                    setStep("email");
                    setCode("");
                    setMessage(null);
                    setError(null);
                  }}
                >
                  Use a different email
                </Button>
              </div>
            </form>
          )}
          {message && <div className="text-sm text-muted-foreground mt-3">{message}</div>}
          {error && <div className="text-sm text-red-600 mt-3">{error}</div>}
        </CardContent>
      </Card>
    </div>
  );
}
