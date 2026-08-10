import { useEffect, useRef, useState } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import api from "@/lib/api";
import { ensureProfileLoaded } from "@/lib/store";
import * as analytics from "@/lib/analytics";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

/**
 * Finishes a social sign-in whose provider gave us no email address.
 *
 * ORCID is the common case: most researchers keep their email private, so the
 * only thing the provider vouches for is the ORCID iD. Rather than inventing a
 * stand-in address, we ask for a real one and mail a code to prove the person
 * controls it. No account exists until that code comes back.
 *
 * The ticket standing in for the authenticated identity arrives in the URL
 * fragment; the confirmation email's link supplies both ticket and code as
 * query parameters, which skips straight to the code step.
 */
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

    // Fragment: arriving from the sign-in redirect. Query: arriving from the
    // "Confirm My Email" button in the confirmation email.
    const fragment = new URLSearchParams(window.location.hash.replace(/^#/, ""));
    const query = new URLSearchParams(location.search);

    const ticketParam = fragment.get("ticket") || query.get("ticket") || "";
    setTicket(ticketParam);
    setProviderName(fragment.get("provider_name") || "your account");
    const suggested = fragment.get("suggested_email");
    if (suggested) setEmail(suggested);

    const codeParam = query.get("code");
    if (codeParam) {
      setCode(codeParam);
      setStep("code");
    }

    // Keep the ticket out of the address bar so it isn't shared or replayed.
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
      try {
        await ensureProfileLoaded();
        const profile = await api.fetchProfile().catch(() => null);
        if (profile) {
          analytics.identifyUser(profile.id ?? profile.username ?? profile.email, profile.email);
        }
        analytics.trackLoginSuccess({ email: profile?.email ?? email });
      } catch (profileErr) {
        // Already signed in; a profile hiccup shouldn't strand the user here.
        console.error("OAuthComplete: could not load profile", profileErr);
      }
      const destination = next && next.startsWith("/") && !next.startsWith("//") ? next : "/query";
      navigate(destination, { replace: true });
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
      analytics.trackError("oauth_verify_email", err, { email });
    } finally {
      setLoading(false);
    }
  }

  if (!ticket) {
    return (
      <div className="w-full max-w-lg mx-auto p-4">
        <div className="rounded-md border border-destructive/40 bg-destructive/10 p-4">
          <h1 className="font-medium mb-1">Sign-in request missing</h1>
          <p className="text-sm text-muted-foreground">
            This page needs a sign-in request to finish. Please start again from the login page.
          </p>
          <Link to="/login" className="mt-3 inline-block text-sm underline">
            Back to login
          </Link>
        </div>
      </div>
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
              <div>
                <Button type="submit" disabled={loading || !email}>
                  {loading ? "Sending…" : "Send confirmation code"}
                </Button>
              </div>
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
