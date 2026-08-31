import { useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import api from "@/lib/api";
import { ensureProfileLoaded, useAppStore } from "@/lib/store";
import * as analytics from "@/lib/analytics";
import { Loader } from "@/components/ui/loader";

/**
 * Landing spot for a social sign-in.
 *
 * The API redirects here with the result in the URL *fragment* — browsers
 * never transmit fragments, so the token stays out of server logs and
 * `Referer` headers. We read it, hand it to the token store, and scrub it from
 * the address bar before navigating on.
 */
export default function OAuthCallback() {
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);
  // React 18 mounts effects twice in StrictMode; the fragment is consumed
  // destructively, so guard against running this a second time.
  const handled = useRef(false);

  useEffect(() => {
    if (handled.current) return;
    handled.current = true;

    const params = new URLSearchParams(window.location.hash.replace(/^#/, ""));
    const failure = params.get("error");
    const accessToken = params.get("access_token");

    // Drop the fragment either way so a refresh or a shared URL can't replay it.
    window.history.replaceState(null, "", window.location.pathname);

    if (failure) {
      setError(failure);
      analytics.trackError("oauth_login", new Error(failure));
      return;
    }
    if (!accessToken) {
      setError("Sign-in did not return a token. Please try again.");
      return;
    }

    const expiresIn = Number(params.get("expires_in"));
    api.saveOAuthToken({
      access_token: accessToken,
      token_type: params.get("token_type") || "Bearer",
      expires_in: Number.isFinite(expiresIn) && expiresIn > 0 ? expiresIn : undefined,
    });
    // The token just changed, so whatever profile the store is holding belongs
    // to whoever was signed in before. Drop it: a cached profile still inside
    // its five-minute window would otherwise survive the switch and leave the
    // sidebar and protected pages showing the previous account.
    useAppStore.getState().clearProfile();

    const next = params.get("next");
    // Only in-app paths; the API already filters these, this is belt and braces.
    const destination = next && next.startsWith("/") && !next.startsWith("//") ? next : "/query";

    (async () => {
      try {
        const profile = await ensureProfileLoaded({ force: true });
        if (profile) {
          analytics.identifyUser(profile.id ?? profile.username ?? profile.email, profile.email);
        }
        analytics.trackLoginSuccess({ email: profile?.email });
      } catch (err) {
        // A profile hiccup shouldn't strand a user who is already signed in.
        console.error("OAuthCallback: could not load profile", err);
      }
      navigate(destination, { replace: true });
    })();
  }, [navigate]);

  if (error) {
    return (
      <div className="w-full max-w-lg mx-auto p-4">
        <div className="rounded-md border border-destructive/40 bg-destructive/10 p-4">
          <h1 className="font-medium mb-1">Sign-in failed</h1>
          <p className="text-sm text-muted-foreground">{error}</p>
          <Link to="/login" className="mt-3 inline-block text-sm underline">
            Back to login
          </Link>
        </div>
      </div>
    );
  }

  return <Loader />;
}
