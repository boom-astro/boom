import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import api from "@/lib/api";
import { ensureProfileLoaded, useAppStore } from "@/lib/store";
import * as analytics from "@/lib/analytics";
import { safeNext } from "@/lib/oauth";
import { Loader } from "@/components/ui/loader";
import { SignInError } from "@/components/sign-in-error";

/** The API returns the token in the URL fragment, which browsers never transmit. */
export default function OAuthCallback() {
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);
  // StrictMode mounts effects twice, and the fragment below is consumed destructively.
  const handled = useRef(false);

  useEffect(() => {
    if (handled.current) return;
    handled.current = true;

    const params = new URLSearchParams(window.location.hash.replace(/^#/, ""));
    const failure = params.get("error");
    const accessToken = params.get("access_token");

    // Before the early returns: a fragment left in place can be replayed by a refresh or a share.
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
    // The store's five-minute freshness window would otherwise serve the previous account.
    useAppStore.getState().clearProfile();

    const destination = safeNext(params.get("next"));

    (async () => {
      try {
        const profile = await ensureProfileLoaded({ force: true });
        if (profile) analytics.identifyProfile(profile);
        analytics.trackLoginSuccess();
      } catch (err) {
        console.error("OAuthCallback: could not load profile", err);
      }
      navigate(destination, { replace: true });
    })();
  }, [navigate]);

  if (error) return <SignInError title="Sign-in failed">{error}</SignInError>;

  return <Loader />;
}
