// Always use the same-origin proxy; production should map /api to the backend via the web server
const API_BASE = "/api/babamul";

export type TokenRecord = {
  access_token: string;
  token_type: string;
  expires_at: number; // unix ms
};

// Generic API object shape for responses we don't have a stricter schema for
export type ApiObject = Record<string, unknown>;

// Profile shape returned by `/profile`
export type Profile = {
  /** Babamul user id, and the PostHog `distinct_id` the API keys its events on. */
  id: string;
  username: string;
  email: string;
  created_at: number;
  name?: string;
  avatar?: string;
  /** Provider slugs the account can sign in with, e.g. ["google", "orcid"] */
  identity_providers?: string[];
  orcid_id?: string | null;
} | null;

// A social sign-in provider advertised by `/oauth/providers`
export type OAuthProvider = { id: string; name: string; start_url: string };

// Kafka credential returned by `/babamul/kafka-credentials`
export type KafkaCredential = { id: string; name: string; kafka_username: string; kafka_password: string };

// Personal Access Token types
export type TokenPublic = { id: string; name: string; created_at: number; expires_at: number; last_used_at: number | null };
export type TokenResponse = { id: string; name: string; access_token: string; created_at: number; expires_at: number };

const TOKEN_KEY = "api_token";
const USERNAME_KEY = "api_user";

// Helper function to parse JSON with bigint support
async function parseResponseJson(res: Response): Promise<unknown> {
  const text = await res.text();
  // TODO: find a better way to handle bigints in JSON without a heavy dependency
  // For now we convert them to strings by wrapping large integers in quotes, to avoid loss of precision
  const bigIntRegex = /:\s*(-?\d{16,})(?![\d]*["])/g;
  const safeText = text.replace(bigIntRegex, (_, numStr) => `:"${numStr}"`);
  return JSON.parse(safeText);
}

type DataEnvelope<T> = { data?: T };

function unwrapData<T>(body: unknown, fallback: T): T {
  if (body && typeof body === 'object' && 'data' in body) {
    const dataVal = (body as DataEnvelope<unknown>).data;
    if (dataVal !== undefined) return dataVal as T;
  }
  if (body !== null && body !== undefined) {
    return body as T;
  }
  return fallback;
}

function getSavedToken(): TokenRecord | null {
  const raw = localStorage.getItem(TOKEN_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw) as TokenRecord;
  } catch {
    return null;
  }
}

function saveToken(body: { access_token: string; token_type?: string; expires_in?: number }) {
  const now = Date.now();
  const expires_in = typeof body.expires_in === "number" ? body.expires_in : 3600;
  const rec: TokenRecord = {
    access_token: body.access_token,
    token_type: body.token_type || "Bearer",
    expires_at: now + expires_in * 1000,
  };
  localStorage.setItem(TOKEN_KEY, JSON.stringify(rec));
}

export function logout() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USERNAME_KEY);
}

export function getTokenRecord(): TokenRecord | null {
  const t = getSavedToken();
  if (!t) return null;
  if (t.expires_at <= Date.now()) {
    // expired -> clear and return null
    localStorage.removeItem(TOKEN_KEY);
    return null;
  }
  return t;
}

export async function login(email: string, password: string): Promise<TokenRecord | null> {
  const res = await fetch(`${API_BASE}/auth`, {
    method: "POST",
    // headers: { "Content-Type": "application/json" },
    // use form-encoded for compatibility with OAuth2 password grant
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ email, password }).toString(),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Login failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res);
  if (!body || typeof body !== 'object' || !('access_token' in body)) throw new Error("Auth response missing access_token");
  saveToken(body as { access_token: string; token_type?: string; expires_in?: number });
  try {
    localStorage.setItem(USERNAME_KEY, email);
  } catch {
    // ignore
  }
  return getTokenRecord();
}

/**
 * Social sign-in providers this deployment has configured.
 *
 * Returns an empty list when none are set up, so the login page can simply
 * render nothing rather than special-casing the feature being off.
 */
export async function fetchOAuthProviders(): Promise<OAuthProvider[]> {
  const res = await fetch(`${API_BASE}/oauth/providers`);
  if (!res.ok) return [];
  const body = await parseResponseJson(res).catch(() => ({ data: [] }));
  const result = unwrapData<unknown>(body, []);
  return Array.isArray(result) ? (result as OAuthProvider[]) : [];
}

/**
 * Hand the browser over to a provider's consent screen.
 *
 * A full navigation, not a fetch: the OAuth flow is a chain of redirects that
 * ends back at `/oauth/callback` in this app.
 */
export function startOAuthLogin(provider: OAuthProvider, redirectTo?: string) {
  const params = new URLSearchParams();
  if (redirectTo) params.set("redirect_to", redirectTo);
  const query = params.toString();
  // `start_url` is API-relative; API_BASE already carries the /api prefix.
  const path = provider.start_url.replace(/^\/babamul/, "");
  window.location.assign(`${API_BASE}${path}${query ? `?${query}` : ""}`);
}

/**
 * Store the token an OAuth callback delivered in the URL fragment.
 *
 * Mirrors what `login()` persists so the rest of the app can't tell the two
 * sign-in paths apart.
 */
export function saveOAuthToken(params: { access_token: string; token_type?: string; expires_in?: number }) {
  saveToken(params);
  return getTokenRecord();
}

/**
 * Supply an email address for a sign-in whose provider didn't give us one.
 *
 * Mails a confirmation code; no account is created until `verifyOAuthEmail`
 * receives it back.
 */
export async function completeOAuthEmail(ticket: string, email: string): Promise<void> {
  const res = await fetch(`${API_BASE}/oauth/complete`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ticket, email }),
  });
  if (!res.ok) {
    const body = await parseResponseJson(res).catch(() => null);
    const message =
      body && typeof body === "object" && "message" in body
        ? String((body as { message?: unknown }).message)
        : `Request failed: ${res.status}`;
    throw new Error(message);
  }
}

/** Confirm the emailed code, finishing the sign-in and storing the token. */
export async function verifyOAuthEmail(ticket: string, code: string): Promise<{ next?: string }> {
  const res = await fetch(`${API_BASE}/oauth/verify`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ticket, code }),
  });
  const body = await parseResponseJson(res).catch(() => null);
  if (!res.ok) {
    const message =
      body && typeof body === "object" && "message" in body
        ? String((body as { message?: unknown }).message)
        : `Confirmation failed: ${res.status}`;
    throw new Error(message);
  }
  if (!body || typeof body !== "object" || !("access_token" in body)) {
    throw new Error("Confirmation response missing access_token");
  }
  saveToken(body as { access_token: string; token_type?: string; expires_in?: number });
  const next = (body as { next?: unknown }).next;
  return { next: typeof next === "string" ? next : undefined };
}

export function getEmail(): string | null {
  try {
    return localStorage.getItem(USERNAME_KEY);
  } catch {
    return null;
  }
}

export async function forgotPassword(email: string): Promise<void> {
  const res = await fetch(`${API_BASE}/forgot-password`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Request failed: ${res.status} ${txt}`);
  }
}

export async function resetPassword(email: string, token: string, new_password: string): Promise<void> {
  const res = await fetch(`${API_BASE}/reset-password`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, token, new_password }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Password reset failed: ${res.status} ${txt}`);
  }
}

async function fetchWithAuth(input: RequestInfo, init: RequestInit = {}) {
  const token = getTokenRecord();
  if (!token) {
    throw new Error("Not authenticated");
  }
  const headers = new Headers(init.headers || {});
  headers.set("Authorization", `${token.token_type} ${token.access_token}`);
  const res = await fetch(input, { ...init, headers });
  if (res.status === 401) {
    // server says token invalid -> clear and notify caller
    logout();
    throw new Error("Unauthorized");
  }
  return res;
}

export async function fetchObject(survey: string, objectId: string): Promise<ApiObject> {
  const url = `${API_BASE}/surveys/${encodeURIComponent(survey)}/objects/${encodeURIComponent(objectId)}`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch object failed: ${res.status} ${txt}`);
  }
  // API responses sometimes wrap the payload in { data: ... }
  const body = await parseResponseJson(res).catch(() => ({}));
  return unwrapData<ApiObject>(body, {} as ApiObject);
}

/**
 * The profile as it arrives, tolerating either spelling of the id.
 *
 * `BabamulUserPublic` used to serialize the id as `_id`, which made a `Profile`
 * typed with `id` a claim about the wire that wasn't true. Nothing but
 * analytics read the field, so the `undefined` went unnoticed — it surfaced
 * only as `posthog.identify` falling back to the username, which split each
 * user into a web person and an API person. The API now sends `id`; `_id` is
 * still accepted so a browser holding a cached bundle across the deploy
 * doesn't quietly regress to that fallback.
 */
type ProfileWire = (Omit<NonNullable<Profile>, "id"> & { id?: string; _id?: string }) | null;

function normalizeProfile(wire: ProfileWire): Profile {
  if (!wire) return null;
  const { id, _id, ...rest } = wire;
  return { ...rest, id: id ?? _id ?? "" };
}

export async function fetchProfile(): Promise<Profile> {
  const url = `${API_BASE}/profile`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch profile failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({}));
  return normalizeProfile(unwrapData<ProfileWire>(body, null));
}

/**
 * Set or clear the account's display name.
 *
 * Pass an empty string to clear it — the name is free text the user controls,
 * not an identifier, so having none is a normal state rather than an error.
 */
export async function updateProfileName(name: string): Promise<Profile> {
  const url = `${API_BASE}/profile`;
  const res = await fetchWithAuth(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  const body = await parseResponseJson(res).catch(() => null);
  if (!res.ok) {
    const message =
      body && typeof body === "object" && "message" in body
        ? String((body as { message?: unknown }).message)
        : `Update profile failed: ${res.status}`;
    throw new Error(message);
  }
  return normalizeProfile(unwrapData<ProfileWire>(body, null));
}

export async function fetchKafkaCredentials(): Promise<KafkaCredential[]> {
  const url = `${API_BASE}/kafka-credentials`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch kafka credentials failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({ data: [] }));
  const result = unwrapData<unknown>(body, []);
  return Array.isArray(result) ? (result as KafkaCredential[]) : [];
}

export async function createKafkaCredential(name: string): Promise<KafkaCredential> {
  const url = `${API_BASE}/kafka-credentials`;
  const res = await fetchWithAuth(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Create kafka credential failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({}));
  if (body && typeof body === 'object' && 'credential' in body) {
    return (body as { credential: KafkaCredential }).credential;
  }
  return unwrapData<KafkaCredential>(body, {} as KafkaCredential);
}

export async function deleteKafkaCredential(credentials_id: string): Promise<void> {
  const url = `${API_BASE}/kafka-credentials/${encodeURIComponent(credentials_id)}`;
  const res = await fetchWithAuth(url, {
    method: "DELETE",
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Delete kafka credential failed: ${res.status} ${txt}`);
  }
  // no content expected
}

export async function fetchTokens(): Promise<TokenPublic[]> {
  const url = `${API_BASE}/tokens`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch tokens failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({ data: [] }));
  const result = unwrapData<unknown>(body, []);
  return Array.isArray(result) ? (result as TokenPublic[]) : [];
}

export async function createToken(name: string, expires_in_days?: number): Promise<TokenResponse> {
  const url = `${API_BASE}/tokens`;
  const res = await fetchWithAuth(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, expires_in_days }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Create token failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({}));
  return unwrapData<TokenResponse>(body, {} as TokenResponse);
}

export async function deleteToken(tokenId: string): Promise<void> {
  const url = `${API_BASE}/tokens/${encodeURIComponent(tokenId)}`;
  const res = await fetchWithAuth(url, {
    method: "DELETE",
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Delete token failed: ${res.status} ${txt}`);
  }
}

export type AlertSearchParams = {
  object_id?: string;
  ra?: number;
  dec?: number;
  radius_arcsec?: number;
  start_jd?: number;
  end_jd?: number;
  min_magpsf?: number;
  max_magpsf?: number;
  min_drb?: number;
  max_drb?: number;
  is_rock?: boolean;
  is_star?: boolean;
  is_near_brightstar?: boolean;
  is_stationary?: boolean;
  is_positive?: boolean;
  limit?: number;
  skip?: number;
};

export type Cutouts = {
  cutoutScience?: string;
  cutoutDifference?: string;
  cutoutTemplate?: string;
};

export type Alert = {
  objectId?: string;
  candid: string;
  jd: number;
  candidate: {
    jd: number;
    magpsf?: number;
    fid?: number;
    drb?: number;
    reliability?: number;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

export async function fetchAlerts(survey: string, params: AlertSearchParams): Promise<Alert[]> {
  const searchParams = new URLSearchParams();

  // Add all defined parameters to the query string
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null) {
      searchParams.append(key, String(value));
    }
  });

  const url = `${API_BASE}/surveys/${encodeURIComponent(survey)}/alerts?${searchParams.toString()}`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch alerts failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({ data: [] }));
  const result = unwrapData<unknown>(body, []);
  return Array.isArray(result) ? (result as Alert[]) : [];
}

export async function fetchAlertCutouts(survey: string, candid: string): Promise<Cutouts> {
  const url = `${API_BASE}/surveys/${encodeURIComponent(survey)}/cutouts?candid=${encodeURIComponent(`${candid}`)}`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch cutouts failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({}));
  const result = unwrapData<unknown>(body, {});
  return (typeof result === 'object' && result ? (result as Cutouts) : {} as Cutouts);
}

export async function fetchObjCutouts(survey: string, objectId: string): Promise<Cutouts> {
  const url = `${API_BASE}/surveys/${encodeURIComponent(survey)}/cutouts?objectId=${encodeURIComponent(objectId)}`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch cutouts failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({}));
  const result = unwrapData<unknown>(body, {});
  return (typeof result === 'object' && result ? (result as Cutouts) : {} as Cutouts);
}

export type NightlyStat = {
  date: string;
  ztf?: number;
  lsst?: number;
};

export async function fetchStats(startDate: string, endDate: string, survey?: string): Promise<NightlyStat[]> {
  const params = new URLSearchParams({ start_date: startDate, end_date: endDate });
  if (survey) params.set("survey", survey);
  const url = `${API_BASE}/stats/nightly?${params.toString()}`;
  const res = await fetch(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch stats failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({ data: [] }));
  const result = unwrapData<unknown>(body, []);
  return Array.isArray(result) ? (result as NightlyStat[]) : [];
}

export type TopicInfo = {
  name: string;
  n_alerts: number;
  retention_days: number;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AvroSchema = Record<string, any>;

export async function fetchSchema(survey: string): Promise<AvroSchema> {
  const url = `${API_BASE}/surveys/${encodeURIComponent(survey)}/schemas`;
  const res = await fetch(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch schema failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({}));
  return body as AvroSchema;
}

export async function fetchTopics(): Promise<TopicInfo[]> {
  const url = `${API_BASE}/stats/kafka`;
  const res = await fetch(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch topics failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({ data: [] }));
  const result = unwrapData<unknown>(body, []);
  return Array.isArray(result) ? (result as TopicInfo[]) : [];
}

export type CollectionEntry = {
  name: string;
  count?: number;
  size_bytes?: number;
};

export type CollectionStats = {
  n_collections: number;
  collections: CollectionEntry[];
};

export async function fetchCollectionStats(): Promise<CollectionStats> {
  const url = `${API_BASE}/stats/collections?count=true&size=true`;
  const res = await fetch(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch collection stats failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({}));
  return unwrapData<CollectionStats>(body, { n_collections: 0, collections: [] });
}

export type SearchResult = {
  objectId: string;
  ra: number;
  dec: number;
  survey: string;
};

export type SearchObjectsResponse = {
  results: SearchResult[];
  message?: string;
};

export async function searchObjects(value: string, limit: number = 10): Promise<SearchObjectsResponse> {
  const searchParams = new URLSearchParams({
    object_id: value,
    limit: String(limit),
  });

  const url = `${API_BASE}/objects?${searchParams.toString()}`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    // Gracefully handle 400 with message, error if there is no message
    if (res.status === 400) {
      const body = await parseResponseJson(res).catch(() => ({}));
      let message: string | undefined = undefined;
      if (body && typeof body === 'object' && 'message' in body) {
        const m = (body as { message?: unknown }).message;
        if (typeof m === 'string') message = m;
        return { results: [], message };
      }
      const txt = await res.text().catch(() => "");
      throw new Error(`Search objects failed: ${res.status} ${message ? message : txt}`);
    }
    const txt = await res.text().catch(() => "");
    throw new Error(`Search objects failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({ data: [] }));
  const result = unwrapData<unknown>(body, []);
  const results = Array.isArray(result) ? (result as SearchResult[]) : [];
  // If server returns a message field even on success, include it
  let message: string | undefined = undefined;
  if (body && typeof body === 'object' && 'message' in (body as Record<string, unknown>)) {
    const m = (body as { message?: unknown }).message;
    if (typeof m === 'string') message = m;
  }
  return { results, message };
}

export default {
  login,
  logout,
  getTokenRecord,
  fetchOAuthProviders,
  startOAuthLogin,
  saveOAuthToken,
  completeOAuthEmail,
  verifyOAuthEmail,
  forgotPassword,
  resetPassword,
  fetchObject,
  fetchProfile,
  updateProfileName,
  fetchAlerts,
  fetchAlertCutouts,
  fetchObjCutouts,
  fetchStats,
  fetchCollectionStats,
};
