const API_BASE = "/api/babamul";

export type TokenRecord = {
  access_token: string;
  token_type: string;
  expires_at: number; // unix ms
};

export type ApiObject = Record<string, unknown>;

export type Profile = {
  id?: string;
  username: string;
  email: string;
  created_at: number;
  name?: string;
  avatar?: string;
  /** Provider slugs the account can sign in with, e.g. ["google", "orcid"] */
  identity_providers?: string[];
  orcid_id?: string | null;
} | null;

export type OAuthProvider = { id: string; name: string; start_url: string };

export type KafkaCredential = { id: string; name: string; kafka_username: string; kafka_password: string };

export type TokenPublic = { id: string; name: string; created_at: number; expires_at: number; last_used_at: number | null };
export type TokenResponse = { id: string; name: string; access_token: string; created_at: number; expires_at: number };

export const TOKEN_KEY = "api_token";
export const USERNAME_KEY = "api_user";

async function parseResponseJson(res: Response): Promise<unknown> {
  const text = await res.text();
  // Quote 16+ digit integers: `JSON.parse` would round them and lose candid precision.
  const safeText = text.replace(/:\s*(-?\d{16,})(?![\d]*["])/g, (_, numStr) => `:"${numStr}"`);
  return JSON.parse(safeText);
}

type DataEnvelope<T> = { data?: T };

function unwrapData<T>(body: unknown, fallback: T): T {
  if (body && typeof body === "object" && "data" in body) {
    const dataVal = (body as DataEnvelope<unknown>).data;
    if (dataVal !== undefined) return dataVal as T;
  }
  return body === null || body === undefined ? fallback : (body as T);
}

async function ensureOk(res: Response, action: string): Promise<void> {
  if (res.ok) return;
  const text = await res.text().catch(() => "");
  throw new Error(`${action} failed: ${res.status} ${text}`);
}

async function readJson(res: Response): Promise<unknown> {
  return parseResponseJson(res).catch(() => null);
}

async function readList<T>(res: Response): Promise<T[]> {
  const result = unwrapData<unknown>(await readJson(res), []);
  return Array.isArray(result) ? (result as T[]) : [];
}

function messageOf(body: unknown): string | undefined {
  if (!body || typeof body !== "object" || !("message" in body)) return undefined;
  const message = (body as { message?: unknown }).message;
  return typeof message === "string" ? message : undefined;
}

type TokenBody = { access_token: string; token_type?: string; expires_in?: number };

function hasAccessToken(body: unknown): body is TokenBody {
  return !!body && typeof body === "object" && "access_token" in body;
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

function saveToken(body: TokenBody) {
  const expires_in = typeof body.expires_in === "number" ? body.expires_in : 3600;
  const rec: TokenRecord = {
    access_token: body.access_token,
    token_type: body.token_type || "Bearer",
    expires_at: Date.now() + expires_in * 1000,
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
    localStorage.removeItem(TOKEN_KEY);
    return null;
  }
  return t;
}

export async function login(email: string, password: string): Promise<TokenRecord | null> {
  const res = await fetch(`${API_BASE}/auth`, {
    method: "POST",
    // Form-encoded for compatibility with the OAuth2 password grant.
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ email, password }).toString(),
  });
  await ensureOk(res, "Login");
  const body = await parseResponseJson(res);
  if (!hasAccessToken(body)) throw new Error("Auth response missing access_token");
  saveToken(body);
  localStorage.setItem(USERNAME_KEY, email);
  return getTokenRecord();
}

export async function fetchOAuthProviders(): Promise<OAuthProvider[]> {
  const res = await fetch(`${API_BASE}/oauth/providers`);
  if (!res.ok) return [];
  return readList<OAuthProvider>(res);
}

export function startOAuthLogin(provider: OAuthProvider, redirectTo?: string) {
  const query = redirectTo ? `?${new URLSearchParams({ redirect_to: redirectTo })}` : "";
  // `start_url` is API-relative; API_BASE already carries the /api prefix.
  const path = provider.start_url.replace(/^\/babamul/, "");
  window.location.assign(`${API_BASE}${path}${query}`);
}

export function saveOAuthToken(params: TokenBody) {
  saveToken(params);
  return getTokenRecord();
}

/** No account exists until `verifyOAuthEmail` confirms the code this mails out. */
export async function completeOAuthEmail(ticket: string, email: string): Promise<void> {
  const res = await fetch(`${API_BASE}/oauth/complete`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ticket, email }),
  });
  if (res.ok) return;
  throw new Error(messageOf(await readJson(res)) ?? `Request failed: ${res.status}`);
}

export async function verifyOAuthEmail(ticket: string, code: string): Promise<{ next?: string }> {
  const res = await fetch(`${API_BASE}/oauth/verify`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ticket, code }),
  });
  const body = await readJson(res);
  if (!res.ok) throw new Error(messageOf(body) ?? `Confirmation failed: ${res.status}`);
  if (!hasAccessToken(body)) throw new Error("Confirmation response missing access_token");
  saveToken(body);
  const next = (body as { next?: unknown }).next;
  return { next: typeof next === "string" ? next : undefined };
}

export async function forgotPassword(email: string): Promise<void> {
  const res = await fetch(`${API_BASE}/forgot-password`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email }),
  });
  await ensureOk(res, "Request");
}

export async function resetPassword(email: string, token: string, new_password: string): Promise<void> {
  const res = await fetch(`${API_BASE}/reset-password`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, token, new_password }),
  });
  await ensureOk(res, "Password reset");
}

async function fetchWithAuth(input: RequestInfo, init: RequestInit = {}) {
  const token = getTokenRecord();
  if (!token) throw new Error("Not authenticated");
  const headers = new Headers(init.headers || {});
  headers.set("Authorization", `${token.token_type} ${token.access_token}`);
  const res = await fetch(input, { ...init, headers });
  if (res.status === 401) {
    logout();
    throw new Error("Unauthorized");
  }
  return res;
}

export async function fetchObject(survey: string, objectId: string): Promise<ApiObject> {
  const url = `${API_BASE}/surveys/${encodeURIComponent(survey)}/objects/${encodeURIComponent(objectId)}`;
  const res = await fetchWithAuth(url);
  await ensureOk(res, "Fetch object");
  return unwrapData<ApiObject>(await readJson(res), {});
}

type ProfileWire = (NonNullable<Profile> & { _id?: string }) | null;

function normalizeProfile(wire: ProfileWire): Profile {
  if (!wire || typeof wire.username !== "string" || typeof wire.email !== "string") return null;
  const { id, _id, ...rest } = wire;
  return { ...rest, id: id || _id || undefined };
}

export async function fetchProfile(): Promise<Profile> {
  const res = await fetchWithAuth(`${API_BASE}/profile`);
  await ensureOk(res, "Fetch profile");
  return normalizeProfile(unwrapData<ProfileWire>(await readJson(res), null));
}

/** Pass an empty string to clear the name: having none is a normal state. */
export async function updateProfileName(name: string): Promise<Profile> {
  const res = await fetchWithAuth(`${API_BASE}/profile`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  const body = await readJson(res);
  if (!res.ok) throw new Error(messageOf(body) ?? `Update profile failed: ${res.status}`);
  return normalizeProfile(unwrapData<ProfileWire>(body, null));
}

export async function fetchKafkaCredentials(): Promise<KafkaCredential[]> {
  const res = await fetchWithAuth(`${API_BASE}/kafka-credentials`);
  await ensureOk(res, "Fetch kafka credentials");
  return readList<KafkaCredential>(res);
}

export async function createKafkaCredential(name: string): Promise<KafkaCredential> {
  const res = await fetchWithAuth(`${API_BASE}/kafka-credentials`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  await ensureOk(res, "Create kafka credential");
  const body = await readJson(res);
  if (body && typeof body === "object" && "credential" in body) {
    return (body as { credential: KafkaCredential }).credential;
  }
  return unwrapData<KafkaCredential>(body, {} as KafkaCredential);
}

export async function deleteKafkaCredential(credentials_id: string): Promise<void> {
  const url = `${API_BASE}/kafka-credentials/${encodeURIComponent(credentials_id)}`;
  await ensureOk(await fetchWithAuth(url, { method: "DELETE" }), "Delete kafka credential");
}

export async function fetchTokens(): Promise<TokenPublic[]> {
  const res = await fetchWithAuth(`${API_BASE}/tokens`);
  await ensureOk(res, "Fetch tokens");
  return readList<TokenPublic>(res);
}

export async function createToken(name: string, expires_in_days?: number): Promise<TokenResponse> {
  const res = await fetchWithAuth(`${API_BASE}/tokens`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, expires_in_days }),
  });
  await ensureOk(res, "Create token");
  return unwrapData<TokenResponse>(await readJson(res), {} as TokenResponse);
}

export async function deleteToken(tokenId: string): Promise<void> {
  const url = `${API_BASE}/tokens/${encodeURIComponent(tokenId)}`;
  await ensureOk(await fetchWithAuth(url, { method: "DELETE" }), "Delete token");
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
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null) searchParams.append(key, String(value));
  });

  const url = `${API_BASE}/surveys/${encodeURIComponent(survey)}/alerts?${searchParams}`;
  const res = await fetchWithAuth(url);
  await ensureOk(res, "Fetch alerts");
  return readList<Alert>(res);
}

async function fetchCutouts(survey: string, query: string): Promise<Cutouts> {
  const url = `${API_BASE}/surveys/${encodeURIComponent(survey)}/cutouts?${query}`;
  const res = await fetchWithAuth(url);
  await ensureOk(res, "Fetch cutouts");
  const result = unwrapData<unknown>(await readJson(res), {});
  return typeof result === "object" && result ? (result as Cutouts) : {};
}

export function fetchAlertCutouts(survey: string, candid: string): Promise<Cutouts> {
  return fetchCutouts(survey, `candid=${encodeURIComponent(candid)}`);
}

export function fetchObjCutouts(survey: string, objectId: string): Promise<Cutouts> {
  return fetchCutouts(survey, `objectId=${encodeURIComponent(objectId)}`);
}

export type NightlyStat = {
  date: string;
  ztf?: number;
  lsst?: number;
};

export async function fetchStats(startDate: string, endDate: string, survey?: string): Promise<NightlyStat[]> {
  const params = new URLSearchParams({ start_date: startDate, end_date: endDate });
  if (survey) params.set("survey", survey);
  const res = await fetch(`${API_BASE}/stats/nightly?${params}`);
  await ensureOk(res, "Fetch stats");
  return readList<NightlyStat>(res);
}

export type TopicInfo = {
  name: string;
  n_alerts: number;
  retention_days: number;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AvroSchema = Record<string, any>;

export async function fetchSchema(survey: string): Promise<AvroSchema> {
  const res = await fetch(`${API_BASE}/surveys/${encodeURIComponent(survey)}/schemas`);
  await ensureOk(res, "Fetch schema");
  return ((await readJson(res)) ?? {}) as AvroSchema;
}

export async function fetchTopics(): Promise<TopicInfo[]> {
  const res = await fetch(`${API_BASE}/stats/kafka`);
  await ensureOk(res, "Fetch topics");
  return readList<TopicInfo>(res);
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
  const res = await fetch(`${API_BASE}/stats/collections?count=true&size=true`);
  await ensureOk(res, "Fetch collection stats");
  return unwrapData<CollectionStats>(await readJson(res), { n_collections: 0, collections: [] });
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
  const params = new URLSearchParams({ object_id: value, limit: String(limit) });
  const res = await fetchWithAuth(`${API_BASE}/objects?${params}`);
  if (res.status === 400) {
    const message = messageOf(await readJson(res));
    if (message) return { results: [], message };
    throw new Error(`Search objects failed: ${res.status}`);
  }
  await ensureOk(res, "Search objects");
  const body = await readJson(res);
  const result = unwrapData<unknown>(body, []);
  return {
    results: Array.isArray(result) ? (result as SearchResult[]) : [],
    message: messageOf(body),
  };
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
