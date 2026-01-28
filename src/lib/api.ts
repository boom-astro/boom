// Always use the same-origin proxy; production should map /api to the backend via the web server
const API_BASE = "/api/babamul";
// import JSONbig from 'json-bigint';
// import it with the native bigint support
// var JSONbig = require('json-bigint');
// would turn into var JSONbigNative = require('json-bigint')({ useNativeBigInt: true });

export type TokenRecord = {
  access_token: string;
  token_type: string;
  expires_at: number; // unix ms
};

// Generic API object shape for responses we don't have a stricter schema for
export type ApiObject = Record<string, unknown>;

// Profile shape returned by `/profile` endpoint (partial)
export type Profile = { username?: string; name?: string; email?: string; avatar?: string } | null;

// Kafka credential returned by `/babamul/kafka-credentials`
export type KafkaCredential = { id: string; name: string; kafka_username: string; kafka_password: string };

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

  // return data;
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

export function getEmail(): string | null {
  try {
    return localStorage.getItem(USERNAME_KEY);
  } catch {
    return null;
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

export async function fetchProfile(): Promise<Profile> {
  const url = `${API_BASE}/profile`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch profile failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch(() => ({}));
  return unwrapData<Profile>(body, null);
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
  min_sgscore1?: number;
  max_sgscore1?: number;
  min_distpsnr1?: number;
  max_distpsnr1?: number;
};

export type Cutouts = {
  cutoutScience?: string;
  cutoutDifference?: string;
  cutoutTemplate?: string;
};

export type Alert = {
  objectId?: string;
  candid: number;
  jd: number;
  candidate: {
    jd: number;
    magpsf?: number;
    fid?: number;
    drb?: number;
    reliability?: number;
    [key: string]: unknown;
  };
  magpsf?: number;
  drb?: number;
  reliability?: number;
  cutouts?: Cutouts;
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

export async function fetchAlertCutouts(survey: string, candid: number): Promise<Cutouts> {
  const url = `${API_BASE}/surveys/${encodeURIComponent(survey)}/alerts/${candid}/cutouts`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch cutouts failed: ${res.status} ${txt}`);
  }
  const body = await parseResponseJson(res).catch((_) => {
    return {};
  });
  const result = unwrapData<unknown>(body, {});
  return (typeof result === 'object' && result ? (result as Cutouts) : {} as Cutouts);
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
  fetchObject,
  fetchProfile,
  fetchKafkaCredentials,
  createKafkaCredential,
  fetchAlerts,
  fetchAlertCutouts,
  searchObjects,
};
