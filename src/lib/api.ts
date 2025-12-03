// Use /api prefix in dev so Vite's dev server proxy can forward requests to the backend.
// In production you can set `VITE_API_BASE` to point to your API (for example an absolute URL).
const API_BASE: string = import.meta.env.DEV ? "/api" : (import.meta.env.VITE_API_BASE ?? "");

export type TokenRecord = {
  access_token: string;
  token_type: string;
  expires_at: number; // unix ms
};

// Generic API object shape for responses we don't have a stricter schema for
export type ApiObject = Record<string, unknown>;

// Profile shape returned by `/profile` endpoint (partial)
export type Profile = { username?: string; name?: string; email?: string; avatar?: string } | null;

const TOKEN_KEY = "api_token";
const USERNAME_KEY = "api_user";

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

export async function login(username: string, password: string): Promise<TokenRecord | null> {
  const res = await fetch(`${API_BASE}/auth`, {
    method: "POST",
    // headers: { "Content-Type": "application/json" },
    // use form-encoded for compatibility with OAuth2 password grant
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ username, password }).toString(),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Login failed: ${res.status} ${txt}`);
  }
  const body = await res.json();
  if (!body.access_token) throw new Error("Auth response missing access_token");
  saveToken(body);
  try {
    localStorage.setItem(USERNAME_KEY, username);
  } catch {
    // ignore
  }
  return getTokenRecord();
}

export function getUsername(): string | null {
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
  const body = await res.json().catch(() => ({}));
  return (body && (body.data ?? body)) as ApiObject;
}

export async function fetchProfile(): Promise<Profile> {
  const url = `${API_BASE}/profile`;
  const res = await fetchWithAuth(url);
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Fetch profile failed: ${res.status} ${txt}`);
  }
  const body = await res.json().catch(() => ({}));
  return (body && (body.data ?? body)) as Profile;
}

export default {
  login,
  logout,
  getTokenRecord,
  fetchObject,
  fetchProfile,
};
