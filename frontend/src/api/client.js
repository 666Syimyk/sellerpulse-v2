export const API_URL = import.meta.env.VITE_API_URL ?? (import.meta.env.DEV ? "http://localhost:8000" : "");

export function getToken() {
  return localStorage.getItem("sellerpulse_token");
}

export function setToken(token) {
  if (token) localStorage.setItem("sellerpulse_token", token);
  else localStorage.removeItem("sellerpulse_token");
}

export const subscriptionExpiredHandlers = new Set();

export async function api(path, options = {}) {
  const isFormData = options.body instanceof FormData;
  const headers = { ...(isFormData ? {} : { "Content-Type": "application/json" }), ...(options.headers || {}) };
  const token = getToken();
  if (token) headers.Authorization = `Bearer ${token}`;

  const response = await fetch(`${API_URL}${path}`, { ...options, headers });
  const payload = await response.json().catch(() => ({}));
  if (response.status === 402) {
    subscriptionExpiredHandlers.forEach((fn) => fn());
    throw new Error("subscription_expired");
  }
  if (!response.ok) throw new Error(readableError(payload.detail));
  return payload;
}

function readableError(detail) {
  if (typeof detail !== "string" || !detail) return "Ошибка запроса";
  const mojibakePattern = new RegExp(["\\u0420\\u045f", "\\u0420\\u040e", "\\u0420\\u045c", "\\u0421\\u0453", "\\u0421\\u040a", "\\u0432\\u0402"].join("|"));
  if (mojibakePattern.test(detail)) return "Ошибка запроса";
  return detail;
}
