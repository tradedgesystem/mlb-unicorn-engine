export type FetchJsonResult<T> = {
  ok: boolean;
  data?: T;
  error?: string;
  status?: number;
};

const DEFAULT_TIMEOUT_MS = 4000;

export async function fetchJson<T>(
  url: string,
  options: { timeoutMs?: number; fallback?: T; init?: RequestInit } = {}
): Promise<FetchJsonResult<T>> {
  const { timeoutMs = DEFAULT_TIMEOUT_MS, fallback, init } = options;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, { ...init, signal: controller.signal });
    const status = res.status;

    if (!res.ok) {
      return {
        ok: false,
        status,
        error: `HTTP ${status}`,
        data: fallback,
      };
    }

    const text = await res.text();
    if (!text) {
      return {
        ok: false,
        status,
        error: "Empty response body",
        data: fallback,
      };
    }

    try {
      return {
        ok: true,
        status,
        data: JSON.parse(text) as T,
      };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return {
        ok: false,
        status,
        error: `Invalid JSON: ${message}`,
        data: fallback,
      };
    }
  } catch (err) {
    if (err instanceof DOMException && err.name === "AbortError") {
      return {
        ok: false,
        error: `Timeout after ${timeoutMs}ms`,
        data: fallback,
      };
    }
    const message = err instanceof Error ? err.message : String(err);
    return {
      ok: false,
      error: `Network error: ${message}`,
      data: fallback,
    };
  } finally {
    clearTimeout(timeoutId);
  }
}

