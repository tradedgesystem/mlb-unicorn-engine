export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const DEFAULT_TIMEOUT_MS = 60_000;
const CACHE_CONTROL = "public, max-age=0, s-maxage=60, stale-while-revalidate=300";

export type ProxyGetOptions = {
  timeoutMs?: number;
  cacheMode?: "default" | "bypass";
};

type CacheEntry = {
  body: ArrayBuffer;
  contentType: string | null;
  cachedAt: number;
  ttlMs: number;
};

const CACHE = new Map<string, CacheEntry>();

function ttlMsForPath(pathname: string): number {
  if (pathname === "/api/teams" || pathname.startsWith("/api/teams/")) return 5 * 60_000;
  if (pathname.startsWith("/top50/")) return 60_000;
  if (pathname === "/api/players" || pathname.startsWith("/api/players/")) return 5 * 60_000;
  if (pathname.startsWith("/api/league-averages")) return 2 * 60_000;
  return 60_000;
}

function getBackendBase(): string | null {
  const raw = (process.env.BACKEND_API_BASE || "").trim();
  if (!raw) return null;
  try {
    // Validate URL early to avoid confusing runtime errors.
    new URL(raw);
    return raw;
  } catch {
    return null;
  }
}

function jsonError(
  status: number,
  payload: Record<string, unknown>,
  headers?: Record<string, string>
): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": CACHE_CONTROL,
      ...(headers || {}),
    },
  });
}

export async function proxyGet(
  request: Request,
  upstreamPath: string,
  options: ProxyGetOptions = {}
): Promise<Response> {
  const startedAt = Date.now();
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const backendBase = getBackendBase();
  if (!backendBase) {
    return jsonError(
      500,
      { error: "missing BACKEND_API_BASE", upstream: upstreamPath },
      {
        "x-proxy-upstream": upstreamPath,
        "x-proxy-error": "missing_env",
        "x-proxy-duration-ms": String(Date.now() - startedAt),
        "x-proxy-cache": "miss",
      }
    );
  }

  let upstreamUrl: URL;
  try {
    upstreamUrl = new URL(upstreamPath, backendBase);
  } catch {
    return jsonError(
      500,
      { error: "invalid backend proxy URL", upstream: upstreamPath },
      {
        "x-proxy-upstream": upstreamPath,
        "x-proxy-error": "fetch_error",
        "x-proxy-duration-ms": String(Date.now() - startedAt),
        "x-proxy-cache": "miss",
      }
    );
  }

  const requestUrl = new URL(request.url);
  upstreamUrl.search = requestUrl.search;
  const upstreamPathOnly = `${upstreamUrl.pathname}${upstreamUrl.search}`;
  const cacheKey = upstreamPathOnly;
  const ttlMs = ttlMsForPath(upstreamUrl.pathname);
  const cached = CACHE.get(cacheKey);
  const allowCacheHit = options.cacheMode !== "bypass";
  if (allowCacheHit && cached && Date.now() - cached.cachedAt <= cached.ttlMs) {
    return new Response(cached.body, {
      status: 200,
      headers: {
        "content-type": cached.contentType || "application/json; charset=utf-8",
        "cache-control": CACHE_CONTROL,
        "x-proxy-upstream": upstreamPathOnly,
        "x-proxy-duration-ms": String(Date.now() - startedAt),
        "x-proxy-cache": "hit",
      },
    });
  }

  const controller = new AbortController();
  if (request.signal) {
    if (request.signal.aborted) {
      controller.abort();
    } else {
      request.signal.addEventListener("abort", () => controller.abort(), { once: true });
    }
  }
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const accept = request.headers.get("accept");
    const res = await fetch(upstreamUrl, {
      method: "GET",
      headers: accept ? { accept } : undefined,
      signal: controller.signal,
      cache: "no-store",
    });

    const headers = new Headers(res.headers);
    headers.set("cache-control", CACHE_CONTROL);
    headers.set("x-proxy-upstream", upstreamPathOnly);
    headers.set("x-proxy-duration-ms", String(Date.now() - startedAt));
    headers.set("x-proxy-cache", "miss");

    const body = await res.arrayBuffer();
    const bodyBytes = body.slice(0);
    if (res.status === 200) {
      CACHE.set(cacheKey, {
        body: bodyBytes,
        contentType: headers.get("content-type"),
        cachedAt: Date.now(),
        ttlMs,
      });
    }

    return new Response(bodyBytes, {
      status: res.status,
      statusText: res.statusText,
      headers,
    });
  } catch (err) {
    const durationMs = Date.now() - startedAt;
    if (err instanceof DOMException && err.name === "AbortError") {
      if (cached) {
        return new Response(cached.body, {
          status: 200,
          headers: {
            "content-type": cached.contentType || "application/json; charset=utf-8",
            "cache-control": CACHE_CONTROL,
            "x-proxy-upstream": upstreamPathOnly,
            "x-proxy-duration-ms": String(durationMs),
            "x-proxy-cache": "stale",
            "x-proxy-error": "upstream_timeout",
          },
        });
      }
      return jsonError(
        504,
        {
          error: "upstream timeout",
          upstream: upstreamPathOnly,
          timeout_ms: timeoutMs,
        },
        {
          "x-proxy-upstream": upstreamPathOnly,
          "x-proxy-duration-ms": String(durationMs),
          "x-proxy-error": "timeout",
          "x-proxy-cache": "miss",
        }
      );
    }
    if (cached) {
      return new Response(cached.body, {
        status: 200,
        headers: {
          "content-type": cached.contentType || "application/json; charset=utf-8",
          "cache-control": CACHE_CONTROL,
          "x-proxy-upstream": upstreamPathOnly,
          "x-proxy-duration-ms": String(durationMs),
          "x-proxy-cache": "stale",
          "x-proxy-error": "fetch_error",
        },
      });
    }
    return jsonError(
      502,
      { error: "upstream fetch failed", upstream: upstreamPathOnly },
      {
        "x-proxy-upstream": upstreamPathOnly,
        "x-proxy-duration-ms": String(durationMs),
        "x-proxy-error": "fetch_error",
        "x-proxy-cache": "miss",
      }
    );
  } finally {
    clearTimeout(timeoutId);
  }
}
