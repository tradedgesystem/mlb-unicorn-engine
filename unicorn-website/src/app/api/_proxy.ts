export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const DEFAULT_TIMEOUT_MS = 25_000;
const CACHE_CONTROL = "public, max-age=0, s-maxage=60, stale-while-revalidate=300";

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

export async function proxyGet(request: Request, upstreamPath: string): Promise<Response> {
  const startedAt = Date.now();
  const backendBase = getBackendBase();
  if (!backendBase) {
    return jsonError(
      500,
      { error: "missing BACKEND_API_BASE", upstream: upstreamPath },
      {
        "x-proxy-upstream": upstreamPath,
        "x-proxy-error": "missing_env",
        "x-proxy-duration-ms": String(Date.now() - startedAt),
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
      }
    );
  }

  const requestUrl = new URL(request.url);
  upstreamUrl.search = requestUrl.search;
  const upstreamPathOnly = `${upstreamUrl.pathname}${upstreamUrl.search}`;

  const controller = new AbortController();
  if (request.signal) {
    if (request.signal.aborted) {
      controller.abort();
    } else {
      request.signal.addEventListener("abort", () => controller.abort(), { once: true });
    }
  }
  const timeoutId = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);

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

    const body = await res.arrayBuffer();
    return new Response(body, {
      status: res.status,
      statusText: res.statusText,
      headers,
    });
  } catch (err) {
    const durationMs = Date.now() - startedAt;
    if (err instanceof DOMException && err.name === "AbortError") {
      return jsonError(
        504,
        {
          error: "upstream timeout",
          upstream: upstreamPathOnly,
          timeout_ms: DEFAULT_TIMEOUT_MS,
        },
        {
          "x-proxy-upstream": upstreamPathOnly,
          "x-proxy-duration-ms": String(durationMs),
          "x-proxy-error": "timeout",
        }
      );
    }
    return jsonError(
      502,
      { error: "upstream fetch failed", upstream: upstreamPathOnly },
      {
        "x-proxy-upstream": upstreamPathOnly,
        "x-proxy-duration-ms": String(durationMs),
        "x-proxy-error": "fetch_error",
      }
    );
  } finally {
    clearTimeout(timeoutId);
  }
}
