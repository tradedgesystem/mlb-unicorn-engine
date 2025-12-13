const DEFAULT_TIMEOUT_MS = 10_000;
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

function jsonError(status: number, message: string): Response {
  return new Response(JSON.stringify({ detail: message }), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": CACHE_CONTROL,
    },
  });
}

export async function proxyGet(request: Request, upstreamPath: string): Promise<Response> {
  const backendBase = getBackendBase();
  if (!backendBase) {
    return jsonError(500, "Missing BACKEND_API_BASE");
  }

  let upstreamUrl: URL;
  try {
    upstreamUrl = new URL(upstreamPath, backendBase);
  } catch {
    return jsonError(500, "Invalid backend proxy URL");
  }

  const requestUrl = new URL(request.url);
  upstreamUrl.search = requestUrl.search;

  const controller = new AbortController();
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

    return new Response(res.body, {
      status: res.status,
      statusText: res.statusText,
      headers,
    });
  } catch (err) {
    if (err instanceof DOMException && err.name === "AbortError") {
      return jsonError(504, "Upstream timeout");
    }
    return jsonError(502, "Upstream fetch failed");
  } finally {
    clearTimeout(timeoutId);
  }
}
