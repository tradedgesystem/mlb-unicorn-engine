export const runtime = "nodejs";
export const dynamic = "force-dynamic";

// Intentionally non-fatal: this endpoint should return 200 even if the backend is cold/down,
// so scheduled warm pings don't fail noisily. It should only fail (5xx) for real misconfig.

function json(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function getBackendBase(): string | null {
  const raw = (process.env.BACKEND_API_BASE || "").trim();
  if (!raw) return null;
  const base = raw.replace(/\/+$/, "");
  try {
    new URL(base);
    return base;
  } catch {
    return null;
  }
}

export async function GET() {
  const startedAt = Date.now();
  const base = getBackendBase();
  if (!base) {
    return json({ ok: false, error: "BACKEND_API_BASE missing" }, 500);
  }

  const controller = new AbortController();
  const timeoutMs = 10_000;
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(`${base}/api/teams`, {
      cache: "no-store",
      signal: controller.signal,
    });
    const durationMs = Date.now() - startedAt;

    if (!resp.ok) {
      return json(
        {
          ok: true,
          upstreamOk: false,
          status: resp.status,
          error: "upstream non-2xx",
          duration_ms: durationMs,
        },
        200
      );
    }
    return json({ ok: true, upstreamOk: true, status: resp.status, duration_ms: durationMs }, 200);
  } catch (err) {
    const durationMs = Date.now() - startedAt;
    const isTimeout = (err as { name?: string } | null)?.name === "AbortError";
    return json(
      {
        ok: true,
        upstreamOk: false,
        status: null,
        error: isTimeout ? "upstream timeout" : "upstream fetch error",
        duration_ms: durationMs,
      },
      200
    );
  } finally {
    clearTimeout(timeoutId);
  }
}
