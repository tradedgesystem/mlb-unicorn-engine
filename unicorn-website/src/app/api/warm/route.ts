export const runtime = "nodejs";
export const dynamic = "force-dynamic";

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
  return raw.replace(/\/+$/, "");
}

export async function GET() {
  const base = getBackendBase();
  if (!base) {
    return json({ ok: false, error: "BACKEND_API_BASE missing" }, 500);
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 25_000);
  try {
    const resp = await fetch(`${base}/api/teams`, {
      cache: "no-store",
      signal: controller.signal,
    });

    if (!resp.ok) {
      return json({ ok: false, status: resp.status }, 502);
    }
    return json({ ok: true, status: resp.status }, 200);
  } catch {
    return json({ ok: false, status: 0 }, 502);
  } finally {
    clearTimeout(timeoutId);
  }
}
