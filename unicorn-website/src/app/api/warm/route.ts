import type { NextRequest } from "next/server";

import { proxyGet } from "../_proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type CheckResult = {
  upstream: string;
  status: number;
  proxy_cache: string | null;
  proxy_error: string | null;
  proxy_duration_ms: number | null;
};

async function runCheck(request: NextRequest, upstream: string): Promise<CheckResult> {
  const resp = await proxyGet(request, upstream, { timeoutMs: 60_000, cacheMode: "bypass" });
  const durationHeader = resp.headers.get("x-proxy-duration-ms");
  return {
    upstream,
    status: resp.status,
    proxy_cache: resp.headers.get("x-proxy-cache"),
    proxy_error: resp.headers.get("x-proxy-error"),
    proxy_duration_ms: durationHeader ? Number(durationHeader) : null,
  };
}

export async function GET(request: NextRequest) {
  const [teams119, top50] = await Promise.all([
    runCheck(request, "/api/teams/119"),
    runCheck(request, "/top50/2025-03-27"),
  ]);

  const ok = teams119.status === 200 && top50.status === 200;

  return new Response(
    JSON.stringify({
      ok,
      checks: {
        teams_119: teams119,
        top50_2025_03_27: top50,
      },
      at: new Date().toISOString(),
    }),
    {
      status: 200,
      headers: {
        "content-type": "application/json; charset=utf-8",
        "cache-control": "no-store",
      },
    }
  );
}

