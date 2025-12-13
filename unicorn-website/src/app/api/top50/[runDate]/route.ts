import { proxyGet } from "../../_proxy";
import type { NextRequest } from "next/server";

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ runDate: string }> }
) {
  const { runDate } = await context.params;
  return proxyGet(request, `/top50/${encodeURIComponent(runDate)}`);
}
