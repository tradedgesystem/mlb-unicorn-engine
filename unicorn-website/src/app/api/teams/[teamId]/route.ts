import { proxyGet } from "../../_proxy";
import type { NextRequest } from "next/server";

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ teamId: string }> }
) {
  const { teamId } = await context.params;
  return proxyGet(request, `/api/teams/${encodeURIComponent(teamId)}`);
}
