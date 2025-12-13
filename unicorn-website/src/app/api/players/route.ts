import { proxyGet } from "../_proxy";
import type { NextRequest } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  return proxyGet(request, "/api/players");
}
