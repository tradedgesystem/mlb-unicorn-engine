import { proxyGet } from "../_proxy";
import type { NextRequest } from "next/server";

export async function GET(request: NextRequest) {
  return proxyGet(request, "/api/teams");
}
