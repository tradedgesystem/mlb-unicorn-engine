import { notFound } from "next/navigation";

export const dynamic = "force-dynamic";

export default function SentryTestPage({
  searchParams,
}: {
  searchParams?: Record<string, string | string[] | undefined>;
}) {
  const vercelEnv = process.env.VERCEL_ENV || "";
  const nodeEnv = process.env.NODE_ENV || "";
  const debug =
    (Array.isArray(searchParams?.debug) ? searchParams?.debug[0] : searchParams?.debug) === "1";

  const isProdDeploy = vercelEnv === "production";
  if (isProdDeploy) notFound();

  if (nodeEnv === "production" && !debug) notFound();

  throw new Error("Sentry test error (frontend)");
}
