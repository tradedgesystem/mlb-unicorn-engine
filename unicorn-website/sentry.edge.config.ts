import * as Sentry from "@sentry/nextjs";

const environment =
  process.env.SENTRY_ENVIRONMENT || process.env.VERCEL_ENV || process.env.NODE_ENV;

const isProduction = environment === "production";
const dsn = (process.env.NEXT_PUBLIC_SENTRY_DSN || "").trim() || undefined;

Sentry.init({
  dsn,
  environment,
  release: process.env.VERCEL_GIT_COMMIT_SHA || process.env.NEXT_PUBLIC_RELEASE || "dev",
  tracesSampleRate: isProduction ? 0.1 : 1.0,
});

