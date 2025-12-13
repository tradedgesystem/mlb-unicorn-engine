import type { NextConfig } from "next";
import { withSentryConfig } from "@sentry/nextjs";

const nextConfig: NextConfig = {
  /* config options here */
};

const authToken = process.env.SENTRY_AUTH_TOKEN;
const org = process.env.SENTRY_ORG;
const project = process.env.SENTRY_PROJECT;

const shouldUploadSourcemaps = Boolean(authToken && org && project);

export default withSentryConfig(nextConfig, {
  authToken,
  org,
  project,
  silent: true,
  disableLogger: true,
  sourcemaps: {
    disable: !shouldUploadSourcemaps,
    deleteSourcemapsAfterUpload: true,
  },
  release: {
    name: process.env.VERCEL_GIT_COMMIT_SHA || process.env.NEXT_PUBLIC_RELEASE || undefined,
  },
});
