"use client";

import Link from "next/link";
import { useEffect } from "react";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("Global error boundary", error);
  }, [error]);

  return (
    <main className="min-h-screen px-4 py-10 sm:px-8 lg:px-16 space-y-6">
      <div className="glass p-6 space-y-3">
        <h1 className="text-2xl font-semibold text-neutral-900">Something went wrong</h1>
        <p className="text-sm text-neutral-700">
          An unexpected error occurred. You can reload or go back home.
        </p>
        <div className="flex flex-wrap gap-2">
          <button
            onClick={() => reset()}
            className="glass px-4 py-2 text-sm text-neutral-900 hover:bg-neutral-200"
          >
            Reload
          </button>
          <Link
            href="/"
            className="glass px-4 py-2 text-sm text-neutral-900 hover:bg-neutral-200"
          >
            Home
          </Link>
        </div>
      </div>
    </main>
  );
}

