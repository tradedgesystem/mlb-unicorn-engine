"use client";

import { useEffect, useState } from "react";
import { TopTable } from "../components/TopTable";

export default function Home() {
  const [mounted, setMounted] = useState(false);

  useEffect(() => setMounted(true), []);

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <p className="text-sm text-neutral-500">MLB Unicorn Engine</p>
          <h1 className="text-3xl font-semibold text-neutral-900">Unicorn Top 50</h1>
        </div>
        {mounted && (
          <div className="text-xs text-neutral-500">
            Live data from mlb-unicorn-engine.onrender.com
          </div>
        )}
      </div>
      <TopTable />
    </main>
  );
}
