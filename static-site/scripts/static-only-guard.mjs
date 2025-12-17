import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

async function readText(filePath) {
  return fs.readFile(filePath, "utf8");
}

function fail(message) {
  console.error(message);
  process.exitCode = 1;
}

function checkFile({ filePath, text }) {
  const disallowed = [
    { name: "absolute-url", regex: /\bhttps?:\/\//g },
    { name: "next-api-route", regex: /\/api\//g },
    { name: "backend-host-onrender", regex: /onrender\.com/g },
    { name: "backend-host-mlb-unicorn-engine", regex: /mlb-unicorn-engine/g },
  ];

  for (const rule of disallowed) {
    const match = rule.regex.exec(text);
    rule.regex.lastIndex = 0;
    if (match) {
      fail(`Static-only guard failed: ${rule.name} found in ${filePath}`);
      return;
    }
  }
}

async function main() {
  const thisFile = fileURLToPath(import.meta.url);
  const scriptsDir = path.dirname(thisFile);
  const repoRoot = path.resolve(scriptsDir, "..", "..");

  const runtimeFiles = [
    path.join(repoRoot, "unicorn-website", "justhtml", "assets", "site.js"),
    path.join(repoRoot, "unicorn-website", "justhtml", "build.mjs"),
  ];

  for (const filePath of runtimeFiles) {
    const text = await readText(filePath);
    checkFile({ filePath, text });
  }

  const siteJs = await readText(runtimeFiles[0]);
  if (!/const DATA_BASE\s*=\s*["']\/data\/latest["']\s*;/.test(siteJs)) {
    fail(`Static-only guard failed: DATA_BASE must be "/data/latest" in ${runtimeFiles[0]}`);
  }

  if (process.exitCode) {
    process.exit(process.exitCode);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exitCode = 1;
});

