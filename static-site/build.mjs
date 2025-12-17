import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

async function exists(p) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

async function rmrf(p) {
  await fs.rm(p, { recursive: true, force: true });
}

async function mkdirp(p) {
  await fs.mkdir(p, { recursive: true });
}

async function copyDir(src, dst) {
  await mkdirp(dst);
  // Node 20+: fs.cp exists.
  await fs.cp(src, dst, { recursive: true });
}

function run(cmd, args, { cwd }) {
  const result = spawnSync(cmd, args, { stdio: "inherit", cwd, env: process.env });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Command failed: ${cmd} ${args.join(" ")}`);
}

async function main() {
  const thisFile = fileURLToPath(import.meta.url);
  const staticDir = path.dirname(thisFile);
  const repoRoot = path.resolve(staticDir, "..");

  const dataRoot = path.join(repoRoot, "unicorn-website", "public", "data");
  const dataLatest = path.join(dataRoot, "latest");
  const required = ["meta.json", "teams.json", "unicorns.json", "players_index.json"].map((f) => path.join(dataLatest, f));
  for (const file of required) {
    if (!(await exists(file))) {
      throw new Error(
        `Missing required data file: ${file}\n` +
          `The nightly GitHub Action should publish these into the repo at unicorn-website/public/data/latest/.`,
      );
    }
  }

  const outDir = path.join(staticDir, "dist");
  await rmrf(outDir);

  // Build the HTML/CSS/JS tree (file-based routes) into static-site/dist/.
  run("node", [
    path.join(repoRoot, "unicorn-website", "justhtml", "build.mjs"),
    "--data-latest",
    path.join("unicorn-website", "public", "data", "latest"),
    "--out",
    path.join("static-site", "dist"),
  ], { cwd: repoRoot });

  // Copy the published data product into the deployment output so it can be fetched via same-origin /data/latest/...
  await copyDir(dataRoot, path.join(outDir, "data"));

  console.log(`Static site ready at: ${outDir}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exitCode = 1;
});

