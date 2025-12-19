import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

function parseArgs(argv) {
  const args = {
    dataLatestDir: null,
    outDir: null,
    clean: true,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const value = argv[i + 1];
    if (key === "--data-latest" && value) {
      args.dataLatestDir = value;
      i += 1;
      continue;
    }
    if (key === "--out" && value) {
      args.outDir = value;
      i += 1;
      continue;
    }
    if (key === "--no-clean") {
      args.clean = false;
      continue;
    }
    if (key === "--help" || key === "-h") {
      args.help = true;
      continue;
    }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

async function rmrf(targetPath) {
  await fs.rm(targetPath, { recursive: true, force: true });
}

async function mkdirp(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  try {
    return JSON.parse(raw);
  } catch (err) {
    throw new Error(`Invalid JSON: ${filePath} (${err?.message || err})`);
  }
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function pageTemplate({ title, bodyAttrs, mainHtml }) {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)}</title>
    <link rel="stylesheet" href="/assets/site.css" />
  </head>
  <body ${bodyAttrs}>
    <div class="app">
      <aside class="sidebar">
        <a class="brand" href="/">Unicorn Engine</a>
        <div class="last-updated">
          Last updated: <span id="last-updated">Loading…</span>
        </div>

        <div class="search">
          <label class="label" for="player-search">Player search</label>
          <input id="player-search" class="input" type="search" placeholder="Search players…" autocomplete="off" />
          <div id="search-status" class="hint" aria-live="polite"></div>
          <div id="search-results" class="search-results" role="listbox" aria-label="Search results"></div>
        </div>

        <nav class="teams-nav" aria-label="Teams">
          <div class="nav-title">Teams</div>
          <div id="teams-list" class="teams-list">Loading…</div>
        </nav>
      </aside>

      <main class="main">
        <div id="page-status" class="page-status" aria-live="polite"></div>
        ${mainHtml}
      </main>
    </div>

    <script type="module" src="/assets/site.js"></script>
  </body>
</html>
`;
}

function homePage() {
  return pageTemplate({
    title: "Teams",
    bodyAttrs: 'data-page="home"',
    mainHtml: `
<header class="header">
  <h1>Teams</h1>
  <p class="subhead">All 30 MLB teams.</p>
</header>
<section>
  <div id="teams-home" class="list">Loading…</div>
</section>
`,
  });
}

function teamsIndexPage() {
  return pageTemplate({
    title: "Teams",
    bodyAttrs: 'data-page="teams-index"',
    mainHtml: `
<header class="header">
  <h1>Teams</h1>
  <p class="subhead">All 30 MLB teams.</p>
</header>
<section>
  <div id="teams" class="list">Loading…</div>
</section>
`,
  });
}

function teamPage(teamId) {
  return pageTemplate({
    title: `Team ${teamId}`,
    bodyAttrs: `data-page="team" data-team-id="${escapeHtml(teamId)}"`,
    mainHtml: `
<header class="header">
  <h1 id="team-title">Team ${escapeHtml(teamId)}</h1>
  <p id="team-subtitle" class="subhead">Roster</p>
</header>

<section class="tabs" aria-label="Roster groups">
  <button class="tab active" data-tab="hitters" type="button">Hitters</button>
  <button class="tab" data-tab="starters" type="button">Starters</button>
  <button class="tab" data-tab="relievers" type="button">Relievers</button>
</section>

<section class="tabpanes">
  <div class="tabpane active" data-pane="hitters"><div id="roster-hitters" class="list">Loading…</div></div>
  <div class="tabpane" data-pane="starters"><div id="roster-starters" class="list">Loading…</div></div>
  <div class="tabpane" data-pane="relievers"><div id="roster-relievers" class="list">Loading…</div></div>
</section>
`,
  });
}

function playerPage(playerId) {
  return pageTemplate({
    title: `Player ${playerId}`,
    bodyAttrs: `data-page="player" data-player-id="${escapeHtml(playerId)}"`,
    mainHtml: `
<header class="header">
  <h1 id="player-title">Player ${escapeHtml(playerId)}</h1>
  <p id="player-subtitle" class="subhead">Loading…</p>
  <div id="player-actions" class="actions"></div>
</header>

<section class="grid">
  <div class="panel">
    <h2>Roles</h2>
    <div id="player-roles" class="chips">Loading…</div>
  </div>
  <div class="panel">
    <h2>Predictive stats</h2>
    <div id="player-metrics" class="metrics-host">Loading…</div>
  </div>
</section>

<section class="panel">
  <h2>Recent unicorns</h2>
  <div id="player-recent" class="list">Loading…</div>
</section>
`,
  });
}

function notFoundPage() {
  return pageTemplate({
    title: "Not Found",
    bodyAttrs: 'data-page="not-found"',
    mainHtml: `
<header class="header">
  <h1>Not found</h1>
  <p class="subhead">That page doesn’t exist.</p>
  <div class="actions">
    <a class="action" href="/">Go to home</a>
    <a class="action" href="/teams/">Browse teams</a>
  </div>
</header>
`,
  });
}

function healthPage() {
  return pageTemplate({
    title: "Health",
    bodyAttrs: 'data-page="health"',
    mainHtml: `
<header class="header">
  <h1>OK</h1>
  <p class="subhead">Static site is serving files. Data freshness is shown in the sidebar.</p>
</header>
`,
  });
}

async function copyAssets({ srcDir, outDir }) {
  await mkdirp(path.join(outDir, "assets"));
  const files = await fs.readdir(srcDir);
  for (const file of files) {
    const src = path.join(srcDir, file);
    const dst = path.join(outDir, "assets", file);
    const stat = await fs.stat(src);
    if (!stat.isFile()) continue;
    await fs.copyFile(src, dst);
  }
}

async function writeFile(filePath, contents) {
  await mkdirp(path.dirname(filePath));
  await fs.writeFile(filePath, contents, "utf8");
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help) {
    console.log(`Usage: node unicorn-website/justhtml/build.mjs [--data-latest PATH] [--out PATH] [--no-clean]

Defaults:
  --data-latest unicorn-website/public/data/latest
  --out         dist
`);
    return;
  }

  const thisFile = fileURLToPath(import.meta.url);
  const thisDir = path.dirname(thisFile);
  const websiteDir = path.resolve(thisDir, "..");
  const repoRoot = path.resolve(websiteDir, "..");

  const dataLatestDir = path.resolve(
    repoRoot,
    args.dataLatestDir ?? path.join("unicorn-website", "public", "data", "latest"),
  );
  const outDir = path.resolve(repoRoot, args.outDir ?? "dist");

  const required = ["meta.json", "teams.json", "players_index.json"];
  for (const f of required) {
    const p = path.join(dataLatestDir, f);
    try {
      await fs.access(p);
    } catch {
      throw new Error(
        `Missing required data file: ${p}\n` +
          `Run the nightly workflow, or generate locally:\n` +
          `.venv/bin/python -m backend.app.tools.generate_site_data_product --data-root unicorn-website/public/data`,
      );
    }
  }

  const teams = await readJson(path.join(dataLatestDir, "teams.json"));
  const playersIndex = await readJson(path.join(dataLatestDir, "players_index.json"));

  const teamIds = Array.isArray(teams) ? teams.map((t) => String(t.team_id)).filter(Boolean) : [];
  const playerIds = Array.isArray(playersIndex)
    ? playersIndex.map((p) => String(p.player_id)).filter(Boolean)
    : [];

  if (teamIds.length !== 30) {
    throw new Error(`Expected 30 teams in ${path.join(dataLatestDir, "teams.json")} (got ${teamIds.length})`);
  }

  if (args.clean) {
    await rmrf(outDir);
  }

  await mkdirp(outDir);
  await copyAssets({ srcDir: path.join(thisDir, "assets"), outDir });

  await writeFile(path.join(outDir, "index.html"), homePage());
  await writeFile(path.join(outDir, "404.html"), notFoundPage());
  await writeFile(path.join(outDir, "teams", "index.html"), teamsIndexPage());
  await writeFile(path.join(outDir, "health", "index.html"), healthPage());

  for (const teamId of teamIds) {
    await writeFile(path.join(outDir, "teams", teamId, "index.html"), teamPage(teamId));
  }

  for (const playerId of playerIds) {
    await writeFile(path.join(outDir, "players", playerId, "index.html"), playerPage(playerId));
  }

  await writeFile(
    path.join(outDir, "players", "index.html"),
    pageTemplate({
      title: "Players",
      bodyAttrs: 'data-page="players-index"',
      mainHtml: `
<header class="header">
  <h1>Players</h1>
  <p class="subhead">Use the search box in the sidebar.</p>
</header>
`,
    }),
  );

  console.log(`JustHTML site built to: ${outDir}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exitCode = 1;
});
