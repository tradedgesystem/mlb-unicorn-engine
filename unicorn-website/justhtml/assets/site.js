const DATA_BASE = "/data/latest";
const METRIC_COLUMNS = {
  hitter: [
    { key: "barrel_pct_last_50", label: "Barrel%", kind: "pct" },
    { key: "hard_hit_pct_last_50", label: "HardHit%", kind: "pct" },
    { key: "xwoba_last_50", label: "xwOBA", kind: "dec3" },
    { key: "contact_pct_last_50", label: "Contact%", kind: "pct" },
    { key: "chase_pct_last_50", label: "Chase%", kind: "pct" },
  ],
  starter: [
    { key: "xwoba_last_3_starts", label: "xwOBA", kind: "dec3" },
    { key: "whiff_pct_last_3_starts", label: "Whiff%", kind: "pct" },
    { key: "k_pct_last_3_starts", label: "K%", kind: "pct" },
    { key: "bb_pct_last_3_starts", label: "BB%", kind: "pct" },
    { key: "hard_hit_pct_last_3_starts", label: "HardHit%", kind: "pct" },
  ],
  reliever: [
    { key: "xwoba_last_5_apps", label: "xwOBA", kind: "dec3" },
    { key: "whiff_pct_last_5_apps", label: "Whiff%", kind: "pct" },
    { key: "k_pct_last_5_apps", label: "K%", kind: "pct" },
    { key: "bb_pct_last_5_apps", label: "BB%", kind: "pct" },
    { key: "hard_hit_pct_last_5_apps", label: "HardHit%", kind: "pct" },
  ],
};

const DIVISIONS = {
  AL: {
    East: ["BAL", "BOS", "NYY", "TB", "TOR"],
    Central: ["CLE", "CWS", "DET", "KC", "MIN"],
    West: ["HOU", "LAA", "SEA", "TEX", "ATH"],
  },
  NL: {
    East: ["ATL", "MIA", "NYM", "PHI", "WSH"],
    Central: ["CHC", "CIN", "MIL", "PIT", "STL"],
    West: ["AZ", "COL", "LAD", "SD", "SF"],
  },
};

const TEAM_COLORS = {
  ATH: { primary: "#006D4D" },
  ATL: { primary: "#CE1141" },
  AZ: { primary: "#A71930" },
  BAL: { primary: "#DF4601" },
  BOS: { primary: "#BD3039" },
  CHC: { primary: "#0E3386" },
  CIN: { primary: "#C6011F" },
  CLE: { primary: "#0C2340" },
  COL: { primary: "#33006F" },
  CWS: { primary: "#27251F" },
  DET: { primary: "#0C2340" },
  HOU: { primary: "#EB6E1F" },
  KC: { primary: "#004687" },
  LAA: { primary: "#BA0021" },
  LAD: { primary: "#005A9C" },
  MIA: { primary: "#00A3E0" },
  MIL: { primary: "#12284B" },
  MIN: { primary: "#002B5C" },
  NYM: { primary: "#002D72" },
  NYY: { primary: "#0C2340" },
  PHI: { primary: "#E81828" },
  PIT: { primary: "#FDB827" },
  SD: { primary: "#2F241D" },
  SEA: { primary: "#0C2C56" },
  SF: { primary: "#FD5A1E" },
  STL: { primary: "#C41E3A" },
  TB: { primary: "#092C5C" },
  TEX: { primary: "#003278" },
  TOR: { primary: "#134A8E" },
  WSH: { primary: "#AB0003" },
};

function hexToRgb(hex) {
  const raw = String(hex || "").replace("#", "");
  if (raw.length !== 6) return null;
  const r = parseInt(raw.slice(0, 2), 16);
  const g = parseInt(raw.slice(2, 4), 16);
  const b = parseInt(raw.slice(4, 6), 16);
  if ([r, g, b].some((v) => Number.isNaN(v))) return null;
  return { r, g, b };
}

function applyTeamTheme(abbr) {
  const theme = TEAM_COLORS[String(abbr || "").toUpperCase()];
  const root = document.documentElement;
  if (!theme || !theme.primary) return;
  const rgb = hexToRgb(theme.primary);
  if (!rgb) return;
  document.body.classList.add("team-theme");
  root.style.setProperty("--team-primary", theme.primary);
  root.style.setProperty("--team-primary-10", `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.12)`);
  root.style.setProperty("--team-primary-20", `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.2)`);
}

function clearTeamTheme() {
  const root = document.documentElement;
  document.body.classList.remove("team-theme");
  root.style.removeProperty("--team-primary");
  root.style.removeProperty("--team-primary-10");
  root.style.removeProperty("--team-primary-20");
}

function $(selector) {
  return document.querySelector(selector);
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function showStatus(message) {
  const el = $("#page-status");
  if (!el) return;
  el.textContent = message;
  el.classList.add("show");
}

function clearStatus() {
  const el = $("#page-status");
  if (!el) return;
  el.textContent = "";
  el.classList.remove("show");
}

async function fetchJson(url) {
  let resp;
  try {
    resp = await fetch(url, { headers: { "Cache-Control": "no-cache" } });
  } catch (err) {
    throw new Error(`Failed to fetch ${url}: ${err?.message || err}`);
  }
  if (!resp.ok) {
    throw new Error(`Failed to fetch ${url}: HTTP ${resp.status}`);
  }
  try {
    return await resp.json();
  } catch (err) {
    throw new Error(`Invalid JSON at ${url}: ${err?.message || err}`);
  }
}

function formatNumber(value) {
  if (value === null || value === undefined) return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value);
  return n.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

function formatDec3(value) {
  if (value === null || value === undefined) return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  const fixed = n.toFixed(3);
  return fixed.replace(/^0\./, "."); // FanGraphs-style leading zero drop
}

function formatPct(value) {
  if (value === null || value === undefined) return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function formatMetric(value, kind) {
  if (kind === "pct") return formatPct(value);
  if (kind === "dec3") return formatDec3(value);
  return formatNumber(value);
}

function setLastUpdated(meta) {
  const el = $("#last-updated");
  if (!el) return;
  const value = meta?.last_updated || meta?.lastUpdated || "";
  el.textContent = value ? String(value) : "Unavailable";
}

let playersIndexPromise = null;

async function loadPlayersIndex() {
  if (!playersIndexPromise) {
    playersIndexPromise = fetchJson(`${DATA_BASE}/players_index.json`);
  }
  return playersIndexPromise;
}

function normalizeForSearch(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function renderSearchResults(results) {
  const host = $("#search-results");
  if (!host) return;
  host.textContent = "";
  for (const row of results) {
    const playerId = row.player_id;
    const name = row.name || "";
    const team = row.current_team_id;
    const div = document.createElement("a");
    div.className = "result";
    div.href = `/players/${encodeURIComponent(playerId)}/`;
    div.innerHTML = `<span>${escapeHtml(name)}</span><span class="meta">${escapeHtml(team ?? "")}</span>`;
    host.appendChild(div);
  }
}

function setSearchStatus(message) {
  const el = $("#search-status");
  if (!el) return;
  el.textContent = message || "";
}

function setupSearch() {
  const input = $("#player-search");
  if (!input) return;

  let debounce = null;
  input.addEventListener("input", () => {
    if (debounce) window.clearTimeout(debounce);
    debounce = window.setTimeout(async () => {
      const query = normalizeForSearch(input.value);
      if (!query) {
        setSearchStatus("");
        renderSearchResults([]);
        return;
      }
      setSearchStatus("Searching…");
      try {
        const index = await loadPlayersIndex();
        if (!Array.isArray(index)) {
          setSearchStatus("Search unavailable.");
          renderSearchResults([]);
          return;
        }
        const matches = [];
        for (const row of index) {
          if (!row) continue;
          const name = normalizeForSearch(row.name);
          if (name.includes(query)) matches.push(row);
          if (matches.length >= 10) break;
        }
        setSearchStatus(matches.length ? "" : "No matches.");
        renderSearchResults(matches);
      } catch (err) {
        setSearchStatus(err?.message || String(err));
        renderSearchResults([]);
      }
    }, 120);
  });
}

function renderChips(host, chips) {
  host.textContent = "";
  if (!chips || chips.length === 0) {
    host.textContent = "—";
    return;
  }
  for (const c of chips) {
    const span = document.createElement("span");
    span.className = "chip";
    span.textContent = String(c);
    host.appendChild(span);
  }
}

function renderKeyValues(host, metrics) {
  host.textContent = "";
  if (!metrics || typeof metrics !== "object") {
    host.textContent = "—";
    return;
  }
  const keys = Object.keys(metrics).sort();
  if (keys.length === 0) {
    host.textContent = "—";
    return;
  }
  for (const k of keys) {
    const row = document.createElement("div");
    row.className = "kv-row";
    row.innerHTML = `<span class="k">${escapeHtml(k)}</span><span class="v">${escapeHtml(formatNumber(metrics[k]))}</span>`;
    host.appendChild(row);
  }
}

function renderMetricsTable(host, { caption, columns, values }) {
  const block = document.createElement("div");
  block.className = "metrics-block";
  if (caption) {
    const cap = document.createElement("div");
    cap.className = "metrics-caption";
    cap.textContent = caption;
    block.appendChild(cap);
  }

  const table = document.createElement("table");
  table.className = "metrics-table";

  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  for (const col of columns) {
    const th = document.createElement("th");
    th.textContent = col.label;
    trh.appendChild(th);
  }
  thead.appendChild(trh);

  const tbody = document.createElement("tbody");
  const tr = document.createElement("tr");
  for (const col of columns) {
    const td = document.createElement("td");
    td.textContent = formatMetric(values?.[col.key], col.kind);
    tr.appendChild(td);
  }
  tbody.appendChild(tr);

  table.appendChild(thead);
  table.appendChild(tbody);
  block.appendChild(table);
  host.appendChild(block);
}

function hasAnyMetric(values, columns) {
  if (!values || !Array.isArray(columns) || columns.length === 0) return false;
  for (const col of columns) {
    const v = values?.[col.key];
    if (v === null || v === undefined || v === "") continue;
    const n = Number(v);
    if (Number.isFinite(n)) return true;
    return true;
  }
  return false;
}

function setupTabs() {
  const tabs = Array.from(document.querySelectorAll(".tab"));
  if (tabs.length === 0) return;

  function activate(name) {
    for (const t of tabs) t.classList.toggle("active", t.dataset.tab === name);
    for (const pane of document.querySelectorAll(".tabpane")) {
      pane.classList.toggle("active", pane.dataset.pane === name);
    }
  }

  for (const tab of tabs) {
    tab.addEventListener("click", () => activate(tab.dataset.tab));
  }
}

function renderList(host, rows) {
  host.textContent = "";
  if (!Array.isArray(rows) || rows.length === 0) {
    host.textContent = "No results.";
    return;
  }
  for (const row of rows) {
    const div = document.createElement("div");
    div.className = row.className ? `row ${row.className}` : "row";
    if (row.right === null || row.right === undefined || row.right === "") {
      div.innerHTML = `<div class="full">${row.left}</div>`;
    } else {
      div.innerHTML = `<div>${row.left}</div><div class="right">${row.right}</div>`;
    }
    host.appendChild(div);
  }
}

function renderTeamsList(host, teams, { showId = true } = {}) {
  if (!host) return;
  host.textContent = "";
  if (!Array.isArray(teams) || teams.length === 0) {
    host.textContent = "Unable to load teams.";
    return;
  }
  const rows = teams.map((t) => {
    const teamId = t.team_id;
    const abbr = t.abbreviation || t.abbrev || teamId;
    return {
      left: `<a href="/teams/${encodeURIComponent(teamId)}/">${escapeHtml(abbr)}</a>`,
      right: showId ? escapeHtml(String(teamId)) : null,
    };
  });
  renderList(host, rows);
}

function renderDivisionGroup(host, leagueKey, teamsByAbbr) {
  if (!host) return;
  host.textContent = "";
  const league = DIVISIONS[leagueKey];
  if (!league) {
    host.textContent = "Unavailable.";
    return;
  }
  for (const [division, abbrs] of Object.entries(league)) {
    const wrapper = document.createElement("div");
    wrapper.className = "division";

    const title = document.createElement("div");
    title.className = "division-title";
    title.textContent = `${leagueKey} ${division}`;
    wrapper.appendChild(title);

    const grid = document.createElement("div");
    grid.className = "division-teams";
    for (const abbr of abbrs) {
      const team = teamsByAbbr.get(abbr);
      if (!team) continue;
      const a = document.createElement("a");
      a.className = "team-pill";
      a.href = `/teams/${encodeURIComponent(team.team_id)}/`;
      a.textContent = abbr;
      grid.appendChild(a);
    }
    wrapper.appendChild(grid);
    host.appendChild(wrapper);
  }
}

function renderRosterList(host, roster) {
  if (!host) return;
  host.textContent = "";
  if (!Array.isArray(roster) || roster.length === 0) {
    host.textContent = "No results.";
    return;
  }
  const infieldPositions = new Set(["1B", "2B", "3B", "SS", "IF"]);
  const infielders = [];
  const outfielders = [];
  for (const p of roster) {
    const pos = String(p?.position || "").toUpperCase();
    if (infieldPositions.has(pos)) infielders.push(p);
    else outfielders.push(p);
  }

  const byName = (a, b) => String(a?.name || "").localeCompare(String(b?.name || ""), undefined, { sensitivity: "base" });
  infielders.sort(byName);
  outfielders.sort(byName);

  const ul = document.createElement("ul");
  ul.className = "roster-list-items";
  for (const p of [...infielders, ...outfielders]) {
    const pid = p?.player_id;
    const name = p?.name || `Player ${pid}`;
    const href = p?.href || `/players/${encodeURIComponent(pid)}/`;
    const li = document.createElement("li");
    const a = document.createElement("a");
    a.className = "roster-name";
    a.href = href;
    a.textContent = String(name);
    li.appendChild(a);
    ul.appendChild(li);
  }
  host.appendChild(ul);
}

async function renderHome({ teams }) {
  const alHost = $("#al-divisions");
  const nlHost = $("#nl-divisions");
  if (!Array.isArray(teams) || teams.length === 0) {
    if (alHost) alHost.textContent = "Unable to load teams.";
    if (nlHost) nlHost.textContent = "Unable to load teams.";
    return;
  }
  const teamsByAbbr = new Map();
  for (const t of teams) {
    const abbr = t?.abbreviation || t?.abbrev;
    if (abbr) teamsByAbbr.set(String(abbr), t);
  }
  renderDivisionGroup(alHost, "AL", teamsByAbbr);
  renderDivisionGroup(nlHost, "NL", teamsByAbbr);
}

async function renderTeamsIndex({ teams }) {
  const host = $("#teams");
  renderTeamsList(host, teams, { showId: true });
}

async function renderTeamPage(teamId) {
  const title = $("#team-title");
  const subtitle = $("#team-subtitle");

  try {
    const team = await fetchJson(`${DATA_BASE}/teams/${encodeURIComponent(teamId)}.json`);
    const abbr = team?.abbreviation || team?.abbrev || `Team ${teamId}`;
    if (title) title.textContent = String(abbr);
    if (subtitle) subtitle.textContent = `Team ${teamId}`;
    applyTeamTheme(abbr);

    const groupSpecs = [
      { key: "hitters", host: $("#roster-hitters") },
      { key: "starters", host: $("#roster-starters") },
      { key: "relievers", host: $("#roster-relievers") },
    ];
    for (const spec of groupSpecs) {
      if (!spec.host) continue;
      const roster = Array.isArray(team?.[spec.key]) ? team[spec.key] : [];
      renderRosterList(spec.host, roster);
    }
  } catch (err) {
    showStatus(err?.message || String(err));
    for (const host of ["#roster-hitters", "#roster-starters", "#roster-relievers"].map($)) {
      if (host) host.textContent = "Unable to load roster.";
    }
  }
}

async function renderPlayerPage(playerId, { teamsById }) {
  const title = $("#player-title");
  const subtitle = $("#player-subtitle");
  const actions = $("#player-actions");
  const metricsHost = $("#player-metrics");

  try {
    const p = await fetchJson(`${DATA_BASE}/players/${encodeURIComponent(playerId)}.json`);
    const name = p?.name || p?.player_name || `Player ${playerId}`;
    if (title) title.textContent = String(name);

    const currentTeamId = p?.current_team_id ?? p?.team_id ?? null;
    const teamAbbr = teamsById.get(String(currentTeamId))?.abbreviation || "";

    if (subtitle) {
      subtitle.textContent = currentTeamId ? `Current team: ${teamAbbr || currentTeamId}` : "Current team: —";
    }

    if (actions) {
      actions.textContent = "";
      if (currentTeamId) {
        const a = document.createElement("a");
        a.className = "action";
        a.href = `/teams/${encodeURIComponent(currentTeamId)}/`;
        a.textContent = "Back to current team";
        actions.appendChild(a);
      }
    }

    if (metricsHost) {
      const roles = Array.isArray(p?.roles) ? p.roles.map((r) => String(r).toLowerCase()) : [];
      const primaryRole = String(p?.role || roles[0] || "hitter").toLowerCase();

      const hitterCandidate = p?.hitter_metrics || (primaryRole === "hitter" ? p?.metrics : null);
      const starterCandidate = p?.pitcher_metrics || (primaryRole === "starter" ? p?.metrics : null);
      const relieverCandidate = primaryRole === "reliever" ? p?.metrics : null;

      const hitter = hasAnyMetric(hitterCandidate, METRIC_COLUMNS.hitter) ? hitterCandidate : null;
      const starter = hasAnyMetric(starterCandidate, METRIC_COLUMNS.starter) ? starterCandidate : null;
      const reliever = hasAnyMetric(relieverCandidate, METRIC_COLUMNS.reliever) ? relieverCandidate : null;

      metricsHost.textContent = "";

      if (hitter) {
        renderMetricsTable(metricsHost, {
          caption: roles.includes("starter") || roles.includes("reliever") ? "Hitting" : null,
          columns: METRIC_COLUMNS.hitter,
          values: hitter,
        });
      }

      if (starter) {
        renderMetricsTable(metricsHost, {
          caption: hitter ? "Pitching (Starter)" : null,
          columns: METRIC_COLUMNS.starter,
          values: starter,
        });
      } else if (reliever) {
        renderMetricsTable(metricsHost, {
          caption: hitter ? "Pitching (Reliever)" : null,
          columns: METRIC_COLUMNS.reliever,
          values: reliever,
        });
      }

      if (!hitter && !starter && !reliever) {
        metricsHost.textContent = "—";
      }
    }

  } catch (err) {
    showStatus(err?.message || String(err));
    if (metricsHost) metricsHost.textContent = "Unavailable";
  }
}

async function init() {
  setupSearch();
  setupTabs();

  const teamsById = new Map();

  try {
    const [meta, teams] = await Promise.all([fetchJson(`${DATA_BASE}/meta.json`), fetchJson(`${DATA_BASE}/teams.json`)]);
    clearStatus();
    setLastUpdated(meta);
    if (Array.isArray(teams)) {
      for (const t of teams) {
        teamsById.set(String(t?.team_id), {
          team_id: t?.team_id,
          abbreviation: t?.abbreviation || t?.abbrev || "",
        });
      }
    }
    const page = document.body?.dataset?.page || "";
    if (page !== "team") clearTeamTheme();
    if (page === "home") await renderHome({ teams });
    if (page === "teams-index") await renderTeamsIndex({ teams });

    if (page === "team") {
      const teamId = document.body?.dataset?.teamId;
      if (teamId) await renderTeamPage(teamId);
      else showStatus("Missing team id.");
    }

    if (page === "player") {
      const playerId = document.body?.dataset?.playerId;
      if (playerId) await renderPlayerPage(playerId, { teamsById });
      else showStatus("Missing player id.");
    }
  } catch (err) {
    showStatus(err?.message || String(err));
    const teamsList = $("#teams-list");
    if (teamsList) teamsList.textContent = "Unavailable";
    const lastUpdated = $("#last-updated");
    if (lastUpdated) lastUpdated.textContent = "Unavailable";
  }
}

init();
