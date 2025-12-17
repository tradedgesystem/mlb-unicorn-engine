const DATA_BASE = "/data/latest";

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

function setLastUpdated(meta) {
  const el = $("#last-updated");
  if (!el) return;
  const value = meta?.last_updated || meta?.lastUpdated || "";
  el.textContent = value ? String(value) : "Unavailable";
}

function renderTeamsSidebar(teams) {
  const host = $("#teams-list");
  if (!host) return;
  if (!Array.isArray(teams) || teams.length === 0) {
    host.textContent = "Unavailable";
    return;
  }
  host.textContent = "";
  for (const t of teams) {
    const teamId = t?.team_id;
    const abbr = t?.abbreviation || t?.abbrev || "";
    const a = document.createElement("a");
    a.className = "team-link";
    a.href = `/teams/${encodeURIComponent(teamId)}/`;
    a.textContent = String(abbr || teamId);
    host.appendChild(a);
  }
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
    div.className = "row";
    div.innerHTML = `<div>${row.left}</div><div class="right">${row.right || ""}</div>`;
    host.appendChild(div);
  }
}

async function renderHome({ teamsById }) {
  const host = $("#unicorns");
  if (!host) return;
  try {
    const unicorns = await fetchJson(`${DATA_BASE}/unicorns.json`);
    if (!Array.isArray(unicorns)) throw new Error("unicorns.json is not an array");
    host.textContent = "";
    if (unicorns.length === 0) {
      host.textContent = "No unicorns for this day.";
      return;
    }
    for (const u of unicorns) {
      const pid = u?.player_id;
      const name = u?.name || `Player ${pid}`;
      const roles = Array.isArray(u?.roles) ? u.roles : [];
      const teamId = u?.current_team_id;
      const teamAbbr = teamsById.get(String(teamId))?.abbreviation || "";

      const card = document.createElement("div");
      card.className = "card";
      card.innerHTML = `
        <div class="title">
          <a href="/players/${encodeURIComponent(pid)}/">${escapeHtml(name)}</a>
          <div class="small">${escapeHtml(teamAbbr)}</div>
        </div>
        <div class="chips">${roles.map((r) => `<span class="chip">${escapeHtml(r)}</span>`).join("")}</div>
        <div class="small">${escapeHtml(u?.description || "")}</div>
      `;
      host.appendChild(card);
    }
  } catch (err) {
    host.textContent = "Unable to load unicorns.";
    showStatus(err?.message || String(err));
  }
}

async function renderTeamsIndex({ teams }) {
  const host = $("#teams");
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
      right: escapeHtml(String(teamId)),
    };
  });
  renderList(host, rows);
}

async function renderTeamPage(teamId) {
  const title = $("#team-title");
  const subtitle = $("#team-subtitle");

  try {
    const team = await fetchJson(`${DATA_BASE}/teams/${encodeURIComponent(teamId)}.json`);
    const abbr = team?.abbreviation || team?.abbrev || `Team ${teamId}`;
    if (title) title.textContent = String(abbr);
    if (subtitle) subtitle.textContent = `Team ${teamId}`;

    const groupSpecs = [
      { key: "hitters", host: $("#roster-hitters") },
      { key: "starters", host: $("#roster-starters") },
      { key: "relievers", host: $("#roster-relievers") },
    ];
    for (const spec of groupSpecs) {
      if (!spec.host) continue;
      const roster = Array.isArray(team?.[spec.key]) ? team[spec.key] : [];
      const rows = roster.map((p) => {
        const pid = p?.player_id;
        const name = p?.name || `Player ${pid}`;
        const pos = p?.position || "";
        const roles = Array.isArray(p?.roles) ? p.roles.join(", ") : "";
        const href = p?.href || `/players/${encodeURIComponent(pid)}/`;
        return {
          left: `<a href="${escapeHtml(href)}">${escapeHtml(name)}</a><div class="small">${escapeHtml(
            roles,
          )}</div>`,
          right: escapeHtml(pos),
        };
      });
      renderList(spec.host, rows);
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
  const rolesHost = $("#player-roles");
  const metricsHost = $("#player-metrics");
  const recentHost = $("#player-recent");

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

    const roles = Array.isArray(p?.roles) ? p.roles : p?.role ? [p.role] : [];
    if (rolesHost) renderChips(rolesHost, roles);

    if (metricsHost) {
      const primary = p?.metrics || {};
      const hitter = p?.hitter_metrics || null;
      const pitcher = p?.pitcher_metrics || null;
      if (hitter || pitcher) {
        metricsHost.textContent = "";
        if (hitter) {
          const block = document.createElement("div");
          block.className = "metric-section";
          block.innerHTML = `<div class="metric-title">Hitter</div><div class="kv"></div>`;
          renderKeyValues(block.querySelector(".kv"), hitter);
          metricsHost.appendChild(block);
        }
        if (pitcher) {
          const block = document.createElement("div");
          block.className = "metric-section";
          block.innerHTML = `<div class="metric-title">Pitcher</div><div class="kv"></div>`;
          renderKeyValues(block.querySelector(".kv"), pitcher);
          metricsHost.appendChild(block);
        }
      } else {
        renderKeyValues(metricsHost, primary);
      }
    }

    if (recentHost) {
      const recent = Array.isArray(p?.recent_unicorns) ? p.recent_unicorns : [];
      const rows = recent.map((u) => {
        const runDate = u?.run_date || "—";
        const desc = u?.description || "—";
        const score = u?.score;
        return {
          left: `<div>${escapeHtml(desc)}</div><div class="small">${escapeHtml(runDate)}</div>`,
          right: escapeHtml(formatNumber(score)),
        };
      });
      renderList(recentHost, rows);
    }
  } catch (err) {
    showStatus(err?.message || String(err));
    if (rolesHost) rolesHost.textContent = "Unavailable";
    if (metricsHost) metricsHost.textContent = "Unavailable";
    if (recentHost) recentHost.textContent = "Unavailable";
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
    renderTeamsSidebar(teams);

    const page = document.body?.dataset?.page || "";
    if (page === "home") await renderHome({ teamsById });
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
