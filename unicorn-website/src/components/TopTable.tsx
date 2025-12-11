/* eslint-disable @next/next/no-img-element */
"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchPlayers, fetchTop50, PlayerMap, UnicornRow } from "../lib/legacyTop";
import { slugifyPlayer } from "../lib/slugs";

type Filter = "all" | "hitters" | "pitchers" | "relievers";
type SortKey = "rank" | "score" | "player" | "entity_type";

const FILTERS: { label: string; value: Filter }[] = [
  { label: "All", value: "all" },
  { label: "Hitters", value: "hitters" },
  { label: "Pitchers", value: "pitchers" },
  { label: "Relievers", value: "relievers" },
];

function classifyEntity(row: UnicornRow): Filter {
  const type = row.entity_type?.toLowerCase();
  if (type?.includes("relief") || type === "reliever" || row.pattern_id?.startsWith("UNQ-R")) {
    return "relievers";
  }
  if (type?.includes("pitch")) {
    return "pitchers";
  }
  if (type?.includes("batt") || type === "player") {
    return "hitters";
  }
  return "all";
}

function formatDateInput(date: Date) {
  return date.toISOString().slice(0, 10);
}

const shimmer =
  "bg-gradient-to-r from-white/60 via-white/40 to-white/60 animate-[shimmer_1.6s_ease_infinite]";

export function TopTable() {
  const [rows, setRows] = useState<UnicornRow[]>([]);
  const [players, setPlayers] = useState<PlayerMap>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<Filter>("all");
  const [sortKey, setSortKey] = useState<SortKey>("rank");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [selectedDate, setSelectedDate] = useState<string>("2025-03-27");
  const [expanded, setExpanded] = useState<number | null>(null);
  const [search, setSearch] = useState("");
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const loadData = useCallback(
    async (date: string) => {
      setLoading(true);
      setError(null);
      try {
        const [playersMap, top50] = await Promise.all([fetchPlayers(), fetchTop50(date)]);
        setPlayers(playersMap);
        setRows(top50);
        setLastUpdated(new Date());
      } catch (err) {
        console.error(err);
        setError("Unable to load data. Please retry.");
      } finally {
        setLoading(false);
      }
    },
    [setRows]
  );

  useEffect(() => {
    loadData(selectedDate);
  }, [loadData, selectedDate]);

  const decoratedRows = useMemo(() => {
    return rows.map((r) => ({
      ...r,
      playerName: players[r.entity_id] || `#${r.entity_id}`,
      tag: classifyEntity(r),
    }));
  }, [rows, players]);

  const filteredRows = useMemo(() => {
    return decoratedRows
      .filter((r) => (filter === "all" ? true : r.tag === filter))
      .filter((r) => r.playerName.toLowerCase().includes(search.toLowerCase()));
  }, [decoratedRows, filter, search]);

  const sortedRows = useMemo(() => {
    const sorted = [...filteredRows];
    sorted.sort((a, b) => {
      let lhs: number | string = 0;
      let rhs: number | string = 0;
      if (sortKey === "player") {
        lhs = a.playerName;
        rhs = b.playerName;
      } else if (sortKey === "entity_type") {
        lhs = a.entity_type;
        rhs = b.entity_type;
      } else if (sortKey === "score") {
        lhs = a.score;
        rhs = b.score;
      } else {
        lhs = a.rank;
        rhs = b.rank;
      }

      if (typeof lhs === "string" && typeof rhs === "string") {
        return sortDir === "asc"
          ? lhs.localeCompare(rhs)
          : rhs.localeCompare(lhs);
      }
      const diff = Number(lhs) - Number(rhs);
      return sortDir === "asc" ? diff : -diff;
    });
    return sorted;
  }, [filteredRows, sortDir, sortKey]);

  const onSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((prev) => (prev === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "rank" ? "asc" : "desc");
    }
  };

  const heroDateLabel = new Date(selectedDate).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  });

  return (
    <div className="fade-in space-y-6">
      <header className="glass rounded-3xl p-6">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <p className="text-sm text-neutral-500">MLB Unicorn Engine</p>
            <h1 className="text-3xl font-semibold text-neutral-900">
              Top 50 — {heroDateLabel}
            </h1>
            <p className="text-sm text-neutral-500 mt-1">
              Apple-inspired glass UI with live unicorn scores.
            </p>
          </div>
          <div className="flex flex-wrap gap-3 items-center">
            <label className="glass rounded-full px-4 py-2 text-sm text-neutral-700 shadow-sm">
              <span className="mr-2 text-xs uppercase tracking-wide text-neutral-500">
                Date
              </span>
              <input
                type="date"
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                className="bg-transparent outline-none text-neutral-900"
              />
            </label>
            <div className="flex items-center gap-2">
              {FILTERS.map((f) => (
                <button
                  key={f.value}
                  onClick={() => setFilter(f.value)}
                  className={`rounded-full px-3 py-1 text-sm transition ${
                    filter === f.value
                      ? "glass shadow-sm text-neutral-900"
                      : "bg-white/60 text-neutral-600 hover:bg-white/80"
                  }`}
                >
                  {f.label}
                </button>
              ))}
            </div>
          </div>
        </div>
        <div className="mt-4 flex flex-wrap gap-3">
          <input
            type="search"
            placeholder="Search player"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="glass w-full max-w-sm rounded-2xl px-4 py-2 outline-none focus:ring-2 focus:ring-blue-300 transition"
          />
          {lastUpdated && (
            <div className="text-xs text-neutral-500">
              Last updated {lastUpdated.toLocaleTimeString()}
            </div>
          )}
        </div>
      </header>

      <div className="glass rounded-3xl p-0 overflow-hidden">
        <div className="overflow-x-auto">
          {loading ? (
            <div className="p-8 grid gap-3">
              {Array.from({ length: 6 }).map((_, idx) => (
                <div
                  key={idx}
                  className={`h-14 rounded-2xl ${shimmer}`}
                  aria-hidden
                />
              ))}
            </div>
          ) : error ? (
            <div className="p-8 text-center space-y-3">
              <p className="text-neutral-700">{error}</p>
              <button
                onClick={() => loadData(selectedDate)}
                className="glass inline-flex items-center gap-2 rounded-full px-4 py-2 text-sm font-medium hover:-translate-y-0.5 transition"
              >
                Retry
              </button>
            </div>
          ) : sortedRows.length === 0 ? (
            <div className="p-8 text-center text-neutral-600">
              No data for this date. Try a different date.
            </div>
          ) : (
            <table className="min-w-full">
              <thead className="bg-white/60 backdrop-blur sticky top-0 z-10">
                <tr className="text-left text-sm text-neutral-600">
                  <Th label="#" onClick={() => onSort("rank")} active={sortKey === "rank"} dir={sortDir} />
                  <Th
                    label="Player"
                    onClick={() => onSort("player")}
                    active={sortKey === "player"}
                    dir={sortDir}
                  />
                  <Th
                    label="Type"
                    onClick={() => onSort("entity_type")}
                    active={sortKey === "entity_type"}
                    dir={sortDir}
                  />
                  <Th
                    label="Score"
                    onClick={() => onSort("score")}
                    active={sortKey === "score"}
                    dir={sortDir}
                  />
                  <th className="px-4 py-3">Description</th>
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row, idx) => {
                  const isTop5 = idx < 5;
                  const isExpanded = expanded === row.rank;
                  return (
                    <FragmentRow
                      key={`${row.rank}-${row.entity_id}-${row.pattern_id}`}
                      row={row}
                      isTop5={isTop5}
                      isExpanded={isExpanded}
                      onToggle={() =>
                        setExpanded((prev) => (prev === row.rank ? null : row.rank))
                      }
                    />
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}

type ThProps = {
  label: string;
  onClick: () => void;
  active: boolean;
  dir: "asc" | "desc";
};

function Th({ label, onClick, active, dir }: ThProps) {
  return (
    <th
      onClick={onClick}
      className="px-4 py-3 cursor-pointer select-none hover:text-neutral-900"
    >
      <span className="inline-flex items-center gap-1">
        {label}
        {active && (dir === "asc" ? "↑" : "↓")}
      </span>
    </th>
  );
}

type FragmentRowProps = {
  row: UnicornRow & { playerName: string; tag: Filter };
  isTop5: boolean;
  isExpanded: boolean;
  onToggle: () => void;
};

function badgeColor(tag: Filter) {
  if (tag === "hitters") return "bg-emerald-100 text-emerald-700";
  if (tag === "pitchers") return "bg-sky-100 text-sky-700";
  if (tag === "relievers") return "bg-amber-100 text-amber-700";
  return "bg-neutral-100 text-neutral-700";
}

function FragmentRow({ row, isTop5, isExpanded, onToggle }: FragmentRowProps) {
  const topTone = isTop5
    ? "bg-gradient-to-r from-amber-100/90 to-yellow-50/90 border-amber-200/60"
    : "border-transparent";

  return (
    <>
      <tr
        onClick={onToggle}
        className={`transition cursor-pointer hover:-translate-y-0.5 ${
          isExpanded ? "bg-white/80" : ""
        }`}
      >
        <td
          className={`px-4 py-3 text-sm font-semibold text-neutral-900 border-b border-white/40 ${topTone}`}
        >
          {row.rank}
        </td>
        <td
          className={`px-4 py-3 text-sm text-neutral-900 border-b border-white/40 ${topTone}`}
        >
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-2xl glass text-sm font-semibold">
              {row.playerName
                .split(" ")
                .slice(0, 2)
                .map((part) => part[0])
                .join("")
                .toUpperCase()}
            </div>
            <div className="flex flex-col">
              <a
                href={`/players/${slugifyPlayer(row.playerName)}`}
                className="font-medium leading-tight hover:underline decoration-2 decoration-neutral-400"
              >
                {row.playerName}
              </a>
              <span className="text-xs text-neutral-500">
                Pattern {row.pattern_id}
              </span>
            </div>
          </div>
        </td>
        <td
          className={`px-4 py-3 text-sm border-b border-white/40 ${topTone}`}
        >
          <span className={`rounded-full px-3 py-1 text-xs font-medium ${badgeColor(row.tag)}`}>
            {row.tag === "hitters"
              ? "Hitter"
              : row.tag === "pitchers"
              ? "Pitcher"
              : row.tag === "relievers"
              ? "Reliever"
              : "Player"}
          </span>
        </td>
        <td
          className={`px-4 py-3 text-sm font-semibold text-neutral-900 border-b border-white/40 ${topTone}`}
        >
          {row.score.toFixed(3)}
        </td>
        <td
          className={`px-4 py-3 text-sm text-neutral-700 border-b border-white/40 ${topTone}`}
        >
          <span className="line-clamp-1">{row.description}</span>
        </td>
      </tr>
      {isExpanded && (
        <tr className="bg-white/80">
          <td colSpan={5} className="px-4 pb-4 text-sm text-neutral-700">
            <div className="glass rounded-2xl p-4 mt-2 fade-in">
              <div className="flex flex-wrap gap-4 text-sm text-neutral-700">
                <div>
                  <p className="text-xs uppercase text-neutral-500">Description</p>
                  <p className="font-medium text-neutral-900">{row.description}</p>
                </div>
                <div>
                  <p className="text-xs uppercase text-neutral-500">Metric</p>
                  <p>{row.metric_value?.toFixed(3)}</p>
                </div>
                <div>
                  <p className="text-xs uppercase text-neutral-500">Sample</p>
                  <p>{row.sample_size}</p>
                </div>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}
