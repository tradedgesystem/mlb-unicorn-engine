"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchPlayers, fetchTop50, PlayerMap, UnicornRow } from "../lib/legacyTop";

type SortKey = "rank" | "player";

function positionBadge(row: UnicornRow): string {
  const role = (row.role || "").toLowerCase();
  if (role === "starter") return "SP";
  if (role === "reliever") return "RP";
  const pos = (row.primary_pos || "").trim().toUpperCase();
  if (pos && pos !== "P") return pos;
  return "—";
}

export function TopTable() {
  const [rows, setRows] = useState<UnicornRow[]>([]);
  const [players, setPlayers] = useState<PlayerMap>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>("rank");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [selectedDate, setSelectedDate] = useState<string>("2025-03-27");
  const [expanded, setExpanded] = useState<number | null>(null);
  const [search, setSearch] = useState("");
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const loadData = useCallback(async (runDate: string) => {
    setLoading(true);
    setError(null);
    try {
      const [playersMap, top50] = await Promise.all([fetchPlayers(), fetchTop50(runDate)]);
      setPlayers(playersMap);
      setRows(top50);
      setLastUpdated(new Date());
    } catch (err) {
      console.error(err);
      setError("Unable to load data. Please retry.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData(selectedDate);
  }, [loadData, selectedDate]);

  const decoratedRows = useMemo(() => {
    return rows.map((r) => ({
      ...r,
      playerName: players[r.entity_id] || `#${r.entity_id}`,
    }));
  }, [rows, players]);

  const filteredRows = useMemo(() => {
    const q = search.toLowerCase().trim();
    if (!q) return decoratedRows;
    return decoratedRows.filter((r) => r.playerName.toLowerCase().includes(q));
  }, [decoratedRows, search]);

  const sortedRows = useMemo(() => {
    const sorted = [...filteredRows];
    sorted.sort((a, b) => {
      if (sortKey === "player") {
        return sortDir === "asc"
          ? a.playerName.localeCompare(b.playerName)
          : b.playerName.localeCompare(a.playerName);
      }
      const diff = Number(a.rank) - Number(b.rank);
      return sortDir === "asc" ? diff : -diff;
    });
    return sorted;
  }, [filteredRows, sortDir, sortKey]);

  const onSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((prev) => (prev === "asc" ? "desc" : "asc"));
      return;
    }
    setSortKey(key);
    setSortDir("asc");
  };

  const heroDateLabel = new Date(selectedDate).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  });

  return (
    <div className="space-y-6">
      <header className="glass p-6">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <p className="text-sm text-neutral-700">MLB Unicorn Engine</p>
            <h1 className="text-3xl font-semibold text-neutral-900">
              Top 50 — {heroDateLabel}
            </h1>
          </div>
          <div className="flex flex-wrap gap-3 items-center">
            <label className="glass px-3 py-2 text-sm text-neutral-900">
              <span className="mr-2 text-xs uppercase tracking-wide text-neutral-700">
                Date
              </span>
              <input
                type="date"
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                className="bg-transparent outline-none text-neutral-900"
              />
            </label>
          </div>
        </div>
        <div className="mt-4 flex flex-wrap gap-3 items-center">
          <input
            type="search"
            placeholder="Search player"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="glass w-full max-w-sm px-3 py-2 text-sm outline-none"
          />
          {lastUpdated && (
            <div className="text-xs text-neutral-700">
              Last updated {lastUpdated.toLocaleTimeString()}
            </div>
          )}
        </div>
      </header>

      <div className="glass p-0 overflow-hidden">
        <div className="overflow-x-auto">
          {loading ? (
            <div className="p-8 text-sm text-neutral-800">Loading...</div>
          ) : error ? (
            <div className="p-8 text-center space-y-3">
              <p className="text-neutral-800">{error}</p>
              <button
                onClick={() => loadData(selectedDate)}
                className="glass inline-flex items-center px-4 py-2 text-sm font-medium hover:bg-neutral-200"
              >
                Retry
              </button>
            </div>
          ) : sortedRows.length === 0 ? (
            <div className="p-8 text-center text-neutral-800">
              No data for this date. Try a different date.
            </div>
          ) : (
            <table className="min-w-full">
              <thead className="bg-neutral-200 sticky top-0 z-10 border-b border-neutral-400">
                <tr className="text-left text-sm text-neutral-800">
                  <Th label="#" onClick={() => onSort("rank")} active={sortKey === "rank"} dir={sortDir} />
                  <Th
                    label="Player"
                    onClick={() => onSort("player")}
                    active={sortKey === "player"}
                    dir={sortDir}
                  />
                  <th className="px-4 py-3">Description</th>
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row) => {
                  const isExpanded = expanded === row.rank;
                  return (
                    <FragmentRow
                      key={`${row.rank}-${row.entity_id}-${row.pattern_id}`}
                      row={row}
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
    <th onClick={onClick} className="px-4 py-3 cursor-pointer select-none">
      <span className="inline-flex items-center gap-1">
        {label}
        {active && (dir === "asc" ? "↑" : "↓")}
      </span>
    </th>
  );
}

type FragmentRowProps = {
  row: UnicornRow & { playerName: string };
  isExpanded: boolean;
  onToggle: () => void;
};

function FragmentRow({ row, isExpanded, onToggle }: FragmentRowProps) {
  const pid = Number(row.entity_id);
  const isValidId = Number.isFinite(pid);
  const pos = positionBadge(row);

  return (
    <>
      <tr
        onClick={onToggle}
        className={`cursor-pointer hover:bg-neutral-200 ${
          isExpanded ? "bg-neutral-200" : ""
        }`}
      >
        <td className="px-4 py-3 text-sm font-semibold text-neutral-900 border-b border-neutral-300">
          {row.rank}
        </td>
        <td className="px-4 py-3 text-sm text-neutral-900 border-b border-neutral-300">
          <div className="flex items-center gap-3">
            <div className="glass flex h-10 w-10 items-center justify-center text-xs font-semibold">
              {pos}
            </div>
            <div className="flex flex-col">
              {isValidId ? (
                <Link
                  href={`/players/${pid}`}
                  className="font-medium leading-tight hover:underline"
                  onClick={(e) => e.stopPropagation()}
                >
                  {row.playerName}
                </Link>
              ) : (
                <span className="font-medium leading-tight text-neutral-900">
                  {row.playerName}
                </span>
              )}
              <span className="text-xs text-neutral-700">
                Pattern {row.pattern_id}
              </span>
            </div>
          </div>
        </td>
        <td className="px-4 py-3 text-sm text-neutral-900 border-b border-neutral-300">
          <span className="line-clamp-1">{row.description}</span>
        </td>
      </tr>
      {isExpanded && (
        <tr className="bg-neutral-200">
          <td colSpan={3} className="px-4 pb-4 text-sm text-neutral-900">
            <div className="glass p-4 mt-2">
              <div className="flex flex-wrap gap-6 text-sm text-neutral-900">
                <div>
                  <p className="text-xs uppercase text-neutral-700">Description</p>
                  <p className="font-medium text-neutral-900">{row.description}</p>
                </div>
                <div>
                  <p className="text-xs uppercase text-neutral-700">Metric</p>
                  <p>{row.metric_value?.toFixed(3)}</p>
                </div>
                <div>
                  <p className="text-xs uppercase text-neutral-700">Sample</p>
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

