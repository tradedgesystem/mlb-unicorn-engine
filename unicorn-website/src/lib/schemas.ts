import { z } from "zod";

const zNumber = z.preprocess((value) => {
  if (typeof value === "string" && value.trim() !== "") return Number(value);
  return value;
}, z.number());

const zMetricValue = z.preprocess((value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}, z.number().nullable());

const zMetrics = z.preprocess((value) => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value;
}, z.record(z.string(), zMetricValue));

export const Top50RowSchema = z
  .object({
    run_date: z.string().optional(),
    rank: zNumber,
    entity_id: zNumber,
    pattern_id: z.string(),
    description: z.string(),
    metric_value: zNumber.nullable().optional(),
    sample_size: zNumber.optional(),
    role: z.string().nullable().optional().default(null),
    primary_pos: z.string().nullable().optional().default(null),
  })
  .passthrough();

export const Top50ResponseSchema = z.array(Top50RowSchema);

export const TeamSchema = z
  .object({
    team_id: zNumber,
    team_name: z.string(),
    abbrev: z.string(),
  })
  .passthrough();

export const TeamsListSchema = z.array(TeamSchema);

export const RosterPlayerSchema = z
  .object({
    player_id: zNumber.optional(),
    player_name: z.string().optional(),
    full_name: z.string().optional(),
    position: z.string().nullable().optional(),
    role: z.string().nullable().optional(),
    metrics: zMetrics.optional().default({}),
  })
  .passthrough();

export const TeamDetailSchema = z
  .object({
    team_id: zNumber,
    team_name: z.string(),
    abbrev: z.string(),
    hitters: z.array(RosterPlayerSchema).optional().default([]),
    starters: z.array(RosterPlayerSchema).optional().default([]),
    relievers: z.array(RosterPlayerSchema).optional().default([]),
  })
  .passthrough();

export type Top50Row = z.infer<typeof Top50RowSchema>;
export type Team = z.infer<typeof TeamSchema>;
export type TeamDetail = z.infer<typeof TeamDetailSchema>;
export type RosterPlayer = z.infer<typeof RosterPlayerSchema>;
