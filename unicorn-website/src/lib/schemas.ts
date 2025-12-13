import { z } from "zod";

const zNumber = z.preprocess((value) => {
  if (typeof value === "string" && value.trim() !== "") return Number(value);
  return value;
}, z.number());

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
    player_id: zNumber,
    player_name: z.string().optional(),
    full_name: z.string().optional(),
    position: z.string().nullable().optional(),
    role: z.string().nullable().optional(),
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

