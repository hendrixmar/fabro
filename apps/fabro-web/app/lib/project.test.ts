import { describe, expect, test } from "bun:test";
import type { Automation, Run } from "@qltysh/fabro-api-client";
import { projectRunStats, usedByProjects } from "./project";

function run(kind: string, createdAt: string): Run {
  return {
    lifecycle:  { status: { kind } },
    timestamps: { created_at: createdAt },
  } as unknown as Run;
}

describe("projectRunStats", () => {
  test("counts running, failures in the last day and the latest start", () => {
    const now = Date.parse("2026-09-26T12:00:00Z");
    const stats = projectRunStats(
      [
        run("running", "2026-09-26T11:00:00Z"),
        run("failed", "2026-09-26T10:00:00Z"),
        run("dead", "2026-09-26T09:00:00Z"),
        run("failed", "2026-09-24T10:00:00Z"),
        run("succeeded", "2026-09-26T11:30:00Z"),
      ],
      now,
    );
    expect(stats).toEqual({ running: 1, failed24h: 2, lastRunAt: "2026-09-26T11:30:00Z" });
  });

  test("an empty project has no last run", () => {
    expect(projectRunStats([], 0)).toEqual({ running: 0, failed24h: 0, lastRunAt: null });
  });
});

describe("usedByProjects", () => {
  test("groups links under their global source", () => {
    const automations = [
      { id: "woodpecker-loop", project_id: null, source_automation_id: null },
      { id: "tierrapay-woodpecker", project_id: "tierrapay", source_automation_id: "woodpecker-loop" },
      { id: "mafeva-woodpecker", project_id: "mafeva", source_automation_id: "woodpecker-loop" },
      { id: "custom", project_id: "mafeva", source_automation_id: null },
    ] as unknown as Automation[];
    expect(usedByProjects(automations).get("woodpecker-loop")).toEqual(["mafeva", "tierrapay"]);
    expect(usedByProjects(automations).has("custom")).toBe(false);
  });
});
