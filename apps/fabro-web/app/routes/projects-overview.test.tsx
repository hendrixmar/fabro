import { afterEach, beforeEach, describe, expect, mock, test } from "bun:test";
import TestRenderer, { act } from "react-test-renderer";
import { MemoryRouter, Route, Routes } from "react-router";
import { setupReactTestEnv, textContent } from "../lib/test-utils";

const now = new Date().toISOString();
let runs: unknown[] = [];
let automations: unknown[] = [];
let teardown: (() => void) | undefined;

mock.module("../lib/queries", () => ({
  useRunsPage:       () => ({ data: { data: runs, meta: { total: runs.length, has_more: false } } }),
  useAutomations:    () => ({ data: { data: automations } }),
  useAutomationRuns: () => ({ data: { data: [] } }),
}));
const { default: ProjectOverview } = await import("./projects-overview");

function render() {
  let renderer: TestRenderer.ReactTestRenderer | undefined;
  act(() => {
    renderer = TestRenderer.create(
      <MemoryRouter initialEntries={["/projects/tierrapay"]}>
        <Routes>
          <Route path="/projects/:id" element={<ProjectOverview />} />
        </Routes>
      </MemoryRouter>,
    );
  });
  return renderer!;
}

describe("ProjectOverview", () => {
  beforeEach(() => { teardown = setupReactTestEnv(); });
  afterEach(() => { teardown?.(); runs = []; automations = []; });

  test("shows tiles, linked automations and recent failures", () => {
    runs = [
      { id: "r1", title: "Scan TierraPay", lifecycle: { status: { kind: "failed" } }, timestamps: { created_at: now } },
      { id: "r2", title: "Ticket 12", lifecycle: { status: { kind: "running" } }, timestamps: { created_at: now } },
    ];
    automations = [{
      id: "tierrapay-woodpecker", name: "Woodpecker (TierraPay)", source_automation_id: "woodpecker-loop",
      project_id: "tierrapay", triggers: [{ type: "schedule", id: "s", enabled: true, expression: "*/5 * * * *" }],
    }];
    const text = textContent(render().toJSON());
    expect(text).toContain("Running1");
    expect(text).toContain("Failed (24h)1");
    expect(text).toContain("Woodpecker (TierraPay)");
    expect(text).toContain("*/5 * * * *");
    expect(text).toContain("Scan TierraPay");
  });
});
