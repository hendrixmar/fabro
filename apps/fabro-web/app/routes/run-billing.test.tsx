import { afterEach, describe, expect, mock, test } from "bun:test";
import TestRenderer from "react-test-renderer";

import type {
  RunBilling,
  StageTiming,
} from "@qltysh/fabro-api-client";

import { makeBilledTokenCounts } from "../lib/test-fixtures";

function stageTiming(wall_time_ms = 0, inference_time_ms = 0, tool_time_ms = 0): StageTiming {
  return {
    wall_time_ms,
    inference_time_ms,
    tool_time_ms,
    active_time_ms: inference_time_ms + tool_time_ms,
  };
}

let currentBilling: RunBilling | undefined;

mock.module("../lib/queries", () => ({
  useRunBilling: () => ({ data: currentBilling }),
}));

const { default: RunBillingRoute } = await import("./run-billing");

function billing(overrides: Partial<RunBilling> = {}): RunBilling {
  return {
    stages: [],
    totals: {
      timing: stageTiming(),
      ...makeBilledTokenCounts(),
    },
    by_model: [],
    ...overrides,
  };
}

function renderBilling(data: RunBilling): TestRenderer.ReactTestRenderer {
  currentBilling = data;
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

  let renderer: TestRenderer.ReactTestRenderer | undefined;
  TestRenderer.act(() => {
    renderer = TestRenderer.create(<RunBillingRoute params={{ id: "run_1" }} />);
  });
  return renderer!;
}

function textFromNode(node: ReturnType<TestRenderer.ReactTestRenderer["toJSON"]>): string {
  if (!node) return "";
  if (typeof node === "string") return node;
  if (Array.isArray(node)) return node.map(textFromNode).join(" ");
  return (node.children ?? []).map(textFromNode).join(" ");
}

function textFromInstance(node: TestRenderer.ReactTestInstance): string {
  return node.children
    .map((child) => (typeof child === "string" ? child : textFromInstance(child)))
    .join("");
}

describe("RunBilling", () => {
  afterEach(() => {
    currentBilling = undefined;
    delete (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT;
  });

  test("shows a no-model-usage empty state when every stage is non-billable", () => {
    const renderer = renderBilling(
      billing({
        stages: [
          {
            stage: { id: "start", name: "start" },
            model: null,
            billing: makeBilledTokenCounts(),
            timing: stageTiming(),
            state: "succeeded",
          },
          {
            stage: { id: "command", name: "command" },
            model: null,
            billing: makeBilledTokenCounts(),
            timing: stageTiming(61000),
            state: "succeeded",
          },
        ],
        totals: {
          timing: stageTiming(61000),
          ...makeBilledTokenCounts(),
        },
      }),
    );

    const text = textFromNode(renderer.toJSON());
    expect(text).toContain("No model usage");
    expect(text).toContain("This run didn't call any AI models.");
    expect(text).not.toContain("No stages yet");
    expect(text).not.toContain("By model");
    expect(text).not.toContain("start");
    expect(text).not.toContain("command");
  });

  test("renders mixed LLM and non-LLM rows while counting only LLM rows by model", () => {
    const renderer = renderBilling(
      billing({
        stages: [
          {
            stage: { id: "start", name: "start" },
            model: null,
            billing: makeBilledTokenCounts(),
            timing: stageTiming(),
            state: "succeeded",
          },
          {
            stage: { id: "agent", name: "agent" },
            model: {
              provider: "anthropic",
              model_id: "claude-sonnet-4-5",
            },
            billing: makeBilledTokenCounts({
              input_tokens: 1200,
              output_tokens: 300,
              total_tokens: 1500,
              total_usd_micros: 240000,
            }),
            timing: stageTiming(42000),
            state: "succeeded",
          },
        ],
        totals: {
          timing: stageTiming(42000),
          ...makeBilledTokenCounts({
            input_tokens: 1200,
            output_tokens: 300,
            total_tokens: 1500,
            total_usd_micros: 240000,
          }),
        },
        by_model: [
          {
            model: {
              provider: "anthropic",
              model_id: "claude-sonnet-4-5",
            },
            stages: 1,
            billing: makeBilledTokenCounts({
              input_tokens: 1200,
              output_tokens: 300,
              total_tokens: 1500,
              total_usd_micros: 240000,
            }),
          },
        ],
      }),
    );

    const text = textFromNode(renderer.toJSON());
    expect(text).not.toContain("start");
    expect(text).toContain("agent");
    expect(text).toContain("By model");

    const footers = renderer.root.findAll((node) => node.type === "tfoot");
    const byModelFooterCells = footers[1].findAll((node) => node.type === "td");
    expect(textFromInstance(byModelFooterCells[1])).toBe("1");
  });

  test("keeps the empty state for runs with no stages", () => {
    const renderer = renderBilling(billing());

    const text = textFromNode(renderer.toJSON());
    expect(text).toContain("No stages yet");
    expect(text).toContain("Stages will appear as soon as the run starts executing.");
  });

  test("keeps ACP model usage separate and leaves unreported cost unavailable", () => {
    const codexBilling = makeBilledTokenCounts({
      input_tokens:      100,
      output_tokens:     30,
      cache_read_tokens: 40,
      reasoning_tokens:  10,
      total_tokens:      180,
    });
    const ompBilling = makeBilledTokenCounts({
      input_tokens:      200,
      output_tokens:     50,
      cache_read_tokens: 80,
      total_tokens:      330,
      total_usd_micros:  12_300,
    });

    const renderer = renderBilling(
      billing({
        stages: [
          {
            stage:   { id: "codex", name: "codex" },
            model:   { provider: "codex", model_id: "gpt-5.4" },
            billing: codexBilling,
            timing:  stageTiming(),
            state:   "succeeded",
          },
          {
            stage:   { id: "omp", name: "omp" },
            model:   { provider: "omp", model_id: "gpt-5.4" },
            billing: ompBilling,
            timing:  stageTiming(),
            state:   "succeeded",
          },
        ],
        totals: {
          timing: stageTiming(),
          ...makeBilledTokenCounts({
            input_tokens:      300,
            output_tokens:     80,
            cache_read_tokens: 120,
            reasoning_tokens:  10,
            total_tokens:      510,
            total_usd_micros:  12_300,
          }),
        },
        by_model: [
          {
            model:   { provider: "codex", model_id: "gpt-5.4" },
            stages:  1,
            billing: codexBilling,
          },
          {
            model:   { provider: "omp", model_id: "gpt-5.4" },
            stages:  1,
            billing: ompBilling,
          },
        ],
      }),
    );

    const tables = renderer.root.findAll((node) => node.type === "table");
    const byModelBody = tables[1].find((node) => node.type === "tbody");
    const rowsByModel = new Map(
      byModelBody.findAll((node) => node.type === "tr").map((row) => {
        const cells = row
          .findAll((node) => node.type === "td")
          .map((cell) => textFromInstance(cell));
        return [cells[0], cells] as const;
      }),
    );

    expect(rowsByModel.get("codex:gpt-5.4")).toEqual([
      "codex:gpt-5.4",
      "1",
      "100 / 40",
      "—",
    ]);
    expect(rowsByModel.get("omp:gpt-5.4")).toEqual([
      "omp:gpt-5.4",
      "1",
      "200 / 50",
      "$0.01",
    ]);
    expect(textFromNode(renderer.toJSON())).not.toContain("$0.00");
  });

  test("renders an in-flight row with live billing and includes its elapsed time in the footer", () => {
    const originalNow = Date.now;
    // Pin "now" to 30s after the in-flight row started.
    const startedAt = "2026-04-29T12:00:00.000Z";
    const fakeNow = new Date("2026-04-29T12:00:30.000Z").getTime();
    Date.now = () => fakeNow;

    try {
      const renderer = renderBilling(
        billing({
          stages: [
            {
              stage: { id: "in-flight", name: "in-flight" },
              model: {
                provider: "anthropic",
                model_id: "claude-opus-4-6",
                speed: "fast",
              },
              billing: makeBilledTokenCounts({
                input_tokens: 1200,
                output_tokens: 300,
                total_tokens: 1500,
                total_usd_micros: 240000,
              }),
              timing: stageTiming(),
              started_at: startedAt,
              state: "running",
            },
          ],
          totals: {
            timing: stageTiming(),
            ...makeBilledTokenCounts({
              input_tokens: 1200,
              output_tokens: 300,
              total_tokens: 1500,
              total_usd_micros: 240000,
            }),
          },
          by_model: [
            {
              model: {
                provider: "anthropic",
                model_id: "claude-opus-4-6",
                speed: "fast",
              },
              stages: 1,
              billing: makeBilledTokenCounts({
                input_tokens: 1200,
                output_tokens: 300,
                total_tokens: 1500,
                total_usd_micros: 240000,
              }),
            },
          ],
        }),
      );

      const text = textFromNode(renderer.toJSON());
      // Empty-state must NOT show — the table should appear as soon as the
      // first stage starts.
      expect(text).not.toContain("No stages yet");
      expect(text).toContain("in-flight");
      expect(text).toContain("anthropic:claude-opus-4-6 · fast");
      expect(text).toContain("1.2k");
      expect(text).toContain("300");
      expect(text).toContain("$0.24");
      expect(text).toContain("By model");

      // Both the row's runtime cell and the footer total should reflect
      // ~30s elapsed since started_at.
      expect(text).toContain("30s");

      const footers = renderer.root.findAll((node) => node.type === "tfoot");
      const footerCells = footers[0].findAll((node) => node.type === "td");
      // The Run time column in the footer is index 3 (Total / [empty Model] /
      // Tokens / Run time / Billing).
      const footerRuntime = textFromInstance(footerCells[3]);
      expect(footerRuntime).toContain("30s");
    } finally {
      Date.now = originalNow;
    }
  });
});
