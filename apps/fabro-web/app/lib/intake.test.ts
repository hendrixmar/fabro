import { describe, expect, mock, test } from "bun:test";

import { ApiError } from "./api-client";
import {
  hasUnknownOutcome,
  intakeCapabilityState,
  intakeExecutionState,
  planeSetupPlan,
} from "./intake";
import { newRequestChatKey, streamIntakeAdvisor } from "./intake-chat";

const encoder = new TextEncoder();

function streamResponse(chunks: string[], status = 200, headers: HeadersInit = {}) {
  return new Response(
    new ReadableStream({
      start(controller) {
        for (const chunk of chunks) {
          controller.enqueue(encoder.encode(chunk));
        }
        controller.close();
      },
    }),
    { status, headers: { "content-type": "text/event-stream", ...headers } },
  );
}

describe("intake chat stream", () => {
  test("joins advisor chunks split across frames and stops at the done event", async () => {
    const chunks: string[] = [];
    const fetchMock = mock(() =>
      Promise.resolve(
        streamResponse([
          'event: message\ndata: "Hel',
          'lo"\n\ndata: " wor',
          'ld"\n\nevent: done\ndata: {}\n\n',
        ]),
      ),
    );

    await streamIntakeAdvisor({
      projectId: "tierra-pay",
      key: newRequestChatKey("tierra-pay"),
      text: "Sharpen the goal",
      fetchImpl: fetchMock,
      onChunk: (chunk) => chunks.push(chunk),
    });

    expect(fetchMock.mock.calls[0]?.[0]).toBe(
      "/api/v1/projects/tierra-pay/intake/chat/nueva-tierra-pay",
    );
    expect(JSON.parse(fetchMock.mock.calls[0]?.[1]?.body as string)).toEqual({
      text: "Sharpen the goal",
    });
    expect(chunks.join("")).toBe("Hello world");
  });

  test("surfaces an unreachable bridge as an actionable error", async () => {
    const fetchMock = mock(() =>
      Promise.resolve(
        new Response(
          JSON.stringify({
            errors: [{ code: "intake_bridge_unreachable", detail: "the feature-intake bridge is unreachable" }],
          }),
          { status: 503, headers: { "content-type": "application/json" } },
        ),
      ),
    );

    const error = await streamIntakeAdvisor({
      projectId: "tierra-pay",
      key: "nueva",
      text: "hi",
      fetchImpl: fetchMock,
      onChunk: () => {},
    }).catch((cause: unknown) => cause);

    expect(error).toBeInstanceOf(ApiError);
    expect((error as ApiError).status).toBe(503);
    expect((error as ApiError).message).toContain("unreachable");
  });
});

describe("intake capability states", () => {
  test("an unbound project is not configured, never ready", () => {
    expect(
      intakeCapabilityState({ binding: null, paused: false, setup_required: true }),
    ).toBe("not-configured");
    expect(intakeExecutionState({ binding: null, paused: false, setup_required: true })).toBe(
      "setup-required",
    );
  });

  test("missing, failed and stale readiness never reports ready", () => {
    const base = { binding: "tierra-pay", setup_required: false, paused: false };
    expect(intakeCapabilityState({ ...base })).toBe("blocked");
    expect(
      intakeCapabilityState({
        ...base,
        readiness: { authoring_ready: false, execution_ready: false, checks: [] },
      }),
    ).toBe("blocked");
    expect(
      intakeCapabilityState({
        ...base,
        readiness: { authoring_ready: true, execution_ready: false, checks: [] },
      }),
    ).toBe("ready");
    expect(
      intakeExecutionState({
        ...base,
        readiness: { authoring_ready: true, execution_ready: false, checks: [] },
      }),
    ).toBe("setup-required");
    expect(
      intakeCapabilityState({
        ...base,
        paused: true,
        readiness: { authoring_ready: true, execution_ready: true, checks: [] },
      }),
    ).toBe("paused");
  });
});

describe("plane setup plan", () => {
  const existing = [
    { name: "Backlog", group: "backlog" },
    { name: "Todo", group: "unstarted" },
    { name: "Awaiting Client PRD", group: "backlog" },
  ];

  test("creates only the missing states and labels", () => {
    const plan = planeSetupPlan(existing, [{ name: "initiative" }]);
    expect(plan.createStates.map((state) => state.name)).toEqual([
      "Intake",
      "Approved PRD",
      "Awaiting Client Spec",
      "Approved Spec",
      "Awaiting Client Design",
      "Approved Design",
      "Cancelled",
    ]);
    expect(plan.existingStates).toEqual([
      "Awaiting Client PRD",
      "Backlog",
      "Todo",
    ]);
    expect(plan.createLabels).toEqual(["needs-operator"]);
    expect(plan.incompatible).toEqual([]);
  });

  test("flags an existing state whose group contradicts the intake workflow", () => {
    const plan = planeSetupPlan(
      [{ name: "Backlog", group: "started" }],
      [{ name: "initiative" }, { name: "needs-operator" }],
    );
    expect(plan.incompatible).toEqual([
      "Backlog has group started but intake needs backlog",
    ]);
    expect(plan.createLabels).toEqual([]);
  });
});

describe("unknown outcomes", () => {
  test("only an unresolved dispatch is unknown", () => {
    expect(hasUnknownOutcome(null)).toBe(false);
    expect(
      hasUnknownOutcome({ stage: "prd", event: "intent", kind: "running", outcome: "running", message: "" }),
    ).toBe(false);
    expect(
      hasUnknownOutcome({ stage: "prd", event: "submitted", kind: "succeeded", outcome: "unknown", message: "" }),
    ).toBe(false);
    expect(
      hasUnknownOutcome({ stage: "prd", event: "unknown", kind: "unknown", outcome: "unknown", message: "" }),
    ).toBe(true);
  });
});
