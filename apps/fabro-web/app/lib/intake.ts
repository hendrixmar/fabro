import type {
  IntakeInitiativeDetail,
  IntakeReadinessCheck,
  IntakeReadinessReport,
  IntakeRunRecord,
  IntakeSetupStatus,
} from "@qltysh/fabro-api-client";

/**
 * Capability states, derived from real intake data only.
 *
 * A missing readiness report (never checked), a paused project, or any check
 * that is not `pass` is never reported as ready: unknown, missing, and stale
 * checks are deliberately not treated as success.
 */
export type IntakeCapabilityState =
  | "not-configured"
  | "ready"
  | "paused"
  | "blocked";

export const INTAKE_CAPABILITY_LABEL: Record<IntakeCapabilityState, string> = {
  "not-configured": "Not configured",
  ready: "Ready for authoring",
  paused: "Paused",
  blocked: "Blocked",
};

export function intakeCapabilityState(
  status: IntakeSetupStatus | undefined,
): IntakeCapabilityState {
  if (!status || status.setup_required || !status.binding) return "not-configured";
  if (status.paused) return "paused";
  return status.readiness?.authoring_ready ? "ready" : "blocked";
}

/** Execution readiness is reported separately and is never implied by authoring. */
export type IntakeExecutionState = "setup-required" | "ready";

export const INTAKE_EXECUTION_LABEL: Record<IntakeExecutionState, string> = {
  "setup-required": "Execution setup required",
  ready: "Ready for supervised execution",
};

export function intakeExecutionState(
  status: IntakeSetupStatus | undefined,
): IntakeExecutionState {
  return status?.readiness?.execution_ready ? "ready" : "setup-required";
}

/** Every check that is not a verified pass, each with its own next action. */
export function blockingChecks(
  report: IntakeReadinessReport | null | undefined,
): IntakeReadinessCheck[] {
  return (report?.checks ?? []).filter((check) => check.outcome !== "pass");
}

/**
 * Provider errors already carry an actionable message from the API; anything
 * else gets the caller's fallback so the screen never renders an empty alert.
 */
export function intakeErrorMessage(cause: unknown, fallback: string): string {
  return cause instanceof Error && cause.message ? cause.message : fallback;
}

const STAGE_LABELS: Record<string, string> = {
  prd: "PRD",
  spec: "Specification",
  design: "Design",
  freeze: "Freeze",
  execute: "Prepare execution",
  cancel: "Cancel",
};

export function stageLabel(stage: string | null | undefined): string {
  if (!stage) return "No document stage yet";
  return STAGE_LABELS[stage] ?? stage;
}

/** Document stages in the order the factory drafts them. */
export const INTAKE_DOC_STAGES = ["prd", "spec", "design"] as const;

/** Stages an operator can dispatch directly from the detail screen. */
export const INTAKE_RUNNABLE_STAGES = ["prd", "spec", "design", "freeze"] as const;

/** The approval action available while a document awaits review, or null. */
export function approveActionLabel(
  stage: string | null | undefined,
): string | null {
  switch (stage) {
    case "prd":
      return "Approve and draft next document";
    case "spec":
      return "Approve and draft next document";
    case "design":
      return "Approve design";
    default:
      return null;
  }
}

/**
 * True when the latest dispatch never resolved. Recovery is a reconciliation,
 * never a retry: repeating an unknown submission could duplicate its effect.
 */
export function hasUnknownOutcome(
  run: IntakeRunRecord | null | undefined,
): boolean {
  if (!run) return false;
  if (run.kind === "succeeded" || run.kind === "failed" || run.kind === "error") {
    return false;
  }
  return run.outcome === "unknown";
}

/** Live stage activity, from persisted run/Plane state rather than optimism. */
export function initiativeActivity(
  run: IntakeRunRecord | null | undefined,
): string {
  if (!run) return "No dispatch recorded for this stage.";
  return [run.event, run.outcome, run.message]
    .filter((part): part is string => typeof part === "string" && part.trim() !== "")
    .join(" · ");
}

/** Digests recorded in the machine block or a document's front matter. */
export function approvedDigests(
  detail: IntakeInitiativeDetail | undefined,
): Array<{ stage: string; digest: string }> {
  if (!detail) return [];
  const found: Array<{ stage: string; digest: string }> = [];
  const block = detail.block ?? {};
  for (const [key, value] of Object.entries(block)) {
    if (key.endsWith(".sha256") && typeof value === "string" && value !== "") {
      found.push({ stage: key.slice(0, -".sha256".length), digest: value });
    }
  }
  for (const stage of INTAKE_DOC_STAGES) {
    const meta = detail.docs?.[stage]?.meta;
    if (!meta) continue;
    for (const [key, value] of Object.entries(meta)) {
      if (
        typeof value === "string" &&
        value !== "" &&
        /digest|approx|approved|sha/i.test(key) &&
        !found.some((entry) => entry.stage === stage)
      ) {
        found.push({ stage, digest: value });
      }
    }
  }
  return found;
}

/** Document front-matter keys that are not digests, for the metadata rows. */
export function documentMetaRows(
  meta: Record<string, unknown> | undefined,
): Array<{ key: string; value: string }> {
  if (!meta) return [];
  return Object.entries(meta)
    .filter(([key]) => !/digest|sha/i.test(key))
    .map(([key, value]) => ({
      key,
      value:
        typeof value === "string"
          ? value
          : typeof value === "number" || typeof value === "boolean"
            ? String(value)
            : JSON.stringify(value),
    }));
}

/**
 * The intake profile's Plane states and labels, mirroring `intake/setup_plane.py`.
 * This is a wire contract with the bridge, not a local preference: the setup
 * preview must name the exact rows the server will create.
 */
const INTAKE_PLANE_STATES: ReadonlyArray<readonly [string, string]> = [
  ["Intake", "backlog"],
  ["Awaiting Client PRD", "backlog"],
  ["Approved PRD", "unstarted"],
  ["Awaiting Client Spec", "backlog"],
  ["Approved Spec", "unstarted"],
  ["Awaiting Client Design", "backlog"],
  ["Approved Design", "unstarted"],
  ["Backlog", "backlog"],
  ["Todo", "unstarted"],
  ["Cancelled", "cancelled"],
];

const INTAKE_PLANE_LABELS = ["initiative", "needs-operator"];

export interface PlaneSetupPlan {
  createStates: Array<{ name: string; group: string }>;
  existingStates: string[];
  createLabels: string[];
  existingLabels: string[];
  /** Existing states whose group contradicts the intake workflow. */
  incompatible: string[];
}

/** What setting up feature intake will write into the selected Plane project. */
export function planeSetupPlan(
  existingStates: Array<{ name: string; group?: string | null }>,
  existingLabels: Array<{ name: string }>,
): PlaneSetupPlan {
  const existing = (name: string) => existingStates.find((s) => s.name === name);
  return {
    createStates: INTAKE_PLANE_STATES.filter(([name]) => !existing(name)).map(
      ([name, group]) => ({ name, group }),
    ),
    existingStates: INTAKE_PLANE_STATES.filter(([name]) => existing(name)).map(
      ([name]) => name,
    ),
    createLabels: INTAKE_PLANE_LABELS.filter((name) =>
      !existingLabels.some((label) => label.name === name),
    ),
    existingLabels: INTAKE_PLANE_LABELS.filter((name) =>
      existingLabels.some((label) => label.name === name),
    ),
    incompatible: INTAKE_PLANE_STATES.filter(
      ([name, group]) => existing(name) && existing(name)?.group !== group,
    ).map(
      ([name, group]) =>
        `${name} has group ${existing(name)?.group} but intake needs ${group}`,
    ),
  };
}
