import type { Automation, GithubRepository } from "@qltysh/fabro-api-client";

import type { ApiError } from "./api-client";

/** Project ids use the same lower-case slug shape as automation ids. */
export const PROJECT_ID_PATTERN = /^[a-z0-9][a-z0-9-]{0,62}$/;

const MAX_AUTOMATION_ID_LENGTH = 63;

/** `owner/repo` for a canonical repository slug, or null when it is unusable. */
export function canonicalRepositorySlug(value: string): string | null {
  const trimmed = value
    .trim()
    .replace(/^git@github\.com:/i, "")
    .replace(/^https?:\/\/(?:www\.)?github\.com\//i, "")
    .replace(/\.git$/i, "")
    .replace(/^\/+|\/+$/g, "");
  const parts = trimmed.split("/").filter(Boolean);
  if (parts.length !== 2) return null;
  if (!/^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/.test(parts[0])) return null;
  if (!/^[A-Za-z0-9._-]+$/.test(parts[1])) return null;
  return `${parts[0]}/${parts[1]}`;
}

/**
 * Why authoring and execution stay blocked for a repository that can still be
 * connected for visibility.
 */
export function repositoryBlockReason(repository: GithubRepository): string | null {
  if (repository.disabled) {
    return "This repository is disabled on GitHub. It can be connected, but authoring and runs stay blocked until it is re-enabled.";
  }
  if (repository.archived) {
    return "This repository is archived. It can be connected, but authoring and runs stay blocked until it is unarchived.";
  }
  if (!repository.default_branch) {
    return "This repository has no commits yet. It can be connected, but authoring and runs stay blocked until it has a default branch.";
  }
  return null;
}

export type ProjectAutomationState =
  | "error"
  | "configuration"
  | "enabled"
  | "disabled";

export const PROJECT_AUTOMATION_STATE_LABEL: Record<ProjectAutomationState, string> = {
  error:         "Error",
  configuration: "Configuration needed",
  enabled:       "Enabled",
  disabled:      "Disabled",
};

/** Display state for a project instance, from its real triggers and last error. */
export function projectAutomationState(
  automation: Automation,
): ProjectAutomationState {
  if (automation.last_error) return "error";
  if (!automation.environment_id) return "configuration";
  return automation.triggers.some((trigger) => trigger.enabled)
    ? "enabled"
    : "disabled";
}

/** `<project>-<source>`, lower-case, within the automation id length limit. */
export function proposeProjectAutomationId(
  projectId: string,
  sourceId: string,
): string {
  return slugify(`${projectId}-${sourceId}`, MAX_AUTOMATION_ID_LENGTH);
}

/** Project id derived from a display name or repository name. */
export function proposeProjectId(source: string): string {
  return slugify(source, MAX_AUTOMATION_ID_LENGTH);
}

function slugify(value: string, maxLength: number): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, maxLength)
    .replace(/-+$/, "");
}

/**
 * Attention line for a project's automation instances: erroring instances and
 * instances still missing an environment, or null when none need attention.
 */
export function projectAutomationAttention(automations: Automation[]): string | null {
  const errored = automations.filter((automation) => automation.last_error).length;
  const unconfigured = automations.filter(
    (automation) => !automation.last_error && !automation.environment_id,
  ).length;
  const parts: string[] = [];
  if (errored > 0) parts.push(`${errored} with a failed run`);
  if (unconfigured > 0) parts.push(`${unconfigured} missing an environment`);
  return parts.length > 0 ? parts.join(" · ") : null;
}

/**
 * The existing project id reported by a duplicate-repository 409, or null when
 * the conflict is something else (a stale revision, a duplicate project id).
 */
export function connectedProjectIdFromConflict(error: ApiError): string | null {
  const body = error.body as
    | { errors?: Array<{ code?: unknown; detail?: unknown }> }
    | null
    | undefined;
  const entry = body?.errors?.[0];
  if (entry?.code !== "project_repository_connected") return null;
  const detail = typeof entry.detail === "string" ? entry.detail : "";
  const match = detail.match(/ as project ([a-z0-9][a-z0-9-]*)$/);
  return match ? match[1] : null;
}
