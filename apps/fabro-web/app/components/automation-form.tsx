import { useRef, type ReactNode } from "react";
import { Link } from "react-router";
import { Switch } from "@headlessui/react";
import type {
  Automation,
  AutomationGitWorkflowSource,
  AutomationTrigger,
  Environment,
  Run,
  RunProjection,
  WorkflowSettings,
} from "@qltysh/fabro-api-client";

import {
  findApiTrigger,
  findPlaneTrigger,
  findScheduleTrigger,
  gitTarget,
  type GitRunTarget,
} from "../lib/automation";
import { Panel, Row } from "./settings-panel";
import { INPUT_CLASS } from "./ui";
import { isCloneBasedEnvironment, providerLabel } from "../lib/environment-providers";
import { sandboxRuntime } from "../lib/run-sandbox-lifecycle";
import { usePlaneProjectMetadata, usePlaneProjects } from "../lib/queries";

export interface AutomationFormValues {
  id: string;
  name: string;
  description: string;
  environmentId: string;
  targetRepository: string;
  targetBranch: string;
  targetTag: string;
  targetSha: string;
  workflow: string;
  usesRemoteWorkflow: boolean;
  workflowSourceRepository: string;
  workflowSourceBranch: string;
  workflowSourceTag: string;
  workflowSourceSha: string;
  manualEnabled: boolean;
  scheduleEnabled: boolean;
  cron: string;
  planeEnabled: boolean;
  planeProjectId: string;
  planeReadyStateId: string;
  planeInProgressStateId: string;
  planeDoneStateId: string;
  planeCancelledStateId: string;
  planeFailureLabelId: string;
  planeDefaultHarness: "codex" | "omp";
  planeCodexLabelId: string;
  planeOmpLabelId: string;
  planePollIntervalSeconds: string;
  planeMaxConcurrency: string;
}

export const EMPTY_AUTOMATION_FORM: AutomationFormValues = {
  id:                         "",
  name:                       "",
  description:                "",
  environmentId:   "",
  targetRepository:           "",
  targetBranch:               "main",
  targetTag:                  "",
  targetSha:                  "",
  workflow:                   "",
  usesRemoteWorkflow:         false,
  workflowSourceRepository:  "",
  workflowSourceBranch:      "main",
  workflowSourceTag:         "",
  workflowSourceSha:         "",
  manualEnabled:             true,
  scheduleEnabled:           false,
  cron:                      "0 9 * * 1-5",
  planeEnabled:              false,
  planeProjectId:            "",
  planeReadyStateId:         "",
  planeInProgressStateId:    "",
  planeDoneStateId:          "",
  planeCancelledStateId:     "",
  planeFailureLabelId:       "",
  planeDefaultHarness:       "codex",
  planeCodexLabelId:         "",
  planeOmpLabelId:           "",
  planePollIntervalSeconds:  "60",
  planeMaxConcurrency:       "3",
};

const CRON_PRESETS: ReadonlyArray<{ label: string; value: string }> = [
  { label: "Every hour",      value: "0 * * * *" },
  { label: "Daily 9:00 UTC",  value: "0 9 * * *" },
  { label: "Weekdays 9:00",   value: "0 9 * * 1-5" },
  { label: "Mondays 8:00",    value: "0 8 * * 1" },
];

export function automationToFormValues(automation: Automation): AutomationFormValues {
  const apiTrigger = findApiTrigger(automation);
  const scheduleTrigger = findScheduleTrigger(automation);
  const planeTrigger = findPlaneTrigger(automation);
  const target = gitTarget(automation.target);
  const workflowSource = automation.workflow_source;
  return {
    id:                         automation.id,
    name:                       automation.name,
    description:                automation.description ?? "",
    environmentId:   automation.environment_id ?? "",
    targetRepository:           target?.repo ?? "",
    targetBranch:               target?.branch ?? EMPTY_AUTOMATION_FORM.targetBranch,
    targetTag:                  target?.tag ?? "",
    targetSha:                  target?.sha ?? "",
    workflow:                   automation.workflow,
    usesRemoteWorkflow:         workflowSource != null,
    workflowSourceRepository:  workflowSource?.repo ?? "",
    workflowSourceBranch:      workflowSource?.branch ?? "main",
    workflowSourceTag:         workflowSource?.tag ?? "",
    workflowSourceSha:         workflowSource?.sha ?? "",
    manualEnabled:             apiTrigger?.enabled ?? false,
    scheduleEnabled:           scheduleTrigger?.enabled ?? false,
    cron:                      scheduleTrigger?.expression ?? "0 9 * * 1-5",
    planeEnabled:             planeTrigger?.enabled ?? false,
    planeProjectId:           planeTrigger?.project_id ?? "",
    planeReadyStateId:        planeTrigger?.ready_state_id ?? "",
    planeInProgressStateId:   planeTrigger?.in_progress_state_id ?? "",
    planeDoneStateId:         planeTrigger?.done_state_id ?? "",
    planeCancelledStateId:    planeTrigger?.cancelled_state_id ?? "",
    planeFailureLabelId:      planeTrigger?.failure_label_id ?? "",
    planeDefaultHarness:      planeTrigger?.default_harness === "omp" ? "omp" : "codex",
    planeCodexLabelId:        planeTrigger?.codex_label_id ?? "",
    planeOmpLabelId:          planeTrigger?.omp_label_id ?? "",
    planePollIntervalSeconds: String(planeTrigger?.poll_interval_seconds ?? 60),
    planeMaxConcurrency:      String(planeTrigger?.max_concurrency ?? 3),
  };
}

export function automationFormValuesFromRun(
  run: Run,
  runState?: RunProjection | null,
  settings?: WorkflowSettings | null,
  environments?: Environment[],
): AutomationFormValues {
  const name = firstPresentString(
    run.title,
    run.workflow.name,
    run.workflow.graph_name,
    run.workflow.slug,
    "New automation",
  );
  const workflowName = firstPresentString(
    run.workflow.name,
    run.workflow.graph_name,
    name,
  );
  const canonicalTarget = gitTarget(runState?.spec.target);
  const targetRepository = canonicalTarget?.repo
    ?? githubRepositoryFromSettings(settings)
    ?? githubRepositoryName(run.repository?.name)
    ?? githubRepositoryFromOriginUrl(run.repository?.origin_url)
    ?? "";
  const cloneBranch = sandboxRuntime(run.sandbox)?.clone_branch;
  const sourceEnvironment = settings?.run?.environment;
  const environmentId = sourceEnvironment
    && environments?.some(
      (environment) => environment.id === sourceEnvironment.id && isCloneBasedEnvironment(environment),
    )
      ? sourceEnvironment.id
      : "";
  return {
    ...EMPTY_AUTOMATION_FORM,
    id: kebabify(name),
    name,
    environmentId,
    targetRepository,
    targetBranch: canonicalTarget?.branch
      ?? cloneBranch
      ?? EMPTY_AUTOMATION_FORM.targetBranch,
    targetTag: canonicalTarget?.tag ?? "",
    targetSha: canonicalTarget?.sha ?? "",
    workflow: run.workflow.slug?.trim() || kebabify(workflowName),
  };
}

export function triggersFromFormValues(values: AutomationFormValues): AutomationTrigger[] {
  const triggers: AutomationTrigger[] = [];
  if (values.manualEnabled) {
    triggers.push({ id: "manual", type: "api", enabled: true });
  }
  if (values.scheduleEnabled) {
    triggers.push({
      id:         "schedule",
      type:       "schedule",
      enabled:    true,
      expression: values.cron.trim(),
    });
  }
  if (values.planeEnabled) {
    triggers.push({
      id:                    "plane-tickets",
      type:                  "plane",
      enabled:               true,
      project_id:            values.planeProjectId.trim(),
      ready_state_id:        values.planeReadyStateId.trim(),
      in_progress_state_id:  values.planeInProgressStateId.trim(),
      done_state_id:         values.planeDoneStateId.trim(),
      cancelled_state_id:    values.planeCancelledStateId.trim(),
      failure_label_id:      values.planeFailureLabelId.trim() || null,
      default_harness:       values.planeDefaultHarness,
      codex_label_id:        values.planeCodexLabelId.trim() || null,
      omp_label_id:          values.planeOmpLabelId.trim() || null,
      poll_interval_seconds: Number(values.planePollIntervalSeconds) || 60,
      max_concurrency:       Number(values.planeMaxConcurrency) || 3,
      max_retries:           1,
    });
  }
  return triggers;
}

export function isFormValid(values: AutomationFormValues): boolean {
  const baseValid =
    values.id.trim() !== "" &&
    values.name.trim() !== "" &&
    values.environmentId.trim() !== "" &&
    values.targetRepository.trim() !== "" &&
    values.targetBranch.trim() !== "" &&
    isOptionalShaValid(values.targetSha) &&
    values.workflow.trim() !== "" &&
    isWorkflowSourceValid(values);
  if (!values.planeEnabled) return baseValid;
  return (
    baseValid &&
    values.planeProjectId.trim() !== "" &&
    values.planeReadyStateId.trim() !== "" &&
    values.planeInProgressStateId.trim() !== "" &&
    values.planeDoneStateId.trim() !== "" &&
    values.planeCancelledStateId.trim() !== ""
  );
}

const GIT_SHA_RE = /^[0-9a-fA-F]{40}$/;

/** An empty SHA means "no pin"; anything else must be a full 40-hex commit id. */
function isOptionalShaValid(sha: string): boolean {
  const trimmed = sha.trim();
  return trimmed === "" || GIT_SHA_RE.test(trimmed);
}

/** Canonical Git target sent in create/replace requests. */
export function targetFromFormValues(values: AutomationFormValues): GitRunTarget {
  return {
    kind:   "git",
    repo:   values.targetRepository.trim(),
    branch: values.targetBranch.trim(),
    tag:    values.targetTag.trim() || undefined,
    sha:    values.targetSha.trim().toLowerCase() || undefined,
  };
}

function isWorkflowSourceValid(values: AutomationFormValues): boolean {
  if (!values.usesRemoteWorkflow) return true;
  return (
    values.workflowSourceRepository.trim() !== "" &&
    values.workflowSourceBranch.trim() !== "" &&
    isOptionalShaValid(values.workflowSourceSha)
  );
}

export function workflowSourceFromFormValues(
  values: AutomationFormValues,
): AutomationGitWorkflowSource | undefined {
  if (!values.usesRemoteWorkflow) return undefined;
  return {
    repo:   values.workflowSourceRepository.trim(),
    branch: values.workflowSourceBranch.trim(),
    tag:    values.workflowSourceTag.trim() || undefined,
    sha:    values.workflowSourceSha.trim().toLowerCase() || undefined,
  };
}

function kebabify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function firstPresentString(...values: Array<string | null | undefined>): string {
  for (const value of values) {
    const trimmed = value?.trim();
    if (trimmed) return trimmed;
  }
  return "";
}

function githubRepositoryFromSettings(
  settings?: WorkflowSettings | null,
): string | null {
  const owner = settings?.run?.scm?.owner;
  const repository = settings?.run?.scm?.repository;
  if (!owner || !repository) return null;
  return githubRepositoryName(`${owner}/${repository}`);
}

function githubRepositoryName(value: string | null | undefined): string | null {
  const trimmed = value?.trim().replace(/\.git$/i, "");
  if (!trimmed) return null;

  const match = trimmed.match(/^([A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)\/([A-Za-z0-9._-]+)$/);
  if (!match) return null;
  return `${match[1]}/${match[2]}`;
}

function githubRepositoryFromOriginUrl(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  if (!trimmed) return null;

  const scpLikeMatch = trimmed.match(
    /^git@github\.com:([A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)\/([A-Za-z0-9._-]+?)(?:\.git)?$/i,
  );
  if (scpLikeMatch) {
    return githubRepositoryName(`${scpLikeMatch[1]}/${scpLikeMatch[2]}`);
  }

  try {
    const url = new URL(trimmed);
    if (url.hostname.toLowerCase() !== "github.com") return null;
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts.length !== 2) return null;
    return githubRepositoryName(`${parts[0]}/${parts[1]}`);
  } catch {
    return null;
  }
}

function describeCron(expression: string): string {
  const trimmed = expression.trim();
  const preset = CRON_PRESETS.find((p) => p.value === trimmed);
  if (preset) return preset.label;
  if (!/^[\d*/,\-\s]+$/.test(trimmed) || trimmed.split(/\s+/).length !== 5) {
    return "Waiting for a valid expression…";
  }
  return "Computed when saved";
}

interface AutomationFormFieldsProps {
  values: AutomationFormValues;
  onChange: (values: AutomationFormValues) => void;
  lockIdAndTarget?: boolean;
  environments?: Environment[];
  environmentsLoading?: boolean;
  environmentsError?: boolean;
}

export function AutomationFormFields({
  values,
  onChange,
  lockIdAndTarget = false,
  environments = [],
  environmentsLoading = false,
  environmentsError = false,
}: AutomationFormFieldsProps) {
  const slugTouchedRef = useRef(values.id.length > 0);
  const shaValid = isOptionalShaValid(values.targetSha);
  const workflowSourceShaValid = isOptionalShaValid(values.workflowSourceSha);
  const compatibleEnvironments = environments
    .filter(isCloneBasedEnvironment)
    .sort((left, right) => left.id.localeCompare(right.id));
  const selectedEnvironmentMissing = values.environmentId !== ""
    && !compatibleEnvironments.some((environment) => environment.id === values.environmentId);

  function patch(partial: Partial<AutomationFormValues>) {
    onChange({ ...values, ...partial });
  }

  function onNameChange(next: string) {
    if (slugTouchedRef.current || lockIdAndTarget) {
      patch({ name: next });
    } else {
      patch({ name: next, id: kebabify(next) });
    }
  }

  function onSlugChange(next: string) {
    slugTouchedRef.current = true;
    patch({ id: kebabify(next) });
  }

  return (
    <>
      <Panel title="Basics">
        <Row title={<Label required>Name</Label>} help="Shown wherever this automation is listed.">
          <input
            type="text"
            name="name"
            aria-label="Automation name"
            value={values.name}
            onChange={(e) => onNameChange(e.target.value)}
            placeholder="Fix Build"
            autoComplete="off"
            className={INPUT_CLASS}
          />
        </Row>
        {lockIdAndTarget ? null : (
          <Row
            title={<Label required>Slug</Label>}
            help={
              <>
                Identifier used in the URL:{" "}
                <span className="font-mono text-fg-2">/automations/{values.id || "<slug>"}</span>
              </>
            }
          >
            <input
              type="text"
              name="slug"
              aria-label="Automation slug"
              value={values.id}
              onChange={(e) => onSlugChange(e.target.value)}
              placeholder="fix-build"
              autoComplete="off"
              spellCheck={false}
              className={`${INPUT_CLASS} font-mono`}
            />
          </Row>
        )}
        <Row title={<Label optional>Description</Label>} help="A short summary teammates will see when browsing automations.">
          <textarea
            name="description"
            aria-label="Automation description"
            value={values.description}
            onChange={(e) => patch({ description: e.target.value })}
            rows={2}
            placeholder="Diagnose and fix CI build failures by analyzing logs and applying targeted patches."
            className={`${INPUT_CLASS} resize-y`}
          />
        </Row>
      </Panel>

      <Panel title="Runtime">
        <Row
          title={<Label required>Environment</Label>}
          help="Server-managed Docker or Daytona environment used whenever this automation runs."
        >
          <div className="space-y-2">
            <select
              name="environment_id"
              aria-label="Automation environment"
              value={values.environmentId}
              onChange={(event) => patch({ environmentId: event.target.value })}
              disabled={environmentsLoading || environmentsError || compatibleEnvironments.length === 0}
              className={`${INPUT_CLASS} font-mono`}
            >
              <option value="">
                {environmentsLoading ? "Loading environments…" : "Select an environment…"}
              </option>
              {selectedEnvironmentMissing ? (
                <option value={values.environmentId} disabled>
                  {values.environmentId} (unavailable)
                </option>
              ) : null}
              {compatibleEnvironments.map((environment) => (
                <option key={environment.id} value={environment.id}>
                  {environment.id} · {providerLabel(environment.provider)}
                </option>
              ))}
            </select>
            {environmentsError ? (
              <p className="text-xs leading-relaxed text-coral">
                Couldn&apos;t load environments. Refresh the page and try again.
              </p>
            ) : !environmentsLoading && compatibleEnvironments.length === 0 ? (
              <p className="text-xs leading-relaxed text-fg-muted">
                No Docker or Daytona environments are available.{" "}
                <Link to="/settings/environments" className="text-mint hover:text-fg">
                  Create an environment
                </Link>{" "}
                before saving this automation.
              </p>
            ) : selectedEnvironmentMissing ? (
              <p className="text-xs leading-relaxed text-coral">
                This environment is no longer available. Choose another environment before saving.
              </p>
            ) : null}
          </div>
        </Row>
      </Panel>

      <Panel title="Run target">
        <Row
          title={<Label required>Repository</Label>}
          help="GitHub repository whose workspace the run changes, in owner/repo form."
        >
          <input
            type="text"
            name="target_repository"
            aria-label="Run target repository"
            value={values.targetRepository}
            onChange={(e) => patch({ targetRepository: e.target.value })}
            placeholder="acme/orders-api"
            autoComplete="off"
            spellCheck={false}
            className={`${INPUT_CLASS} font-mono`}
          />
        </Row>
        <Row
          title={<Label required>Working branch</Label>}
          help="Attached branch retained with the run, including when a tag or exact commit is selected."
        >
          <input
            type="text"
            name="target_branch"
            aria-label="Working branch"
            value={values.targetBranch}
            onChange={(e) => patch({ targetBranch: e.target.value })}
            placeholder="main"
            autoComplete="off"
            spellCheck={false}
            className={`${INPUT_CLASS} font-mono`}
          />
        </Row>
        <Row
          title={<Label optional>Tag</Label>}
          help="Bare tag name resolved when the automation fires. Used only when exact SHA is empty."
        >
          <input
            type="text"
            name="target_tag"
            aria-label="Tag"
            value={values.targetTag}
            onChange={(e) => patch({ targetTag: e.target.value })}
            placeholder="v1.2.3"
            autoComplete="off"
            spellCheck={false}
            className={`${INPUT_CLASS} font-mono`}
          />
        </Row>
        <Row
          title={<Label optional>Exact SHA</Label>}
          help={
            shaValid
              ? "A 40-character commit SHA pins exact content and takes precedence over branch and tag."
              : <span className="text-coral">Enter exactly 40 hexadecimal characters.</span>
          }
        >
          <input
            type="text"
            name="target_sha"
            aria-label="Exact commit SHA"
            aria-invalid={!shaValid}
            value={values.targetSha}
            onChange={(e) => patch({ targetSha: e.target.value })}
            placeholder="0123456789abcdef0123456789abcdef01234567"
            autoComplete="off"
            spellCheck={false}
            className={`${INPUT_CLASS} font-mono`}
          />
        </Row>
      </Panel>

      <Panel title="Workflow">
        <Row
          title={<Label required>Workflow slug</Label>}
          help={
            values.usesRemoteWorkflow
              ? "Dash-separated identifier resolved in the remote workflow checkout."
              : "Dash-separated identifier resolved in the run target checkout."
          }
        >
          <input
            type="text"
            name="workflow_slug"
            aria-label="Workflow slug"
            value={values.workflow}
            onChange={(e) => patch({ workflow: kebabify(e.target.value) })}
            placeholder="patch-cves"
            autoComplete="off"
            spellCheck={false}
            className={`${INPUT_CLASS} font-mono`}
          />
        </Row>
        <Row
          title="Remote workflow"
          help="Load workflow files from a GitHub repository and revision instead of the run target checkout. The repository may match the run target."
        >
          <ToggleSwitch
            checked={values.usesRemoteWorkflow}
            onChange={(usesRemoteWorkflow) => patch({ usesRemoteWorkflow })}
            label="Use a remote workflow"
          />
        </Row>
        {values.usesRemoteWorkflow ? (
          <>
            <Row
              title={<Label required>Workflow repository</Label>}
              help="GitHub owner/repo containing the workflow files."
            >
              <input
                type="text"
                name="workflow_source_repository"
                aria-label="Remote workflow repository"
                value={values.workflowSourceRepository}
                onChange={(e) => patch({ workflowSourceRepository: e.target.value })}
                placeholder="acme/automation-workflows"
                autoComplete="off"
                spellCheck={false}
                className={`${INPUT_CLASS} font-mono`}
              />
            </Row>
            <Row
              title={<Label required>Branch</Label>}
              help="Fallback revision and audit context. An exact SHA does not need to be reachable from this branch."
            >
              <input
                type="text"
                name="workflow_source_branch"
                aria-label="Remote workflow branch"
                value={values.workflowSourceBranch}
                onChange={(e) => patch({ workflowSourceBranch: e.target.value })}
                placeholder="main"
                autoComplete="off"
                spellCheck={false}
                className={`${INPUT_CLASS} font-mono`}
              />
            </Row>
            <Row
              title={<Label optional>Tag</Label>}
              help="Bare tag name resolved when the automation fires. Used only when exact SHA is empty."
            >
              <input
                type="text"
                name="workflow_source_tag"
                aria-label="Remote workflow tag"
                value={values.workflowSourceTag}
                onChange={(e) => patch({ workflowSourceTag: e.target.value })}
                placeholder="v1.2.3"
                autoComplete="off"
                spellCheck={false}
                className={`${INPUT_CLASS} font-mono`}
              />
            </Row>
            <Row
              title={<Label optional>Exact SHA</Label>}
              help={
                workflowSourceShaValid
                  ? "A 40-character commit SHA takes precedence over tag and branch. It is fetched directly and need not be reachable from the named branch."
                  : <span className="text-coral">Enter exactly 40 hexadecimal characters.</span>
              }
            >
              <input
                type="text"
                name="workflow_source_sha"
                aria-label="Remote workflow exact commit SHA"
                aria-invalid={!workflowSourceShaValid}
                value={values.workflowSourceSha}
                onChange={(e) => patch({ workflowSourceSha: e.target.value })}
                placeholder="0123456789abcdef0123456789abcdef01234567"
                autoComplete="off"
                spellCheck={false}
                className={`${INPUT_CLASS} font-mono`}
              />
            </Row>
          </>
        ) : null}
      </Panel>

      <Panel title="Triggers">
        <Row title="Manual / API" help="Start a run by clicking Run in the UI or calling the API.">
          <ToggleSwitch
            checked={values.manualEnabled}
            onChange={(manualEnabled) => patch({ manualEnabled })}
            label="Enable manual and API triggers"
          />
        </Row>
        <Row title="Schedule" help="Start runs automatically on a recurring cron schedule.">
          <ToggleSwitch
            checked={values.scheduleEnabled}
            onChange={(scheduleEnabled) => patch({ scheduleEnabled })}
            label="Enable scheduled triggers"
          />
        </Row>
        {values.scheduleEnabled ? (
          <Row
            title="Cron expression"
            help={
              <>
                Five-field POSIX cron in UTC. Next run:{" "}
                <span className="text-fg-2">{describeCron(values.cron)}</span>
              </>
            }
          >
            <div className="space-y-2">
              <input
                type="text"
                name="cron"
                aria-label="Cron expression"
                value={values.cron}
                onChange={(e) => patch({ cron: e.target.value })}
                placeholder="0 9 * * 1-5"
                autoComplete="off"
                spellCheck={false}
                className={`${INPUT_CLASS} font-mono`}
              />
              <div className="flex flex-wrap gap-1.5">
                {CRON_PRESETS.map((preset) => {
                  const active = preset.value === values.cron;
                  return (
                    <button
                      key={preset.value}
                      type="button"
                      onClick={() => patch({ cron: preset.value })}
                      aria-pressed={active}
                      className={`rounded-full px-2.5 py-1 text-xs transition-colors ${
                        active
                          ? "bg-teal-500/15 text-teal-300 outline-1 -outline-offset-1 outline-teal-500/40"
                          : "bg-overlay text-fg-3 hover:bg-overlay-strong hover:text-fg-2"
                      }`}
                    >
                      {preset.label}
                    </button>
                  );
                })}
              </div>
            </div>
          </Row>
        ) : null}
        <Row title="Plane tickets" help="Poll a Plane project for ready tickets and start runs automatically.">
          <ToggleSwitch
            checked={values.planeEnabled}
            onChange={(planeEnabled) => patch({ planeEnabled })}
            label="Enable Plane ticket trigger"
          />
        </Row>
        {values.planeEnabled ? (
          <PlaneTriggerFields values={values} patch={patch} />
        ) : null}
      </Panel>
    </>
  );
}

function PlaneTriggerFields({
  values,
  patch,
}: {
  values: AutomationFormValues;
  patch: (next: Partial<AutomationFormValues>) => void;
}) {
  const projectsQuery = usePlaneProjects(true);
  const metadataQuery = usePlaneProjectMetadata(values.planeProjectId || undefined);
  const projects = projectsQuery.data?.data ?? [];
  const states = metadataQuery.data?.states ?? [];
  const labels = metadataQuery.data?.labels ?? [];
  const loadError =
    projectsQuery.error instanceof Error
      ? projectsQuery.error.message
      : metadataQuery.error instanceof Error
        ? metadataQuery.error.message
        : null;

  return (
    <>
      {loadError ? (
        <Row title="Plane status" help="Fix the Plane integration before enabling this trigger.">
          <p className="text-sm text-coral">{loadError}</p>
        </Row>
      ) : null}
      <Row title="Project" help="Plane project to poll for ready tickets.">
        <select
          aria-label="Plane project"
          value={values.planeProjectId}
          onChange={(event) => patch({
            planeProjectId: event.target.value,
            planeReadyStateId: "",
            planeInProgressStateId: "",
            planeDoneStateId: "",
            planeCancelledStateId: "",
          })}
          className={INPUT_CLASS}
        >
          <option value="">Select a project</option>
          {projects.map((project) => (
            <option key={project.id} value={project.id}>
              {project.identifier ? `${project.identifier} — ${project.name}` : project.name}
            </option>
          ))}
        </select>
      </Row>
      <Row title="Lifecycle states" help="Exact Ready, In Progress, Done, and Cancelled state IDs.">
        <div className="grid gap-2 sm:grid-cols-2">
          <select aria-label="Ready state" value={values.planeReadyStateId} onChange={(event) => patch({ planeReadyStateId: event.target.value })} className={INPUT_CLASS}>
            <option value="">Ready</option>
            {states.map((state) => <option key={state.id} value={state.id}>{state.name}</option>)}
          </select>
          <select aria-label="In Progress state" value={values.planeInProgressStateId} onChange={(event) => patch({ planeInProgressStateId: event.target.value })} className={INPUT_CLASS}>
            <option value="">In Progress</option>
            {states.map((state) => <option key={state.id} value={state.id}>{state.name}</option>)}
          </select>
          <select aria-label="Done state" value={values.planeDoneStateId} onChange={(event) => patch({ planeDoneStateId: event.target.value })} className={INPUT_CLASS}>
            <option value="">Done</option>
            {states.map((state) => <option key={state.id} value={state.id}>{state.name}</option>)}
          </select>
          <select aria-label="Cancelled state" value={values.planeCancelledStateId} onChange={(event) => patch({ planeCancelledStateId: event.target.value })} className={INPUT_CLASS}>
            <option value="">Cancelled</option>
            {states.map((state) => <option key={state.id} value={state.id}>{state.name}</option>)}
          </select>
        </div>
      </Row>
      <Row title="Default harness" help="Used unless a ticket has a configured Codex or OMP override label.">
        <select
          aria-label="Default harness"
          value={values.planeDefaultHarness}
          onChange={(event) => patch({ planeDefaultHarness: event.target.value === "omp" ? "omp" : "codex" })}
          className={INPUT_CLASS}
        >
          <option value="codex">Codex</option>
          <option value="omp">OMP</option>
        </select>
      </Row>
      <Row title="Harness labels" help="Optional labels that override the default harness. Leave empty to use the default only.">
        <div className="grid gap-2 sm:grid-cols-2">
          <select aria-label="Codex label" value={values.planeCodexLabelId} onChange={(event) => patch({ planeCodexLabelId: event.target.value })} className={INPUT_CLASS}>
            <option value="">Codex label</option>
            {labels.map((label) => <option key={label.id} value={label.id}>{label.name}</option>)}
          </select>
          <select aria-label="OMP label" value={values.planeOmpLabelId} onChange={(event) => patch({ planeOmpLabelId: event.target.value })} className={INPUT_CLASS}>
            <option value="">OMP label</option>
            {labels.map((label) => <option key={label.id} value={label.id}>{label.name}</option>)}
          </select>
        </div>
      </Row>
      <Row title="Polling" help="Interval 15–3600 seconds. Concurrency 1–10.">
        <div className="grid gap-2 sm:grid-cols-3">
          <input aria-label="Poll interval seconds" value={values.planePollIntervalSeconds} onChange={(event) => patch({ planePollIntervalSeconds: event.target.value })} className={INPUT_CLASS} />
          <input aria-label="Max concurrency" value={values.planeMaxConcurrency} onChange={(event) => patch({ planeMaxConcurrency: event.target.value })} className={INPUT_CLASS} />
          <select aria-label="Failure label" value={values.planeFailureLabelId} onChange={(event) => patch({ planeFailureLabelId: event.target.value })} className={INPUT_CLASS}>
            <option value="">Failure label</option>
            {labels.map((label) => <option key={label.id} value={label.id}>{label.name}</option>)}
          </select>
        </div>
      </Row>
    </>
  );
}

function Label({
  children,
  required,
  optional,
}: {
  children: ReactNode;
  required?: boolean;
  optional?: boolean;
}) {
  return (
    <span className="inline-flex items-baseline gap-1.5">
      <span>{children}</span>
      {required ? (
        <span aria-label="required" className="text-coral">
          *
        </span>
      ) : null}
      {optional ? <span className="text-xs font-normal text-fg-muted">Optional</span> : null}
    </span>
  );
}

function ToggleSwitch({
  checked,
  onChange,
  label,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  label: string;
}) {
  return (
    <Switch
      checked={checked}
      onChange={onChange}
      aria-label={label}
      className="group relative inline-flex h-5 w-9 shrink-0 cursor-pointer items-center rounded-full bg-overlay-strong outline-1 -outline-offset-1 outline-line-strong transition-colors duration-150 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-teal-500 data-checked:bg-teal-500"
    >
      <span className="pointer-events-none inline-block size-4 translate-x-0.5 rounded-full bg-fg shadow-sm transition-transform duration-150 group-data-checked:translate-x-[1.125rem]" />
    </Switch>
  );
}
