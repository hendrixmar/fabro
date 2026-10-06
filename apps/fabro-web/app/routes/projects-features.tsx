import { useState } from "react";
import { Link, useParams } from "react-router";
import { useSWRConfig } from "swr";
import { PlusIcon } from "@heroicons/react/20/solid";
import type {
  IntakeInitiativeSummary,
  IntakeSetupStatus,
  Project,
} from "@qltysh/fabro-api-client";

import {
  ActivityLine,
  ChecksNote,
  ReadinessChecks,
  StateText,
} from "../components/intake-panels";
import { Panel, Row } from "../components/settings-panel";
import { EmptyState, ErrorState, LoadingState } from "../components/state";
import { useToast } from "../components/toast";
import {
  ConfirmDialog,
  ErrorMessage,
  INPUT_CLASS,
  PRIMARY_BUTTON_CLASS,
  SECONDARY_BUTTON_CLASS,
} from "../components/ui";
import { ApiError } from "../lib/api-client";
import { formatRelativeTime } from "../lib/format";
import {
  INTAKE_CAPABILITY_LABEL,
  INTAKE_EXECUTION_LABEL,
  blockingChecks,
  intakeCapabilityState,
  intakeErrorMessage,
  intakeExecutionState,
  planeSetupPlan,
  stageLabel,
} from "../lib/intake";
import {
  useDetachProjectIntake,
  useRecheckProjectIntakeReadiness,
  useSetProjectIntakePause,
  useSetupProjectIntake,
} from "../lib/mutations";
import {
  usePlaneProjectMetadata,
  usePlaneProjects,
  useProject,
  useProjectIntake,
  useProjectIntakeInitiatives,
} from "../lib/queries";
import { queryKeys } from "../lib/query-keys";

export function meta() {
  return [{ title: "Feature requests — Fabro" }];
}

const CAPABILITY_TONE: Record<string, string> = {
  "not-configured": "text-fg-muted",
  ready: "text-mint",
  paused: "text-amber",
  blocked: "text-coral",
};

const EXECUTION_TONE: Record<string, string> = {
  "setup-required": "text-amber",
  ready: "text-mint",
};

export default function ProjectFeatures() {
  const { id } = useParams<{ id: string }>();
  const projectQuery = useProject(id);

  if (projectQuery.isLoading && !projectQuery.data) {
    return <LoadingState label="Loading project…" />;
  }

  if (!projectQuery.data) {
    return (
      <ErrorState
        title="Couldn't load this project"
        description="The project could not be read, so its feature requests are unavailable."
      />
    );
  }

  return <FeatureRequests project={projectQuery.data} />;
}

function FeatureRequests({ project }: { project: Project }) {
  const { mutate } = useSWRConfig();
  const statusQuery = useProjectIntake(project.id);
  const status = statusQuery.data;

  if (statusQuery.isLoading && !status) {
    return <LoadingState label="Loading feature intake…" />;
  }

  if (!status) {
    return (
      <UnavailablePanel
        error={statusQuery.error}
        onRetry={() => mutate(queryKeys.intake.status(project.id))}
      />
    );
  }

  const capability = intakeCapabilityState(status);
  const execution = intakeExecutionState(status);

  return (
    <div className="space-y-6">
      {statusQuery.error ? (
        <IntakeInlineError
          message={intakeErrorMessage(
            statusQuery.error,
            "Feature intake could not be re-read. Showing the last known state.",
          )}
          onRetry={() => mutate(queryKeys.intake.status(project.id))}
        />
      ) : null}

      <p className="max-w-prose text-sm/6 text-fg-3">
        Feature requests move through problem → specification → design inside
        Plane, and Fabro drafts the documents. Authoring needs only GitHub,
        Plane, and the authoring runtime; provisioning infrastructure and
        creating implementation tickets are a separate, explicit step.
      </p>

      {capability === "not-configured" ? (
        <SetupIntake project={project} />
      ) : (
        <>
          <Panel title="Capability">
            <Row
              title="Feature authoring"
              help="Drafting and approving documents. Reported from the stored readiness snapshot, never inferred."
            >
              <StateText
                label={INTAKE_CAPABILITY_LABEL[capability]}
                tone={CAPABILITY_TONE[capability]}
              />
            </Row>
            <Row
              title="Execution"
              help="Provisioning and ticket execution need their own readiness and an explicit action."
            >
              <StateText
                label={INTAKE_EXECUTION_LABEL[execution]}
                tone={EXECUTION_TONE[execution]}
              />
            </Row>
            <Row title="Binding" help="Registered intake binding serving this project.">
              <span className="font-mono text-xs text-fg-2">{status.binding}</span>
            </Row>
            <ReadinessRow status={status} projectId={project.id} />
          </Panel>

          <PausePanel projectId={project.id} paused={status.paused} />

          <InitiativesPanel projectId={project.id} />

          <DetachPanel projectId={project.id} />
        </>
      )}
    </div>
  );
}

function ReadinessRow({
  status,
  projectId,
}: {
  status: IntakeSetupStatus;
  projectId: string;
}) {
  const recheck = useRecheckProjectIntakeReadiness(projectId);
  const [error, setError] = useState<string | null>(null);
  const blockers = blockingChecks(status.readiness).length;

  return (
    <Row
      title="Readiness checks"
      help="Authoring and execution are decided by their complete check sets."
    >
      <div className="space-y-3">
        <ReadinessChecks
          report={status.readiness}
          emptyLabel="No readiness snapshot is stored for this binding yet."
        />
        <ChecksNote />
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            disabled={recheck.isMutating}
            onClick={async () => {
              setError(null);
              try {
                await recheck.trigger();
              } catch (cause) {
                setError(
                  intakeErrorMessage(cause, "The readiness probe could not run."),
                );
              }
            }}
            className={SECONDARY_BUTTON_CLASS}
          >
            {recheck.isMutating ? "Re-probing…" : "Re-probe readiness"}
          </button>
          {blockers > 0 ? (
            <span className="text-xs text-fg-muted">
              {blockers} check{blockers === 1 ? "" : "s"} not passing
            </span>
          ) : null}
        </div>
        {error ? <ErrorMessage message={error} /> : null}
      </div>
    </Row>
  );
}

function PausePanel({
  projectId,
  paused,
}: {
  projectId: string;
  paused: boolean;
}) {
  const pause = useSetProjectIntakePause(projectId);
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);

  return (
    <Panel title="Pause">
      <Row
        title={paused ? "Feature intake is paused" : "Pause feature intake"}
        help="Pausing blocks new dispatches. Runs already in flight are not cancelled."
      >
        <div className="space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            <input
              type="text"
              aria-label="Reason for changing the pause state"
              placeholder="Reason (required)"
              value={reason}
              onChange={(event) => setReason(event.target.value)}
              className={`${INPUT_CLASS} max-w-md`}
            />
            <button
              type="button"
              disabled={pause.isMutating || reason.trim() === ""}
              onClick={async () => {
                setError(null);
                try {
                  await pause.trigger({ paused: !paused, reason: reason.trim() });
                  setReason("");
                } catch (cause) {
                  setError(
                    intakeErrorMessage(
                      cause,
                      "The pause state could not be changed. Your reason is kept.",
                    ),
                  );
                }
              }}
              className={SECONDARY_BUTTON_CLASS}
            >
              {paused ? "Resume feature intake" : "Pause feature intake"}
            </button>
          </div>
          {error ? <ErrorMessage message={error} /> : null}
        </div>
      </Row>
    </Panel>
  );
}

function InitiativesPanel({ projectId }: { projectId: string }) {
  const { mutate } = useSWRConfig();
  const query = useProjectIntakeInitiatives(projectId, true);
  const rows = query.data ?? [];

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-base font-semibold text-fg">Feature requests</h3>
        <Link
          to={`/projects/${encodeURIComponent(projectId)}/features/new`}
          className={PRIMARY_BUTTON_CLASS}
        >
          <PlusIcon className="size-4" aria-hidden="true" />
          New feature request
        </Link>
      </div>

      {query.isLoading && !query.data ? (
        <LoadingState label="Loading feature requests…" />
      ) : query.error ? (
        <IntakeInlineError
          message={intakeErrorMessage(
            query.error,
            "Feature requests could not be read from Plane. The rest of the project page still works.",
          )}
          onRetry={() => mutate(queryKeys.intake.initiatives(projectId))}
        />
      ) : rows.length === 0 ? (
        <EmptyState
          title="No feature requests yet"
          description="Draft one with the problem, users, goal, out-of-scope, and constraints. Fabro writes the PRD and leaves it for your review."
        />
      ) : (
        <Panel title="Feature requests">
          <ul className="divide-y divide-line">
            {rows.map((row) => (
              <InitiativeRow key={row.id} projectId={projectId} row={row} />
            ))}
          </ul>
        </Panel>
      )}
    </div>
  );
}

function InitiativeRow({
  projectId,
  row,
}: {
  projectId: string;
  row: IntakeInitiativeSummary;
}) {
  return (
    <li className="px-4 py-3.5">
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
        <Link
          to={`/projects/${encodeURIComponent(projectId)}/features/${encodeURIComponent(row.id)}`}
          className="text-sm font-medium text-fg-2 hover:text-fg"
        >
          {row.seq != null ? `${row.seq} · ` : ""}
          {row.name}
        </Link>
        {row.updated_at ? (
          <span className="text-xs text-fg-muted">
            updated {formatRelativeTime(row.updated_at)}
          </span>
        ) : null}
      </div>
      <p className="mt-1 text-xs/5 text-fg-3">
        Plane state <span className="font-mono text-fg-2">{row.state}</span>
        {" · "}
        {stageLabel(row.stage)}
      </p>
      {row.run ? (
        <p className="mt-1">
          <ActivityLine run={row.run} />
        </p>
      ) : null}
    </li>
  );
}

function SetupIntake({ project }: { project: Project }) {
  const { mutate } = useSWRConfig();
  const projectsQuery = usePlaneProjects(true);
  const [planeProjectId, setPlaneProjectId] = useState("");
  const metadataQuery = usePlaneProjectMetadata(planeProjectId || undefined);
  const setup = useSetupProjectIntake(project.id);
  const [error, setError] = useState<string | null>(null);
  const inFlight = setup.isMutating;

  const planeProjects = projectsQuery.data?.data ?? [];
  const plan = planeSetupPlan(
    metadataQuery.data?.states ?? [],
    metadataQuery.data?.labels ?? [],
  );
  const metadataReady = metadataQuery.data !== undefined && !metadataQuery.isLoading;

  async function submit() {
    if (planeProjectId === "" || inFlight) return;
    setError(null);
    try {
      await setup.trigger({ plane_project_id: planeProjectId, revision: project.revision });
    } catch (cause) {
      // A stale revision is the project's problem, not the operator's input:
      // the selection survives while the project detail re-reads its revision.
      const stale = cause instanceof ApiError && (cause.status === 409 || cause.status === 428);
      if (stale) void mutate(queryKeys.projects.detail(project.id));
      setError(
        stale
          ? `${intakeErrorMessage(cause, "The project changed while you were choosing.")} The project was re-read; review the selection and try again.`
          : intakeErrorMessage(cause, "Feature intake could not be set up."),
      );
    }
  }

  return (
    <div className="space-y-6">
      <Panel title="Set up feature intake">
        <Row
          title="Plane project"
          help="Feature requests live in Plane. Pick the project that already exists for this repository — Fabro never creates a Plane project."
        >
          <div className="space-y-2">
            {projectsQuery.isLoading && !projectsQuery.data ? (
              <p className="text-sm text-fg-3">Loading Plane projects…</p>
            ) : projectsQuery.error ? (
              <ErrorMessage
                message={intakeErrorMessage(
                  projectsQuery.error,
                  "The Plane integration could not list projects. Feature intake needs Plane.",
                )}
              />
            ) : (
              <select
                aria-label="Plane project"
                value={planeProjectId}
                onChange={(event) => setPlaneProjectId(event.target.value)}
                className={INPUT_CLASS}
              >
                <option value="">Select a Plane project</option>
                {planeProjects.map((option) => (
                  <option key={option.id} value={option.id}>
                    {option.identifier ? `${option.identifier} — ${option.name}` : option.name}
                  </option>
                ))}
              </select>
            )}
          </div>
        </Row>

        <Row
          title="What setup writes"
          help="These are the exact Plane rows Fabro will add. Nothing is deployed, no ticket is created, and no trigger is enabled."
        >
          {planeProjectId === "" ? (
            <p className="text-sm text-fg-muted">
              Select a Plane project to preview the states and labels that will be
              created.
            </p>
          ) : metadataQuery.error ? (
            <ErrorMessage
              message={intakeErrorMessage(
                metadataQuery.error,
                "The selected Plane project's states and labels could not be read.",
              )}
            />
          ) : !metadataReady ? (
            <p className="text-sm text-fg-3">Reading Plane states and labels…</p>
          ) : (
            <div className="space-y-2 text-xs/5 text-fg-3">
              <p>
                States to create:{" "}
                {plan.createStates.length === 0 ? (
                  <span className="text-fg-muted">none — all intake states exist</span>
                ) : (
                  <span className="text-fg-2">
                    {plan.createStates
                      .map((state) => `${state.name} (${state.group})`)
                      .join(", ")}
                  </span>
                )}
              </p>
              <p>
                Labels to create:{" "}
                {plan.createLabels.length === 0 ? (
                  <span className="text-fg-muted">none — both exist</span>
                ) : (
                  <span className="font-mono text-fg-2">
                    {plan.createLabels.join(", ")}
                  </span>
                )}
              </p>
              {plan.existingStates.length > 0 || plan.existingLabels.length > 0 ? (
                <p className="text-fg-muted">
                  Already present and reused:{" "}
                  {[...plan.existingStates, ...plan.existingLabels].join(", ")}
                </p>
              ) : null}
              {plan.incompatible.length > 0 ? (
                <ErrorMessage
                  message={`This Plane project cannot host intake without a change you control: ${plan.incompatible.join("; ")}.`}
                />
              ) : null}
              <p>
                Fabro also clones{" "}
                <span className="font-mono text-fg-2">{project.repository}</span>{" "}
                into a server-managed checkout, records the authoring binding, and
                publishes documents as a branch and draft pull request. It does not
                modify application code or deploy anything.
              </p>
            </div>
          )}
        </Row>
      </Panel>

      <div className="flex flex-wrap items-center gap-3">
        <button
          type="button"
          disabled={
            planeProjectId === "" ||
            inFlight ||
            !metadataReady ||
            plan.incompatible.length > 0
          }
          onClick={() => void submit()}
          className={PRIMARY_BUTTON_CLASS}
        >
          {inFlight ? "Setting up…" : "Set up feature intake"}
        </button>
        <span className="text-xs text-fg-muted">
          Confirming creates the listed Plane states and labels and registers the
          binding. Nothing else runs.
        </span>
      </div>
      {error ? <ErrorMessage message={error} /> : null}
    </div>
  );
}

function DetachPanel({ projectId }: { projectId: string }) {
  const detach = useDetachProjectIntake(projectId);
  const toast = useToast();
  const [open, setOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function confirm() {
    setError(null);
    try {
      await detach.trigger();
      toast.push({ message: "Feature intake detached. Plane resources were kept." });
      setOpen(false);
    } catch (cause) {
      setError(intakeErrorMessage(cause, "Feature intake could not be detached."));
    }
  }

  return (
    <Panel title="Detach">
      <Row
        title="Detach feature intake"
        help="Removes this project's intake binding. Plane issues, Git branches, run history, and the registered resources are kept."
      >
        <div className="space-y-2">
          <button
            type="button"
            onClick={() => setOpen(true)}
            className={SECONDARY_BUTTON_CLASS}
          >
            Detach feature intake
          </button>
          {error ? <ErrorMessage message={error} /> : null}
        </div>
      </Row>
      <ConfirmDialog
        open={open}
        title="Detach feature intake?"
        description={
          <>
            Fabro pauses intake for this project and removes the binding. Plane
            issues, Git branches, run history, and the registry resources are
            kept. Setting up the same binding again reconnects it; resuming
            feature intake is a separate, explicit action.
          </>
        }
        confirmLabel="Detach"
        pendingLabel="Detaching…"
        pending={detach.isMutating}
        onConfirm={() => void confirm()}
        onCancel={() => setOpen(false)}
      />
    </Panel>
  );
}

/** Inline, actionable failure that leaves the rest of the project page usable. */
function IntakeInlineError({
  message,
  onRetry,
}: {
  message: string;
  onRetry: () => void;
}) {
  return (
    <div
      role="alert"
      className="flex flex-wrap items-center justify-between gap-3 rounded-md bg-coral/10 px-3 py-2 text-sm text-fg-2 outline-1 -outline-offset-1 outline-coral/40"
    >
      <span className="min-w-0 max-w-prose">{message}</span>
      <button
        type="button"
        onClick={onRetry}
        className="shrink-0 text-mint underline hover:text-fg"
      >
        Try again
      </button>
    </div>
  );
}

function UnavailablePanel({
  error,
  onRetry,
}: {
  error: unknown;
  onRetry: () => void;
}) {
  const status = error instanceof ApiError ? error.status : null;
  const details =
    status === 503
      ? "This server has no reachable feature-intake bridge, so no intake call can succeed. Feature requests stay unavailable until the intake integration is configured; the rest of the project still works."
      : status === 404
        ? "This project is not connected to this Fabro server."
        : "Feature intake could not be read for this project.";

  return (
    <div className="space-y-3">
      <ErrorState
        title={status === 503 ? "Feature intake is unavailable" : "Couldn't load feature intake"}
        description={`${intakeErrorMessage(error, details)} ${status === 503 ? details : ""}`}
        onRetry={onRetry}
      />
      <p className="text-center text-xs text-fg-muted">
        <Link to="/projects" className="text-mint hover:text-fg hover:underline">
          Back to projects
        </Link>
      </p>
    </div>
  );
}
