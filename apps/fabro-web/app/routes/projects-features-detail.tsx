import { useState } from "react";
import { Link, useParams } from "react-router";
import { useSWRConfig } from "swr";
import type {
  IntakeInitiativeDetail,
  IntakeReadinessCheck,
  Project,
} from "@qltysh/fabro-api-client";

import { IntakeAdvisor } from "../components/intake-advisor";
import {
  ActivityLine,
  ChecksNote,
  PlaneIssueRef,
  RunHistory,
  SupervisedField,
} from "../components/intake-panels";
import { Markdown } from "../components/stage-renderers/primitives";
import { Panel, Row } from "../components/settings-panel";
import { ErrorState, LoadingState } from "../components/state";
import {
  ErrorMessage,
  INPUT_CLASS,
  PRIMARY_BUTTON_CLASS,
  SECONDARY_BUTTON_CLASS,
} from "../components/ui";
import { formatAbsoluteTs, formatRelativeTime } from "../lib/format";
import {
  INTAKE_DOC_STAGES,
  INTAKE_EXECUTION_LABEL,
  INTAKE_RUNNABLE_STAGES,
  approvedDigests,
  approveActionLabel,
  blockingChecks,
  documentMetaRows,
  hasUnknownOutcome,
  intakeErrorMessage,
  intakeExecutionState,
  stageLabel,
} from "../lib/intake";
import {
  useApproveProjectIntakeInitiative,
  useCancelProjectIntakeInitiative,
  useCommentProjectIntakeInitiative,
  useExecuteProjectIntakeInitiative,
  useReconcileProjectIntakeInitiative,
  useReviseProjectIntakeInitiative,
  useRunProjectIntakeStage,
} from "../lib/mutations";
import {
  useProject,
  useProjectIntake,
  useProjectIntakeHistory,
  useProjectIntakeInitiative,
} from "../lib/queries";
import { queryKeys } from "../lib/query-keys";

export function meta() {
  return [{ title: "Feature request — Fabro" }];
}

export default function ProjectFeatureDetail() {
  const { id, issue } = useParams<{ id: string; issue: string }>();
  const { mutate } = useSWRConfig();
  const projectQuery = useProject(id);
  const detailQuery = useProjectIntakeInitiative(id, issue);

  if (projectQuery.isLoading && !projectQuery.data) {
    return <LoadingState label="Loading project…" />;
  }

  if (!projectQuery.data) {
    return (
      <ErrorState
        title="Couldn't load this project"
        description="The project could not be read, so this feature request is unavailable."
      />
    );
  }

  if (detailQuery.isLoading && !detailQuery.data) {
    return <LoadingState label="Loading feature request…" />;
  }

  if (!detailQuery.data) {
    return (
      <ErrorState
        title="Couldn't load this feature request"
        description={intakeErrorMessage(
          detailQuery.error,
          "Feature intake did not return this request.",
        )}
        onRetry={() =>
          mutate(queryKeys.intake.initiative(projectQuery.data!.id, issue ?? ""))
        }
      />
    );
  }

  return (
    <InitiativeDetail
      project={projectQuery.data}
      detail={detailQuery.data}
      issue={issue ?? detailQuery.data.id}
    />
  );
}

function InitiativeDetail({
  project,
  detail,
  issue,
}: {
  project: Project;
  detail: IntakeInitiativeDetail;
  issue: string;
}) {
  const historyQuery = useProjectIntakeHistory(project.id, issue);
  const rows = historyQuery.data ?? detail.history;
  const unknownOutcome = hasUnknownOutcome(detail.run);

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="text-base font-semibold text-fg">
            {detail.seq != null ? `${detail.seq} · ` : ""}
            {detail.name}
          </h3>
          <p className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs/5 text-fg-3">
            <span>
              Stage <span className="font-medium text-fg-2">{stageLabel(detail.stage)}</span>
            </span>
            <span className="text-fg-muted">·</span>
            <span>
              Plane state <span className="font-mono text-fg-2">{detail.state}</span>
            </span>
            {detail.updated_at ? (
              <>
                <span className="text-fg-muted">·</span>
                <span title={formatAbsoluteTs(detail.updated_at)}>
                  updated {formatRelativeTime(detail.updated_at)}
                </span>
              </>
            ) : null}
          </p>
          <p className="mt-1">
            <ActivityLine run={detail.run} />
          </p>
        </div>
        <Link
          to={`/projects/${encodeURIComponent(project.id)}/features`}
          className={SECONDARY_BUTTON_CLASS}
        >
          All feature requests
        </Link>
      </div>

      {detail.needs_operator || unknownOutcome ? (
        <RecoveryBanner
          projectId={project.id}
          issue={issue}
          needsOperator={Boolean(detail.needs_operator)}
          unknownOutcome={unknownOutcome}
        />
      ) : null}

      <Panel title="References">
        <Row
          title="Plane issue"
          help="This server's API exposes no Plane workspace URL, so the issue id is the reliable hand-off."
        >
          <PlaneIssueRef issue={issue} />
        </Row>
        <Row title="Repository" help="Canonical repository and branch this request targets.">
          <a
            href={`https://github.com/${project.repository}`}
            target="_blank"
            rel="noreferrer"
            className="font-mono text-xs text-mint hover:text-fg hover:underline"
          >
            {project.repository}
          </a>
        </Row>
        <Row title="Draft pull request" help="Document authoring opens a branch and draft PR; it never changes application code.">
          {detail.pr_url ? (
            <a
              href={detail.pr_url}
              target="_blank"
              rel="noreferrer"
              className="truncate font-mono text-xs text-mint hover:text-fg hover:underline"
              title={detail.pr_url}
            >
              {detail.pr_url}
            </a>
          ) : (
            <span className="text-fg-muted">No draft pull request yet</span>
          )}
        </Row>
        <Row
          title="Approved digests"
          help="Recorded per generated document. Approval never runs provisioning."
        >
          <DigestList detail={detail} />
        </Row>
        {detail.description ? (
          <Row title="Original request" help="The request as filed in Plane.">
            <pre className="max-h-64 overflow-auto whitespace-pre-wrap font-sans text-xs/5 text-fg-3">
              {detail.description}
            </pre>
          </Row>
        ) : null}
      </Panel>

      <Documents detail={detail} />

      <div className="grid gap-6 lg:grid-cols-2">
        <Panel title="Open questions">
          {detail.questions.length === 0 ? (
            <Row title="Questions" help="Raised by the current document.">
              <span className="text-fg-muted">None recorded.</span>
            </Row>
          ) : (
            detail.questions.map((question, index) => (
              <Row key={`${index}-${question}`} title={`Question ${index + 1}`}>
                <span className="text-fg-3">{question}</span>
              </Row>
            ))
          )}
        </Panel>

        <Panel title="Assumptions">
          {detail.supuestos.length === 0 ? (
            <Row title="Assumptions" help="What the current document assumes.">
              <span className="text-fg-muted">None recorded.</span>
            </Row>
          ) : (
            detail.supuestos.map((assumption, index) => (
              <Row key={`${index}-${assumption}`} title={`Assumption ${index + 1}`}>
                <span className="text-fg-3">{assumption}</span>
              </Row>
            ))
          )}
        </Panel>
      </div>

      <Panel title="Comments">
        {detail.comments.length === 0 ? (
          <Row title="Comments" help="Human and automated comments on this request.">
            <span className="text-fg-muted">No comments yet.</span>
          </Row>
        ) : (
          detail.comments.map((comment, index) => (
            <Row
              key={comment.id ?? `${index}-${comment.who}`}
              title={
                <span className="flex flex-wrap items-center gap-2">
                  <span>{comment.who}</span>
                  {comment.factory ? (
                    <span className="text-xs text-fg-muted">automated</span>
                  ) : null}
                </span>
              }
              help={comment.at ? formatAbsoluteTs(comment.at) : undefined}
            >
              <span className="whitespace-pre-wrap text-fg-3">{comment.text}</span>
            </Row>
          ))
        )}
      </Panel>

      <ActionsPanel projectId={project.id} detail={detail} issue={issue} />

      <Panel title="Run history">
        <div className="px-4 py-3.5">
          {historyQuery.error ? (
            <ErrorMessage
              message={intakeErrorMessage(
                historyQuery.error,
                "Run history could not be read; showing the history read with the request.",
              )}
            />
          ) : null}
          <div className="mt-2">
            <RunHistory records={rows} />
          </div>
        </div>
      </Panel>

      <div className="max-w-2xl">
        <AdvisorForIssue projectId={project.id} issue={issue} />
      </div>
    </div>
  );
}

function AdvisorForIssue({ projectId, issue }: { projectId: string; issue: string }) {
  const comment = useCommentProjectIntakeInitiative(projectId, issue);
  const [error, setError] = useState<string | null>(null);

  return (
    <div className="space-y-2">
      <IntakeAdvisor
        projectId={projectId}
        sessionKey={issue}
        onPostComment={async (text) => {
          setError(null);
          try {
            await comment.trigger({ text });
          } catch (cause) {
            setError(
              intakeErrorMessage(
                cause,
                "The advisor's reply could not be posted as a comment.",
              ),
            );
          }
        }}
        postCommentPending={comment.isMutating}
        placeholder="Ask about this feature request…"
      />
      {error ? <ErrorMessage message={error} /> : null}
      <p className="text-xs text-fg-muted">
        The advisor is read-only. Posting its reply is an explicit comment and
        never approves a document or starts a run.
      </p>
    </div>
  );
}

function DigestList({ detail }: { detail: IntakeInitiativeDetail }) {
  const digests = approvedDigests(detail);
  if (digests.length === 0) {
    return <span className="text-fg-muted">No approved document digest recorded.</span>;
  }
  return (
    <ul className="space-y-1">
      {digests.map((entry, index) => (
        <li key={`${entry.stage}-${index}`} className="flex flex-wrap items-baseline gap-2">
          <span className="text-xs font-medium text-fg-2">{stageLabel(entry.stage)}</span>
          <span className="break-all font-mono text-xs text-fg-3">{entry.digest}</span>
        </li>
      ))}
    </ul>
  );
}

function Documents({ detail }: { detail: IntakeInitiativeDetail }) {
  const stages = INTAKE_DOC_STAGES.filter((stage) => detail.docs?.[stage]?.body);
  if (stages.length === 0) {
    return (
      <Panel title="Documents">
        <Row title="Generated documents" help="Drafted by the authoring stages.">
          <span className="text-fg-muted">
            No document has been drafted for this request yet.
          </span>
        </Row>
      </Panel>
    );
  }
  return (
    <>
      {stages.map((stage) => {
        const doc = detail.docs![stage];
        const meta = documentMetaRows(doc.meta);
        return (
          <Panel key={stage} title={`${stageLabel(stage)} document`}>
            {meta.length > 0 ? (
              <Row title="Metadata" help="Front matter recorded with this document.">
                <dl className="space-y-0.5">
                  {meta.map((entry) => (
                    <div key={entry.key} className="flex flex-wrap gap-2 text-xs">
                      <dt className="font-mono text-fg-muted">{entry.key}</dt>
                      <dd className="text-fg-3">{entry.value}</dd>
                    </div>
                  ))}
                </dl>
              </Row>
            ) : null}
            <div className="px-4 py-3.5">
              <Markdown content={doc.body} />
            </div>
          </Panel>
        );
      })}
    </>
  );
}

function RecoveryBanner({
  projectId,
  issue,
  needsOperator,
  unknownOutcome,
}: {
  projectId: string;
  issue: string;
  needsOperator: boolean;
  unknownOutcome: boolean;
}) {
  const reconcile = useReconcileProjectIntakeInitiative(projectId, issue);
  const [error, setError] = useState<string | null>(null);

  return (
    <div className="space-y-2 rounded-md bg-amber/10 px-3 py-2.5 outline-1 -outline-offset-1 outline-amber/40">
      <p className="text-sm text-fg-2">
        {needsOperator
          ? "This request is flagged for an operator."
          : "The last submission's outcome is unknown."}{" "}
        Reconcile re-reads Plane and the run state instead of repeating a
        submission that may already have taken effect.
      </p>
      <div className="flex flex-wrap items-center gap-2">
        <button
          type="button"
          disabled={reconcile.isMutating}
          onClick={async () => {
            setError(null);
            try {
              await reconcile.trigger();
            } catch (cause) {
              setError(intakeErrorMessage(cause, "Reconciliation could not run."));
            }
          }}
          className={SECONDARY_BUTTON_CLASS}
        >
          {reconcile.isMutating ? "Reconciling…" : "Reconcile"}
        </button>
        <span className="text-xs text-fg-muted">
          Not a retry: it never re-submits an unknown outcome.
        </span>
      </div>
      {error ? <ErrorMessage message={error} /> : null}
    </div>
  );
}

function ActionsPanel({
  projectId,
  detail,
  issue,
}: {
  projectId: string;
  detail: IntakeInitiativeDetail;
  issue: string;
}) {
  const statusQuery = useProjectIntake(projectId);
  const approve = useApproveProjectIntakeInitiative(projectId, issue);
  const revise = useReviseProjectIntakeInitiative(projectId, issue);
  const cancel = useCancelProjectIntakeInitiative(projectId, issue);
  const runStage = useRunProjectIntakeStage(projectId, issue);
  const execute = useExecuteProjectIntakeInitiative(projectId, issue);

  const [supervised, setSupervised] = useState(false);
  const [feedback, setFeedback] = useState("");
  const [stage, setStage] = useState<string>(
    INTAKE_RUNNABLE_STAGES.find((candidate) => candidate === detail.stage) ?? "prd",
  );
  const [confirming, setConfirming] = useState<"cancel" | "execute" | null>(null);
  const [error, setError] = useState<string | null>(null);

  const execution = intakeExecutionState(statusQuery.data);
  const executionBlockers: IntakeReadinessCheck[] = blockingChecks(
    statusQuery.data?.readiness,
  );
  const approveLabel = approveActionLabel(detail.stage);
  const unknownOutcome = hasUnknownOutcome(detail.run);

  async function run(action: () => Promise<unknown>, fallback: string) {
    setError(null);
    try {
      await action();
      // Nothing is assumed: the queries are invalidated by the mutation and the
      // detail re-reads the persisted Plane and run state.
    } catch (cause) {
      setError(intakeErrorMessage(cause, fallback));
    }
  }

  const busy =
    approve.isMutating ||
    revise.isMutating ||
    cancel.isMutating ||
    runStage.isMutating ||
    execute.isMutating;

  return (
    <Panel title="Actions">
      <div className="space-y-2 px-4 py-3.5">
        {error ? <ErrorMessage message={error} /> : null}
        <SupervisedField
          id="intake-supervised"
          checked={supervised}
          onChange={setSupervised}
          hint="Required for every dispatch. Each stage pauses for your review; approving a document never provisions anything."
        />
      </div>

      <Row
        title="Approval"
        help={
          detail.stage === "design"
            ? "Approving the design records its digest and stops there: no coding and no provisioning start."
            : "Approving drafts the next document. Nothing is deployed or executed."
        }
      >
        <div className="space-y-2">
          {approveLabel ? (
            <button
              type="button"
              disabled={busy || !supervised}
              onClick={() =>
                void run(
                  () => approve.trigger({ supervised }),
                  "The approval was refused.",
                )
              }
              className={PRIMARY_BUTTON_CLASS}
            >
              {approve.isMutating ? "Approving…" : approveLabel}
            </button>
          ) : (
            <p className="text-sm text-fg-muted">
              No document is awaiting approval in state{" "}
              <span className="font-mono text-fg-3">{detail.state}</span>.
            </p>
          )}
        </div>
      </Row>

      <Row title="Request changes" help="Send the current document back with feedback.">
        <div className="space-y-2">
          <label htmlFor="intake-feedback" className="sr-only">
            Feedback for the current document
          </label>
          <textarea
            id="intake-feedback"
            rows={3}
            value={feedback}
            onChange={(event) => setFeedback(event.target.value)}
            placeholder="What must change before this can be approved?"
            className={INPUT_CLASS}
          />
          <button
            type="button"
            disabled={busy || !supervised || feedback.trim() === ""}
            onClick={() =>
              void run(async () => {
                await revise.trigger({ text: feedback.trim(), supervised });
                setFeedback("");
              }, "The revision request was refused.")
            }
            className={SECONDARY_BUTTON_CLASS}
          >
            {revise.isMutating ? "Sending…" : "Request changes"}
          </button>
        </div>
      </Row>

      <Row
        title="Run or retry a stage"
        help="Dispatch a document stage against the current request."
      >
        {unknownOutcome ? (
          <p className="text-sm text-fg-muted">
            The last submission's outcome is unknown, so no stage is offered
            here. Reconcile first.
          </p>
        ) : (
          <div className="flex flex-wrap items-center gap-2">
            <select
              aria-label="Stage to run"
              value={stage}
              onChange={(event) => setStage(event.target.value)}
              className={`${INPUT_CLASS} max-w-xs`}
            >
              {INTAKE_RUNNABLE_STAGES.map((candidate) => (
                <option key={candidate} value={candidate}>
                  {stageLabel(candidate)}
                </option>
              ))}
            </select>
            <button
              type="button"
              disabled={busy || !supervised}
              onClick={() =>
                void run(
                  () =>
                    runStage.trigger({
                      stage: stage as (typeof INTAKE_RUNNABLE_STAGES)[number],
                      supervised,
                    }),
                  "The stage could not be dispatched.",
                )
              }
              className={SECONDARY_BUTTON_CLASS}
            >
              {runStage.isMutating ? "Dispatching…" : "Run stage"}
            </button>
          </div>
        )}
      </Row>

      <Row
        title="Prepare execution"
        help={INTAKE_EXECUTION_LABEL[execution]}
      >
        <div className="space-y-3">
          <p className="text-xs/5 text-fg-3">
            A separate, explicitly confirmed step. It may configure staging and
            CI and create implementation tickets; it never starts application
            coding by itself, and it is never implied by approving the design.
          </p>
          {execution === "ready" ? (
            <button
              type="button"
              onClick={() => setConfirming("execute")}
              disabled={busy}
              className={SECONDARY_BUTTON_CLASS}
            >
              Prepare execution…
            </button>
          ) : (
            <div className="space-y-2">
              <button type="button" disabled className={SECONDARY_BUTTON_CLASS}>
                Prepare execution
              </button>
              <p className="text-xs/5 text-amber">
                Disabled until execution readiness passes. Missing requirements:
              </p>
              {executionBlockers.length === 0 ? (
                <p className="text-xs/5 text-fg-3">
                  No check set is stored yet — re-probe readiness on the feature
                  requests tab.
                </p>
              ) : (
                <ul className="space-y-1">
                  {executionBlockers.map((check) => (
                    <li key={check.key} className="text-xs/5 text-fg-3">
                      <span className="font-mono text-fg-2">{check.key}</span>:{" "}
                      {check.message || check.next_action}
                    </li>
                  ))}
                </ul>
              )}
              <ChecksNote />
            </div>
          )}
          {confirming === "execute" ? (
            <ConfirmBlock
              title="Prepare execution?"
              consequences="Fabro may configure staging and CI and create implementation tickets for this feature request. Approving documents never does this."
              confirmLabel="Prepare execution"
              pendingLabel="Preparing…"
              pending={execute.isMutating}
              disabled={!supervised}
              disabledHint="Tick supervised execution above to confirm."
              onConfirm={() => {
                setConfirming(null);
                void run(
                  () => execute.trigger({ supervised }),
                  "Execution preparation was refused.",
                );
              }}
              onCancel={() => setConfirming(null)}
            />
          ) : null}
        </div>
      </Row>

      <Row title="Cancel request" help="Cancels the feature request in Plane.">
        <div className="space-y-3">
          {confirming === "cancel" ? (
            <ConfirmBlock
              title="Cancel this feature request?"
              consequences="The request is cancelled in Plane. Documents, branches, and history are kept."
              confirmLabel="Cancel request"
              pendingLabel="Cancelling…"
              pending={cancel.isMutating}
              disabled={!supervised}
              disabledHint="Tick supervised execution above to confirm."
              onConfirm={() => {
                setConfirming(null);
                void run(
                  () => cancel.trigger({ supervised }),
                  "The cancellation was refused.",
                );
              }}
              onCancel={() => setConfirming(null)}
            />
          ) : (
            <button
              type="button"
              onClick={() => setConfirming("cancel")}
              disabled={busy || detail.state === "cancelled"}
              className={SECONDARY_BUTTON_CLASS}
            >
              {detail.state === "cancelled" ? "Already cancelled" : "Cancel request…"}
            </button>
          )}
        </div>
      </Row>
    </Panel>
  );
}

function ConfirmBlock({
  title,
  consequences,
  confirmLabel,
  pendingLabel,
  pending,
  disabled,
  disabledHint,
  onConfirm,
  onCancel,
}: {
  title: string;
  consequences: string;
  confirmLabel: string;
  pendingLabel: string;
  pending: boolean;
  disabled: boolean;
  disabledHint: string;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div
      role="group"
      aria-label={title}
      className="space-y-2 rounded-md border border-line-strong bg-panel-alt px-3 py-2.5"
    >
      <p className="text-sm font-medium text-fg">{title}</p>
      <p className="text-xs/5 text-fg-3">{consequences}</p>
      {disabled ? <p className="text-xs/5 text-amber">{disabledHint}</p> : null}
      <div className="flex flex-wrap items-center gap-2">
        <button
          type="button"
          onClick={onConfirm}
          disabled={pending || disabled}
          className={SECONDARY_BUTTON_CLASS}
        >
          {pending ? pendingLabel : confirmLabel}
        </button>
        <button
          type="button"
          onClick={onCancel}
          disabled={pending}
          className={SECONDARY_BUTTON_CLASS}
        >
          Keep as is
        </button>
      </div>
    </div>
  );
}
