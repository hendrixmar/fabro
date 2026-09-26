import { Link } from "react-router";
import type {
  IntakeReadinessCheck,
  IntakeReadinessReport,
  IntakeRunRecord,
} from "@qltysh/fabro-api-client";

import { Badge, Muted } from "./settings-panel";
import { CopyButton } from "./ui";
import { formatAbsoluteTs, formatRelativeTime } from "../lib/format";
import { initiativeActivity, stageLabel } from "../lib/intake";

const OUTCOME_TONE: Record<IntakeReadinessCheck["outcome"], string> = {
  pass: "text-mint",
  fail: "text-coral",
  not_applicable: "text-fg-muted",
  not_configured: "text-amber",
  unknown: "text-amber",
};

const OUTCOME_LABEL: Record<IntakeReadinessCheck["outcome"], string> = {
  pass: "Pass",
  fail: "Fail",
  not_applicable: "Not applicable",
  not_configured: "Not configured",
  unknown: "Unknown",
};

/** A state the operator must read, not just a colour: the label carries it too. */
export function StateText({ label, tone }: { label: string; tone: string }) {
  return <span className={`text-sm font-medium ${tone}`}>{label}</span>;
}

export function ChecksNote() {
  return (
    <p className="text-xs/5 text-fg-muted">
      Each check reports its own outcome and next action. Unknown, missing, or
      stale checks never count as ready.
    </p>
  );
}

/**
 * Every readiness check with its own outcome, message, and next action. A
 * failed provider stays visible instead of being hidden behind one badge.
 */
export function ReadinessChecks({
  report,
  emptyLabel,
}: {
  report: IntakeReadinessReport | null | undefined;
  emptyLabel: string;
}) {
  const checks = report?.checks ?? [];
  if (checks.length === 0) {
    return <p className="text-sm text-fg-muted">{emptyLabel}</p>;
  }
  return (
    <>
      <ul className="space-y-2">
        {checks.map((check) => (
          <li key={check.key} className="rounded-md border border-line px-3 py-2">
            <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
              <span className="font-mono text-xs text-fg-2">{check.key}</span>
              <StateText
                label={OUTCOME_LABEL[check.outcome]}
                tone={OUTCOME_TONE[check.outcome]}
              />
            </div>
            {check.message ? (
              <p className="mt-1 text-xs/5 text-fg-3">{check.message}</p>
            ) : null}
            {check.next_action ? (
              <p className="mt-1 text-xs/5 text-fg-muted">
                Next: {check.next_action}
              </p>
            ) : null}
          </li>
        ))}
      </ul>
      {report?.checked_at ? (
        <p className="mt-2 text-xs text-fg-muted">
          Readiness checked{" "}
          {formatRelativeTime(
            new Date(report.checked_at * 1000).toISOString(),
          )}
          .
        </p>
      ) : (
        <p className="mt-2 text-xs text-fg-muted">
          Never checked on this server — re-probe before treating anything as
          ready.
        </p>
      )}
    </>
  );
}

/** Persisted dispatch records. Nothing here is optimistic. */
export function RunHistory({ records }: { records: IntakeRunRecord[] }) {
  if (records.length === 0) {
    return <p className="text-sm text-fg-muted">No run history recorded yet.</p>;
  }
  return (
    <ol className="space-y-2">
      {[...records].reverse().map((record, index) => (
        <li
          key={`${record.id ?? index}-${record.event}-${record.ts ?? ""}`}
          className="rounded-md border border-line px-3 py-2"
        >
          <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
            <span className="text-xs font-medium text-fg-2">
              {stageLabel(record.stage)}
            </span>
            <Badge>{record.event}</Badge>
            <span className="text-xs text-fg-3">{record.outcome}</span>
            {record.ts ? (
              <span
                className="text-xs text-fg-muted"
                title={formatAbsoluteTs(new Date(record.ts * 1000).toISOString())}
              >
                {formatRelativeTime(new Date(record.ts * 1000).toISOString())}
              </span>
            ) : null}
          </div>
          {record.message ? (
            <p className="mt-1 text-xs/5 text-fg-3">{record.message}</p>
          ) : null}
          <p className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
            {record.actor ? (
              <span className="text-fg-muted">by {record.actor}</span>
            ) : null}
            {record.run ? (
              <Link
                to={`/runs/${encodeURIComponent(record.run)}`}
                className="text-mint hover:text-fg hover:underline"
              >
                Open Fabro run
              </Link>
            ) : null}
          </p>
        </li>
      ))}
    </ol>
  );
}

export function ActivityLine({
  run,
}: {
  run: IntakeRunRecord | null | undefined;
}) {
  if (!run) return <Muted>No dispatch recorded for this stage.</Muted>;
  return (
    <span className="text-xs/5 text-fg-3">
      {initiativeActivity(run)}
      {run.run ? (
        <>
          {" · "}
          <Link
            to={`/runs/${encodeURIComponent(run.run)}`}
            className="text-mint hover:text-fg hover:underline"
          >
            Open run
          </Link>
        </>
      ) : null}
    </span>
  );
}

/** The Plane issue id is an opaque UUID; copying it is the reliable hand-off. */
export function PlaneIssueRef({ issue }: { issue: string }) {
  return (
    <span className="inline-flex min-w-0 items-center gap-1">
      <span className="truncate font-mono text-xs text-fg-2" title={issue}>
        {issue}
      </span>
      <CopyButton value={issue} label="Copy Plane issue id" />
    </span>
  );
}

export function SupervisedField({
  id,
  checked,
  onChange,
  hint,
}: {
  id: string;
  checked: boolean;
  onChange: (next: boolean) => void;
  hint: string;
}) {
  return (
    <label htmlFor={id} className="flex items-start gap-2 text-sm text-fg-2">
      <input
        id={id}
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
        className="mt-0.5 size-4 shrink-0 rounded border-line-strong bg-panel-alt text-teal-500 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-teal-500"
      />
      <span className="min-w-0">
        <span className="font-medium">
          Supervised execution{" "}
          <span aria-label="required" className="text-coral">
            *
          </span>
        </span>
        <span className="mt-0.5 block text-xs/5 text-fg-3">{hint}</span>
      </span>
    </label>
  );
}
