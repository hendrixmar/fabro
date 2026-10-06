import { Link, useParams } from "react-router";
import type { Automation } from "@qltysh/fabro-api-client";
import { useAutomationRuns, useAutomations, useRunsPage } from "../lib/queries";
import { projectRunStats } from "../lib/project";

export function meta() {
  return [{ title: "Project overview — Fabro" }];
}

export const handle = { hideHeader: true };

function scheduleOf(automation: Automation): string {
  const schedules = automation.triggers.filter(
    (trigger) => trigger.type === "schedule" && trigger.enabled,
  ) as Array<{ expression: string }>;
  return schedules.map((trigger) => trigger.expression).join(", ") || "manual only";
}

function enabled(automation: Automation): boolean {
  return automation.triggers.some((trigger) => trigger.enabled);
}

function Tile({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-lg border border-line bg-panel px-4 py-3">
      <div className="text-xs text-fg-muted">{label}</div>
      <div className="mt-1 text-lg font-semibold text-fg">{value}</div>
    </div>
  );
}

function AutomationRow({ automation }: { automation: Automation }) {
  const lastRun = useAutomationRuns(automation.id, { limit: 1, offset: 0 }).data?.data[0];
  return (
    <li className="flex items-center justify-between gap-4 px-4 py-3">
      <div className="min-w-0">
        <Link to={`/automations/${encodeURIComponent(automation.id)}`} className="font-medium text-fg hover:underline">
          {automation.name}
        </Link>
        <div className="text-xs text-fg-muted">
          {automation.source_automation_id ? `Linked to ${automation.source_automation_id} · ` : null}
          {scheduleOf(automation)}
        </div>
      </div>
      <div className="shrink-0 text-right text-xs text-fg-3">
        <div>{enabled(automation) ? "Enabled" : "Disabled"}</div>
        <div>
          {lastRun ? (
            <Link to={`/runs/${lastRun.id}`} className="hover:underline">
              Last run {new Date(lastRun.timestamps.created_at).toLocaleString()}
            </Link>
          ) : "Never run"}
        </div>
      </div>
    </li>
  );
}

export default function ProjectOverview() {
  const { id = "" } = useParams();
  const runs = useRunsPage({ projectId: id, rootsOnly: true, limit: 100, offset: 0 }).data?.data ?? [];
  const automations = useAutomations({ scope: "project", projectId: id }).data?.data ?? [];
  // ponytail: stats cover the latest 100 root runs; add a server count endpoint if a project outgrows it.
  const stats = projectRunStats(runs, Date.now());
  const failures = runs
    .filter((run) => run.lifecycle.status.kind === "failed" || run.lifecycle.status.kind === "dead")
    .slice(0, 5);
  return (
    <div className="flex flex-col gap-6">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Tile label="Running" value={stats.running} />
        <Tile label="Failed (24h)" value={stats.failed24h} />
        <Tile label="Last run" value={stats.lastRunAt ? new Date(stats.lastRunAt).toLocaleString() : "Never"} />
        <Tile label="Automations enabled" value={`${automations.filter(enabled).length}/${automations.length}`} />
      </div>
      <section>
        <h2 className="mb-2 text-sm font-semibold text-fg">Automations</h2>
        {automations.length === 0 ? (
          <p className="text-sm text-fg-muted">
            No automations yet. <Link to="automations" className="underline">Add one</Link>.
          </p>
        ) : (
          <ul className="divide-y divide-line rounded-lg border border-line">
            {automations.map((automation) => <AutomationRow key={automation.id} automation={automation} />)}
          </ul>
        )}
      </section>
      <section>
        <h2 className="mb-2 text-sm font-semibold text-fg">Recent failures</h2>
        {failures.length === 0 ? (
          <p className="text-sm text-fg-muted">No failed runs.</p>
        ) : (
          <ul className="divide-y divide-line rounded-lg border border-line">
            {failures.map((run) => (
              <li key={run.id} className="px-4 py-2 text-sm">
                <Link to={`/runs/${run.id}`} className="text-fg hover:underline">{run.title}</Link>
                <span className="ml-2 text-xs text-fg-muted">{new Date(run.timestamps.created_at).toLocaleString()}</span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
