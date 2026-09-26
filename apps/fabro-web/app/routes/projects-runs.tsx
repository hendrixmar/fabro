import { useState } from "react";
import { useParams } from "react-router";
import type { BoardColumn, ListRunsDirectionEnum, ListRunsSortEnum } from "@qltysh/fabro-api-client";
import { RunsListView } from "../components/runs-list/runs-list-view";
import { DEFAULT_HIDDEN_RUN_LIST_COLUMNS } from "../components/runs-list/preferences";
import { useAutomations, useRunsPage } from "../lib/queries";

export function meta() {
  return [{ title: "Project runs — Fabro" }];
}

export const handle = { hideHeader: true };

const STATUSES: BoardColumn[] = ["running", "blocked", "succeeded", "failed"];

export default function ProjectRuns() {
  const { id = "" } = useParams();
  const [automationId, setAutomationId] = useState("");
  const [workflow, setWorkflow] = useState("");
  const [status, setStatus] = useState<BoardColumn | "">("");
  const [activity, setActivity] = useState(true);
  const [sort, setSort] = useState<ListRunsSortEnum>("created_at");
  const [direction, setDirection] = useState<ListRunsDirectionEnum>("desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const automations = useAutomations({ scope: "project", projectId: id }).data?.data ?? [];
  const runs = useRunsPage({
    projectId:    id,
    rootsOnly:    true,
    activity,
    automationId: automationId || undefined,
    workflow:     workflow || undefined,
    status:       status ? [status] : undefined,
    sort,
    direction,
    limit:        pageSize,
    offset:       (page - 1) * pageSize,
  });
  // The workflow selector on an automation isn't always the same string as
  // `run.workflow.slug` the server filters against (for example a path
  // selector), so build the dropdown from the runs actually listed instead
  // of the automations — whatever's offered is guaranteed to match.
  // ponytail: options only cover the current page; add a facets endpoint if
  // that's ever confusing with many workflows.
  const workflows = [...new Set(
    (runs.data?.data ?? [])
      .map((run) => run.workflow.slug)
      .filter((slug): slug is string => Boolean(slug)),
  )].sort();
  const select = "rounded border border-line bg-panel px-2 py-1 text-sm";
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-3">
        <select className={select} value={automationId} onChange={(e) => { setAutomationId(e.target.value); setPage(1); }} aria-label="Automation">
          <option value="">All automations</option>
          {automations.map((automation) => <option key={automation.id} value={automation.id}>{automation.name}</option>)}
        </select>
        <select className={select} value={workflow} onChange={(e) => { setWorkflow(e.target.value); setPage(1); }} aria-label="Workflow">
          <option value="">All workflows</option>
          {workflows.map((name) => <option key={name} value={name}>{name}</option>)}
        </select>
        <select className={select} value={status} onChange={(e) => { setStatus(e.target.value as BoardColumn | ""); setPage(1); }} aria-label="Status">
          <option value="">Any status</option>
          {STATUSES.map((value) => <option key={value} value={value}>{value}</option>)}
        </select>
        <label className="flex items-center gap-2 text-sm text-fg-3">
          <input type="checkbox" checked={activity} onChange={(e) => { setActivity(e.target.checked); setPage(1); }} />
          Only runs that did something
        </label>
      </div>
      <RunsListView
        data={runs.data}
        isLoading={runs.data === undefined && runs.isLoading}
        emptyState={<p className="p-6 text-sm text-fg-muted">No runs match these filters.</p>}
        sort={sort}
        direction={direction}
        page={page}
        pageSize={pageSize}
        hiddenColumns={new Set([...DEFAULT_HIDDEN_RUN_LIST_COLUMNS, "project"])}
        onSortClick={(key) => {
          if (key === sort) setDirection(direction === "asc" ? "desc" : "asc");
          else { setSort(key); setDirection("desc"); }
        }}
        onPageChange={setPage}
        onPageSizeChange={(size) => { setPageSize(size); setPage(1); }}
        query=""
        workflowFilter="all"
        createdCutoffMs={null}
      />
    </div>
  );
}
