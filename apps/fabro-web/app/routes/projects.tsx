import { useState } from "react";
import { Link } from "react-router";
import { useSWRConfig } from "swr";
import { PlusIcon } from "@heroicons/react/20/solid";
import { ArrowDownTrayIcon, FolderIcon } from "@heroicons/react/24/outline";
import type { Automation, IntakeImportRow, Project } from "@qltysh/fabro-api-client";

import { Panel, Row } from "../components/settings-panel";
import { EmptyState, ErrorState, LoadingState } from "../components/state";
import {
  ErrorMessage,
  PRIMARY_BUTTON_CLASS,
  SECONDARY_BUTTON_CLASS,
} from "../components/ui";
import { projectAutomationAttention } from "../lib/project";
import { intakeErrorMessage } from "../lib/intake";
import { useImportProjectIntakeBindings } from "../lib/mutations";
import { useAutomations, useProjects } from "../lib/queries";
import { queryKeys } from "../lib/query-keys";
import { plural } from "../lib/plural";

export function meta() {
  return [{ title: "Projects — Fabro" }];
}

export const handle = { hideHeader: true };

const IMPORT_TONE: Record<IntakeImportRow["status"], string> = {
  imported: "text-mint",
  attached: "text-teal-500",
  conflict: "text-amber",
  error: "text-coral",
};

export default function Projects() {
  const { mutate } = useSWRConfig();
  const projectsQuery = useProjects();
  const automationsQuery = useAutomations();
  const importBindings = useImportProjectIntakeBindings();
  const [importRows, setImportRows] = useState<IntakeImportRow[] | null>(null);
  const [importError, setImportError] = useState<string | null>(null);
  const projects = projectsQuery.data?.data ?? [];
  const automations = automationsQuery.data?.data ?? [];

  if (projectsQuery.isLoading && !projectsQuery.data) {
    return <LoadingState label="Loading projects…" />;
  }

  if (projectsQuery.error) {
    return (
      <ErrorState
        title="Couldn't load projects"
        description="Something went wrong while loading the projects connected to this server."
        onRetry={() => mutate(queryKeys.projects.list())}
      />
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h2 className="text-xl font-semibold text-fg">Projects</h2>
        <div className="flex shrink-0 items-center gap-2">
          <button
            type="button"
            disabled={importBindings.isMutating}
            onClick={async () => {
              setImportError(null);
              try {
                const result = await importBindings.trigger();
                setImportRows(result.data);
              } catch (cause) {
                setImportError(
                  intakeErrorMessage(
                    cause,
                    "Existing intake bindings could not be imported.",
                  ),
                );
              }
            }}
            className={SECONDARY_BUTTON_CLASS}
          >
            <ArrowDownTrayIcon className="size-4" aria-hidden="true" />
            {importBindings.isMutating ? "Importing…" : "Import existing projects"}
          </button>
          <Link to="/projects/new" className={PRIMARY_BUTTON_CLASS}>
            <PlusIcon className="size-4" aria-hidden="true" />
            Connect a project
          </Link>
        </div>
      </div>

      {importRows || importError ? (
        <Panel title="Import existing projects">
          <div className="space-y-3 px-4 py-3.5">
            <p className="text-xs/5 text-fg-3">
              Each registered intake binding is resolved against GitHub and
              linked only on an exact repository match. Nothing is enabled, and
              conflicts are reported per row instead of being skipped.
            </p>
            {importError ? <ErrorMessage message={importError} /> : null}
            {importRows ? (
              <ul className="space-y-1.5">
                {importRows.map((row) => (
                  <li
                    key={row.binding}
                    className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5 text-xs/5"
                  >
                    <span className="font-mono text-fg-2">{row.binding}</span>
                    {row.repository ? (
                      <span className="font-mono text-fg-muted">{row.repository}</span>
                    ) : null}
                    <span className={`font-medium ${IMPORT_TONE[row.status]}`}>
                      {row.status}
                    </span>
                    {row.project_id ? (
                      <Link
                        to={`/projects/${encodeURIComponent(row.project_id)}`}
                        className="font-mono text-mint hover:text-fg hover:underline"
                      >
                        {row.project_id}
                      </Link>
                    ) : null}
                    {row.message ? <span className="text-fg-3">{row.message}</span> : null}
                  </li>
                ))}
              </ul>
            ) : null}
          </div>
        </Panel>
      ) : null}

      {projects.length === 0 ? (
        <EmptyState
          icon={FolderIcon}
          title="Connect your first project"
          description="Connecting an existing GitHub repository records it in Fabro so automations and feature requests can point at it. It creates no clone, no deployment, and no automation."
          action={
            <Link to="/projects/new" className={PRIMARY_BUTTON_CLASS}>
              <PlusIcon className="size-4" aria-hidden="true" />
              Connect a project
            </Link>
          }
        />
      ) : (
        <Panel title="Connected projects">
          {automationsQuery.error ? (
            <Row
              title="Automation counts"
              help="Counts and attention status come from the automation catalog."
            >
              <div className="space-y-1">
                <p role="alert" className="text-coral">
                  Couldn't load automations, so counts are unavailable.
                </p>
                <button
                  type="button"
                  onClick={() => mutate(queryKeys.automations.list())}
                  className="text-mint underline hover:text-fg"
                >
                  Retry
                </button>
              </div>
            </Row>
          ) : null}
          {projects.map((project) => (
            <ProjectRow
              key={project.id}
              project={project}
              instances={automations.filter(
                (automation) => automation.project_id === project.id,
              )}
            />
          ))}
        </Panel>
      )}
    </div>
  );
}

function ProjectRow({
  project,
  instances,
}: {
  project: Project;
  instances: Automation[];
}) {
  const attention = projectAutomationAttention(instances);
  return (
    <Row
      title={
        <Link
          to={`/projects/${encodeURIComponent(project.id)}`}
          className="font-medium text-fg-2 hover:text-fg"
        >
          {project.name}
        </Link>
      }
      help={<span className="font-mono">{project.id}</span>}
    >
      <div className="space-y-1">
        <a
          href={`https://github.com/${project.repository}`}
          target="_blank"
          rel="noreferrer"
          className="block truncate font-mono text-xs text-fg-2 hover:text-fg hover:underline"
          title={project.repository}
        >
          {project.repository}
        </a>
        <p className="text-xs/5 text-fg-3">
          {instances.length}{" "}
          {plural(instances.length, "automation", "automations")}
          <span className="text-fg-muted"> · </span>
          {project.intake_binding_id
            ? "Feature intake connected"
            : "Feature intake not configured"}
        </p>
        <p className={attention ? "text-xs/5 text-amber" : "text-xs/5 text-fg-muted"}>
          {attention
            ? `Needs attention: ${attention}`
            : "No automation issues reported"}
        </p>
      </div>
    </Row>
  );
}
