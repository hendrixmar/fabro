import { useState } from "react";
import { Link, Outlet, useLocation, useParams } from "react-router";
import { useSWRConfig } from "swr";
import { ChevronRightIcon } from "@heroicons/react/20/solid";
import type { Project } from "@qltysh/fabro-api-client";

import { PanelSkeleton } from "../components/settings-panel";
import { ErrorState } from "../components/state";
import { useToast } from "../components/toast";
import {
  ErrorMessage,
  INPUT_CLASS,
  SECONDARY_BUTTON_CLASS,
} from "../components/ui";
import { ApiError, apiData, projectsApi } from "../lib/api-client";
import { useProject } from "../lib/queries";
import { queryKeys } from "../lib/query-keys";

export function meta() {
  return [{ title: "Project — Fabro" }];
}

export const handle = { hideHeader: true };

export default function ProjectDetail() {
  const { id } = useParams<{ id: string }>();
  const query = useProject(id);
  const { pathname } = useLocation();

  if (query.isLoading && !query.data) {
    return <PanelSkeleton />;
  }

  if (query.error) {
    return (
      <ErrorState
        title="Couldn't load this project"
        description="The server returned an error while loading a project connected to this Fabro server."
      />
    );
  }

  if (!query.data) {
    return (
      <ErrorState
        title="Project not found"
        description="This project is not connected to this Fabro server."
      />
    );
  }

  const project = query.data;
  const basePath = `/projects/${encodeURIComponent(project.id)}`;
  const tabs = [
    {
      name:        "Automations",
      path:        `${basePath}/automations`,
      active:      !pathname.startsWith(`${basePath}/features`) &&
        !pathname.startsWith(`${basePath}/setup`),
    },
    { name: "Feature requests", path: `${basePath}/features`, active: pathname.startsWith(`${basePath}/features`) },
    { name: "Setup", path: `${basePath}/setup`, active: pathname.startsWith(`${basePath}/setup`) },
  ];

  return (
    <div className="space-y-6">
      <nav className="flex items-center gap-1 text-sm text-fg-muted">
        <Link to="/projects" className="text-fg-3 hover:text-fg">
          Projects
        </Link>
        <ChevronRightIcon className="size-3" aria-hidden="true" />
        <span>{project.name}</span>
      </nav>

      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-3">
            <ProjectName project={project} />
            <span className="font-mono text-xs text-fg-muted">{project.id}</span>
          </div>
          <p className="mt-2 flex flex-wrap items-center gap-x-2 text-sm text-fg-3">
            <a
              href={`https://github.com/${project.repository}`}
              target="_blank"
              rel="noreferrer"
              className="font-mono text-xs text-mint hover:text-fg hover:underline"
            >
              {project.repository}
            </a>
            <span className="text-fg-muted">
              default branch{" "}
              <span className="font-mono text-fg-3">{project.default_branch}</span>
            </span>
          </p>
          <p className="mt-1 text-xs/5 text-fg-muted">
            {project.intake_binding_id
              ? `Feature intake binding ${project.intake_binding_id}`
              : "Feature intake not configured"}
          </p>
        </div>
      </div>

      <div className="relative before:pointer-events-none before:absolute before:bottom-0 before:left-1/2 before:h-px before:w-screen before:-translate-x-1/2 before:bg-line">
        <nav className="-mb-px flex flex-wrap gap-6">
          {tabs.map((tab) => (
            <Link
              key={tab.name}
              to={tab.path}
              aria-current={tab.active ? "page" : undefined}
              className={`border-b-2 pb-3.5 text-sm font-medium transition-colors ${
                tab.active
                  ? "border-teal-500 text-fg"
                  : "border-transparent text-fg-muted hover:border-line-strong hover:text-fg-3"
              }`}
            >
              {tab.name}
            </Link>
          ))}
        </nav>
      </div>

      <Outlet />
    </div>
  );
}

function ProjectName({ project }: { project: Project }) {
  const { mutate } = useSWRConfig();
  const toast = useToast();
  const [editing, setEditing] = useState(false);
  const [name, setName] = useState(project.name);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function onSubmit(event: React.FormEvent) {
    event.preventDefault();
    const trimmed = name.trim();
    if (trimmed === "" || saving) return;
    setSaving(true);
    setError(null);
    try {
      await apiData(() =>
        projectsApi.updateProject(project.id, project.revision, { name: trimmed }),
      );
      await mutate(queryKeys.projects.detail(project.id));
      await mutate(queryKeys.projects.list());
      toast.push({ message: `Project renamed to “${trimmed}”.` });
      setEditing(false);
    } catch (cause) {
      setError(
        cause instanceof ApiError && cause.message
          ? cause.message
          : "Couldn't rename the project. Please try again.",
      );
      // A stale revision resolves on the next read, so pick up the new one
      // while keeping the typed name and the error visible.
      void mutate(queryKeys.projects.detail(project.id));
    } finally {
      setSaving(false);
    }
  }

  if (!editing) {
    return (
      <>
        <h2 className="text-xl font-semibold text-fg">{project.name}</h2>
        <button
          type="button"
          onClick={() => {
            setName(project.name);
            setError(null);
            setEditing(true);
          }}
          className={SECONDARY_BUTTON_CLASS}
        >
          Rename
        </button>
      </>
    );
  }

  return (
    <form onSubmit={onSubmit} className="w-full space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        <input
          type="text"
          aria-label="Project name"
          value={name}
          onChange={(event) => setName(event.target.value)}
          autoComplete="off"
          className={`${INPUT_CLASS} max-w-xs`}
        />
        <button
          type="submit"
          disabled={saving || name.trim() === ""}
          className={SECONDARY_BUTTON_CLASS}
        >
          {saving ? "Saving…" : "Save name"}
        </button>
        <button
          type="button"
          onClick={() => {
            setName(project.name);
            setError(null);
            setEditing(false);
          }}
          disabled={saving}
          className={SECONDARY_BUTTON_CLASS}
        >
          Cancel
        </button>
      </div>
      {error ? <ErrorMessage message={error} /> : null}
    </form>
  );
}
