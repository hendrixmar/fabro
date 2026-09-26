import { useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { useSWRConfig } from "swr";
import { PlusIcon } from "@heroicons/react/20/solid";
import { Dialog, DialogPanel, DialogTitle } from "@headlessui/react";
import type { Automation, Project } from "@qltysh/fabro-api-client";

import { Panel } from "../components/settings-panel";
import { EmptyState, ErrorState, LoadingState } from "../components/state";
import { useToast } from "../components/toast";
import {
  ErrorMessage,
  INPUT_CLASS,
  PRIMARY_BUTTON_CLASS,
  SECONDARY_BUTTON_CLASS,
} from "../components/ui";
import { gitTarget, workflowSourceLabel } from "../lib/automation";
import {
  PROJECT_AUTOMATION_STATE_LABEL,
  projectAutomationState,
  proposeProjectAutomationId,
} from "../lib/project";
import { isCloneBasedEnvironment, providerLabel } from "../lib/environment-providers";
import { ApiError, apiData, projectsApi } from "../lib/api-client";
import { useAutomations, useEnvironments, useProject } from "../lib/queries";
import { queryKeys } from "../lib/query-keys";

export function meta() {
  return [{ title: "Project automations — Fabro" }];
}

const STATE_TEXT_CLASS: Record<string, string> = {
  error:         "text-coral",
  configuration: "text-amber",
  enabled:       "text-mint",
  disabled:      "text-fg-muted",
};

export default function ProjectAutomations() {
  const { id } = useParams<{ id: string }>();
  const { mutate } = useSWRConfig();
  const projectQuery = useProject(id);
  const instancesQuery = useAutomations({ scope: "project", projectId: id });
  const [selecting, setSelecting] = useState(false);

  const project = projectQuery.data;
  const instances = instancesQuery.data?.data ?? [];

  if (projectQuery.isLoading && !project) {
    return <LoadingState label="Loading project…" />;
  }

  if (!project) {
    return (
      <ErrorState
        title="Couldn't load this project"
        description="The project could not be read, so its automations are unavailable."
      />
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="max-w-prose text-sm text-fg-3">
          Linked automations run a global definition against this project.
          Edits to the global reach every project; each project keeps its own
          trigger and environment.
        </p>
        <div className="flex shrink-0 items-center gap-2">
          <button
            type="button"
            onClick={() => setSelecting(true)}
            className={SECONDARY_BUTTON_CLASS}
          >
            <PlusIcon className="size-4" aria-hidden="true" />
            Select automation
          </button>
          <Link
            to={`/projects/${encodeURIComponent(project.id)}/automations/new`}
            className={PRIMARY_BUTTON_CLASS}
          >
            Create custom automation
          </Link>
        </div>
      </div>

      {instancesQuery.isLoading && !instancesQuery.data ? (
        <LoadingState label="Loading project automations…" />
      ) : instancesQuery.error ? (
        <ErrorState
          title="Couldn't load this project's automations"
          description="The automation catalog could not be read. The rest of the project page still works."
          onRetry={() => mutate(queryKeys.automations.list())}
        />
      ) : instances.length === 0 ? (
        <EmptyState
          title="No automations for this project"
          description="Add a global automation marked available to projects, or create a custom workflow that targets this repository. Nothing runs until you enable a trigger."
        />
      ) : (
        <Panel title="Project automations">
          <ul className="divide-y divide-line">
            {instances.map((automation) => (
              <InstanceRow key={automation.id} automation={automation} />
            ))}
          </ul>
        </Panel>
      )}

      {selecting ? (
        <SelectAutomationDialog
          project={project}
          onClose={() => setSelecting(false)}
        />
      ) : null}
    </div>
  );
}

function InstanceRow({ automation }: { automation: Automation }) {
  const state = projectAutomationState(automation);
  const target = gitTarget(automation.target);
  return (
    <li className="flex flex-wrap items-start justify-between gap-3 px-4 py-3.5">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <Link
            to={`/automations/${encodeURIComponent(automation.id)}`}
            className="text-sm font-medium text-fg-2 hover:text-fg"
          >
            {automation.name}
          </Link>
          <span className={`text-xs font-medium ${STATE_TEXT_CLASS[state]}`}>
            {PROJECT_AUTOMATION_STATE_LABEL[state]}
          </span>
        </div>
        <p className="mt-1 font-mono text-xs text-fg-muted">{automation.id}</p>
        <p className="mt-1 text-xs/5 text-fg-3">
          Workflow{" "}
          <span className="font-mono text-fg-2">{automation.workflow}</span>
          {" · "}
          {target
            ? `${target.repo} · ${target.branch}`
            : "unsupported run target"}
          {" · "}
          {automation.source_automation_id
            ? `Linked to ${automation.source_automation_id}`
            : "Custom"}
        </p>
        <p className="mt-1 text-xs/5 text-fg-muted">
          {automation.environment_id
            ? `Environment ${automation.environment_id}`
            : "No environment selected — select one before running"}
          {automation.triggers.some((trigger) => trigger.enabled)
            ? ` · ${automation.triggers.length} trigger(s) enabled`
            : " · all triggers disabled"}
        </p>
        {automation.last_error ? (
          <p className="mt-1 text-xs/5 text-coral">
            Last run failed: {automation.last_error}
          </p>
        ) : null}
      </div>
      <Link
        to={`/automations/${encodeURIComponent(automation.id)}/edit`}
        className={SECONDARY_BUTTON_CLASS}
      >
        Edit
      </Link>
    </li>
  );
}

function SelectAutomationDialog({
  project,
  onClose,
}: {
  project: Project;
  onClose: () => void;
}) {
  const { mutate } = useSWRConfig();
  const toast = useToast();
  const navigate = useNavigate();
  const sourcesQuery = useAutomations({
    scope: "global",
    availableToProjects: true,
  });
  const environmentsQuery = useEnvironments();
  const sources = sourcesQuery.data?.data ?? [];
  const environments = (environmentsQuery.data?.data ?? []).filter(
    isCloneBasedEnvironment,
  );

  const [source, setSource] = useState<Automation | null>(null);
  const [instanceId, setInstanceId] = useState("");
  const [instanceName, setInstanceName] = useState("");
  const [environmentId, setEnvironmentId] = useState("");
  const idTouched = useRef(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function chooseSource(next: Automation) {
    setSource(next);
    setError(null);
    if (!idTouched.current) {
      setInstanceId(proposeProjectAutomationId(project.id, next.id));
    }
    setInstanceName(`${next.name} (${project.name})`);
    if (!environmentId && next.environment_id) setEnvironmentId(next.environment_id);
  }

  async function onSubmit(event: React.FormEvent) {
    event.preventDefault();
    if (!source || submitting) return;
    const trimmedName = instanceName.trim();
    const trimmedId = instanceId.trim();
    if (trimmedName === "" || trimmedId === "" || environmentId === "") return;
    setSubmitting(true);
    setError(null);
    try {
      await apiData(() =>
        projectsApi.createProjectAutomation(project.id, {
          id:                    trimmedId,
          name:                  trimmedName,
          source_automation_id:  source.id,
          source_revision:       source.revision,
          environment_id:        environmentId,
        }),
      );
      await mutate((key) =>
        Array.isArray(key) && key[0] === "automations" && key[1] === "list",
      );
      await mutate(queryKeys.projects.detail(project.id));
      toast.push({ message: `Automation “${trimmedName}” added to ${project.name}.` });
      navigate(`/automations/${encodeURIComponent(trimmedId)}/edit`);
    } catch (cause) {
      setError(
        cause instanceof ApiError && cause.message
          ? cause.message
          : "Couldn't add the automation. Please try again.",
      );
      setSubmitting(false);
    }
  }

  const canSubmit = source !== null
    && instanceId.trim() !== ""
    && instanceName.trim() !== ""
    && environmentId !== ""
    && !submitting;

  return (
    <Dialog open onClose={() => (submitting ? undefined : onClose())} className="relative z-50">
      <div className="fixed inset-0 bg-black/60 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 flex items-start justify-center overflow-y-auto px-4 py-[10vh]">
        <DialogPanel className="w-full max-w-2xl rounded-lg border border-line-strong bg-panel shadow-2xl shadow-black/40">
          <form onSubmit={onSubmit} className="space-y-4 px-5 py-4">
            <DialogTitle className="text-sm font-semibold text-fg">
              Select an automation for {project.name}
            </DialogTitle>
            <p className="text-xs/5 text-fg-3">
              Project enrollment copies the workflow configuration, not the
              trigger activation: the new instance targets{" "}
              <span className="font-mono text-fg-2">{project.repository}</span>{" "}
              on{" "}
              <span className="font-mono text-fg-2">{project.default_branch}</span>{" "}
              and starts with every trigger disabled.
            </p>

            {sourcesQuery.isLoading && !sourcesQuery.data ? (
              <p className="text-sm text-fg-3">Loading global automations…</p>
            ) : sourcesQuery.error ? (
              <div className="space-y-1">
                <ErrorMessage message="Couldn't load global automations marked available to projects." />
                <button
                  type="button"
                  onClick={() => mutate((key) =>
                    Array.isArray(key) && key[0] === "automations" && key[1] === "list")}
                  className="text-xs text-mint underline hover:text-fg"
                >
                  Retry
                </button>
              </div>
            ) : sources.length === 0 ? (
              <p className="rounded-md border border-line bg-panel-alt px-3 py-2 text-xs/5 text-fg-3">
                No global automation is marked available to projects yet. Mark one
                available on the{" "}
                <Link to="/automations?scope=global" className="text-mint underline hover:text-fg">
                  Automations page
                </Link>
                , or create a custom automation for this project.
              </p>
            ) : (
              <>
                <fieldset className="space-y-1.5">
                  <legend className="text-sm text-fg-2">Global automation</legend>
                  <div className="max-h-56 space-y-1 overflow-y-auto rounded-md border border-line bg-panel/60 p-1">
                    {sources.map((candidate) => {
                      const selected = source?.id === candidate.id;
                      return (
                        <label
                          key={candidate.id}
                          className={`flex cursor-pointer items-start gap-2 rounded-md px-2.5 py-2 text-sm ${
                            selected ? "bg-overlay text-fg" : "text-fg-3 hover:bg-overlay/60"
                          }`}
                        >
                          <input
                            type="radio"
                            name="source_automation"
                            value={candidate.id}
                            checked={selected}
                            onChange={() => chooseSource(candidate)}
                            className="mt-1"
                          />
                          <span className="min-w-0">
                            <span className="block text-sm font-medium">
                              {candidate.name}
                            </span>
                            <span className="block font-mono text-xs text-fg-muted">
                              {candidate.id} · {workflowSourceLabel(candidate.workflow_source)}
                            </span>
                          </span>
                        </label>
                      );
                    })}
                  </div>
                </fieldset>

                <div className="grid gap-3 sm:grid-cols-2">
                  <label className="block space-y-1">
                    <span className="text-sm text-fg-2">Automation id</span>
                    <input
                      type="text"
                      aria-label="Automation id"
                      value={instanceId}
                      onChange={(event) => {
                        idTouched.current = true;
                        setInstanceId(event.target.value.trim().toLowerCase());
                      }}
                      maxLength={63}
                      autoComplete="off"
                      spellCheck={false}
                      className={`${INPUT_CLASS} font-mono`}
                    />
                  </label>
                  <label className="block space-y-1">
                    <span className="text-sm text-fg-2">Name</span>
                    <input
                      type="text"
                      aria-label="Automation name"
                      value={instanceName}
                      onChange={(event) => setInstanceName(event.target.value)}
                      autoComplete="off"
                      className={INPUT_CLASS}
                    />
                  </label>
                </div>

                <label className="block space-y-1">
                  <span className="text-sm text-fg-2">Environment</span>
                  <select
                    aria-label="Automation environment"
                    value={environmentId}
                    onChange={(event) => setEnvironmentId(event.target.value)}
                    disabled={
                      environmentsQuery.isLoading ||
                      Boolean(environmentsQuery.error)
                    }
                    className={`${INPUT_CLASS} font-mono`}
                  >
                    <option value="">
                      {environmentsQuery.isLoading ? "Loading environments…" : "Select an environment…"}
                    </option>
                    {environments.map((environment) => (
                      <option key={environment.id} value={environment.id}>
                        {environment.id} · {providerLabel(environment.provider)}
                      </option>
                    ))}
                  </select>
                  {environmentsQuery.error ? (
                    <span className="block text-xs/5 text-coral">
                      Couldn&apos;t load environments. Refresh and try again.
                    </span>
                  ) : !environmentsQuery.isLoading && environments.length === 0 ? (
                    <span className="block text-xs/5 text-fg-muted">
                      No Docker or Daytona environments exist.{" "}
                      <Link to="/settings/environments" className="text-mint hover:text-fg">
                        Create one
                      </Link>{" "}
                      before adding this automation.
                    </span>
                  ) : null}
                </label>
              </>
            )}

            {error ? <ErrorMessage message={error} /> : null}

            <div className="flex justify-end gap-2">
              <button
                type="button"
                onClick={onClose}
                disabled={submitting}
                className={SECONDARY_BUTTON_CLASS}
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={!canSubmit}
                className={PRIMARY_BUTTON_CLASS}
              >
                {submitting ? "Adding…" : "Add to project"}
              </button>
            </div>
          </form>
        </DialogPanel>
      </div>
    </Dialog>
  );
}
