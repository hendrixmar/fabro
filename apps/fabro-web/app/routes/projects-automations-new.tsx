import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { useSWRConfig } from "swr";
import { ChevronRightIcon } from "@heroicons/react/20/solid";
import type { Environment, Project } from "@qltysh/fabro-api-client";

import {
  AutomationFormFields,
  EMPTY_AUTOMATION_FORM,
  automationPayloadFromFormValues,
  isFormValid,
  type AutomationFormValues,
} from "../components/automation-form";
import { LoadingState, ErrorState } from "../components/state";
import { useToast } from "../components/toast";
import {
  ErrorMessage,
  PRIMARY_BUTTON_CLASS,
  SECONDARY_BUTTON_CLASS,
} from "../components/ui";
import { ApiError, apiData, automationsApi } from "../lib/api-client";
import { useEnvironments, useProject } from "../lib/queries";
import { queryKeys } from "../lib/query-keys";

export function meta() {
  return [{ title: "New project automation — Fabro" }];
}

export default function ProjectAutomationNew() {
  const { id } = useParams<{ id: string }>();
  const projectQuery = useProject(id);
  const environmentsQuery = useEnvironments();
  const project = projectQuery.data;

  if (projectQuery.isLoading && !project) {
    return <LoadingState label="Loading project…" />;
  }

  if (!project) {
    return (
      <ErrorState
        title="Couldn't load this project"
        description="A custom automation needs the project's repository and default branch, which could not be read."
      />
    );
  }

  return (
    <CustomAutomationForm
      key={project.id}
      project={project}
      environments={environmentsQuery.data?.data}
      environmentsLoading={environmentsQuery.isLoading && !environmentsQuery.data}
      environmentsError={Boolean(environmentsQuery.error)}
    />
  );
}

function CustomAutomationForm({
  project,
  environments = [],
  environmentsLoading,
  environmentsError,
}: {
  project: Project;
  environments?: Environment[];
  environmentsLoading: boolean;
  environmentsError: boolean;
}) {
  const navigate = useNavigate();
  const { mutate } = useSWRConfig();
  const toast = useToast();
  const [values, setValues] = useState<AutomationFormValues>({
    ...EMPTY_AUTOMATION_FORM,
    projectId:         project.id,
    targetRepository:  project.repository,
    targetBranch:      project.default_branch,
    // A project instance starts with every trigger disabled, exactly like a
    // project enrollment from a global definition.
    manualEnabled:     false,
    scheduleEnabled:   false,
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const canSubmit = isFormValid(values)
    && !environmentsLoading
    && !environmentsError
    && !submitting;

  async function onSubmit(event: React.FormEvent) {
    event.preventDefault();
    if (!canSubmit) return;
    setSubmitting(true);
    setError(null);
    const trimmedName = values.name.trim();
    try {
      await apiData(() =>
        automationsApi.createAutomation({
          id: values.id.trim(),
          ...automationPayloadFromFormValues(values),
        }),
      );
      await mutate((key) =>
        Array.isArray(key) && key[0] === "automations" && key[1] === "list",
      );
      await mutate(queryKeys.projects.detail(project.id));
      toast.push({ message: `Automation “${trimmedName}” created for ${project.name}.` });
      navigate(`/projects/${encodeURIComponent(project.id)}/automations`);
    } catch (cause) {
      setError(
        cause instanceof ApiError && cause.message
          ? cause.message
          : "Couldn't create the automation. Please try again.",
      );
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={onSubmit} className="space-y-6">
      <div>
        <nav className="mb-4 flex items-center gap-1 text-sm text-fg-muted">
          <Link
            to={`/projects/${encodeURIComponent(project.id)}/automations`}
            className="text-fg-3 hover:text-fg"
          >
            {project.name} automations
          </Link>
          <ChevronRightIcon className="size-3" aria-hidden="true" />
          <span>New automation</span>
        </nav>
        <h2 className="text-xl font-semibold text-fg">New project automation</h2>
        <p className="mt-2 max-w-prose text-sm leading-relaxed text-fg-3">
          This automation belongs to {project.name} and always runs against{" "}
          <span className="font-mono text-xs text-fg-2">{project.repository}</span>{" "}
          on{" "}
          <span className="font-mono text-xs text-fg-2">
            {project.default_branch}
          </span>
          . Workflow files can still come from a separate repository, and every
          trigger starts disabled.
        </p>
      </div>

      <AutomationFormFields
        values={values}
        onChange={setValues}
        lockTargetRepoAndBranch
        environments={environments}
        environmentsLoading={environmentsLoading}
        environmentsError={environmentsError}
      />

      {error ? <ErrorMessage message={error} /> : null}

      <div className="flex items-center justify-end gap-3 pt-2">
        <button
          type="button"
          onClick={() => navigate(`/projects/${encodeURIComponent(project.id)}/automations`)}
          disabled={submitting}
          className={SECONDARY_BUTTON_CLASS}
        >
          Cancel
        </button>
        <button type="submit" disabled={!canSubmit} className={PRIMARY_BUTTON_CLASS}>
          {submitting ? "Creating…" : "Create automation"}
        </button>
      </div>
    </form>
  );
}
