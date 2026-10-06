import { Link, useParams } from "react-router";
import { useSWRConfig } from "swr";

import { Panel, Row } from "../components/settings-panel";
import { ErrorState, LoadingState } from "../components/state";
import {
  PROJECT_AUTOMATION_STATE_LABEL,
  projectAutomationAttention,
  projectAutomationState,
} from "../lib/project";
import { useAutomations, useProject } from "../lib/queries";
import { queryKeys } from "../lib/query-keys";
import { plural } from "../lib/plural";

export function meta() {
  return [{ title: "Project setup — Fabro" }];
}

export default function ProjectSetup() {
  const { id } = useParams<{ id: string }>();
  const { mutate } = useSWRConfig();
  const projectQuery = useProject(id);
  const instancesQuery = useAutomations({ scope: "project", projectId: id });
  const sharedQuery = useAutomations({
    scope: "global",
    availableToProjects: true,
  });
  const project = projectQuery.data;
  const instances = instancesQuery.data?.data ?? [];
  const shared = sharedQuery.data?.data ?? [];

  if (projectQuery.isLoading && !project) {
    return <LoadingState label="Loading project…" />;
  }

  if (!project) {
    return (
      <ErrorState
        title="Couldn't load this project"
        description="The project could not be read, so its setup state is unavailable."
      />
    );
  }

  const attention = projectAutomationAttention(instances);

  return (
    <div className="space-y-6">
      <p className="max-w-prose text-sm leading-relaxed text-fg-3">
        What this build can report for {project.name}: its repository, the
        automations it owns, and whether a feature-intake binding is recorded.
        There is no aggregate readiness badge — each automation reports its own
        environment, trigger, and last-error state.
      </p>

      <Panel title="Repository">
        <Row title="Repository" help="Canonical slug re-read from GitHub by the server.">
          <a
            href={`https://github.com/${project.repository}`}
            target="_blank"
            rel="noreferrer"
            className="font-mono text-xs text-mint hover:text-fg hover:underline"
          >
            {project.repository}
          </a>
        </Row>
        <Row title="Default branch" help="Automations owned by this project target this branch.">
          <span className="font-mono text-xs text-fg-2">{project.default_branch}</span>
        </Row>
        <Row
          title="Project id"
          help="Repository identity is immutable after connecting; renaming the project does not rebind automation targets."
        >
          <span className="font-mono text-xs text-fg-2">{project.id}</span>
        </Row>
      </Panel>

      <Panel title="Feature intake">
        <Row
          title="Setup"
          help="Feature intake arrives with the intake integration. This build exposes no intake setup action, so nothing is configured from here."
        >
          <span className="text-fg-muted">Not available in this build</span>
        </Row>
        <Row
          title="Binding"
          help="Recorded once feature intake is set up for this project."
        >
          {project.intake_binding_id ? (
            <span className="font-mono text-xs text-fg-2">
              {project.intake_binding_id}
            </span>
          ) : (
            <span className="text-fg-muted">None recorded</span>
          )}
        </Row>
        <Row
          title="Feature requests"
          help="The feature-request surface reports intake state without inventing readiness."
        >
          <Link
            to={`/projects/${encodeURIComponent(project.id)}/features`}
            className="text-mint hover:text-fg hover:underline"
          >
            Open feature requests
          </Link>
        </Row>
      </Panel>

      <Panel title="Automations">
        {instancesQuery.error ? (
          <Row title="Project automations" help="Read from the automation catalog.">
            <div className="space-y-1">
              <p role="alert" className="text-coral">
                Couldn&apos;t load this project&apos;s automations.
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
        ) : (
          <>
            <Row
              title="Owned instances"
              help="Concrete per-project automations. Each keeps its own triggers, schedule, and run history."
            >
              <div className="space-y-1">
                <p>
                  {instances.length}{" "}
                  {plural(instances.length, "automation", "automations")}
                </p>
                {instances.length > 0 ? (
                  <ul className="space-y-0.5 text-xs/5 text-fg-3">
                    {instances.map((automation) => (
                      <li key={automation.id}>
                        <Link
                          to={`/automations/${encodeURIComponent(automation.id)}`}
                          className="font-mono text-mint hover:text-fg hover:underline"
                        >
                          {automation.id}
                        </Link>
                        {" · "}
                        {
                          PROJECT_AUTOMATION_STATE_LABEL[
                            projectAutomationState(automation)
                          ]
                        }
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-fg-muted">
                    None yet. Add one from the automations tab.
                  </p>
                )}
                {attention ? (
                  <p className="text-amber">Needs attention: {attention}</p>
                ) : null}
              </div>
            </Row>
            <Row
              title="Available to projects"
              help="Global definitions a project can enroll. Enrollment copies the workflow configuration, never the trigger activation."
            >
              {sharedQuery.error ? (
                <span className="text-fg-muted">
                  Couldn&apos;t read the global automation catalog.
                </span>
              ) : (
                <span>
                  {shared.length}{" "}
                  {plural(shared.length, "definition", "definitions")}{" "}
                  <Link
                    to="/automations?scope=global"
                    className="text-mint hover:text-fg hover:underline"
                  >
                    Review global automations
                  </Link>
                </span>
              )}
            </Row>
          </>
        )}
      </Panel>
    </div>
  );
}
