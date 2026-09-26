import { useRef, useState } from "react";
import { Link, useNavigate } from "react-router";
import { useSWRConfig } from "swr";
import { ChevronRightIcon } from "@heroicons/react/20/solid";

import { RepositoryPicker } from "../components/repository-picker";
import { Panel, Row, Label } from "../components/settings-panel";
import {
  ErrorMessage,
  INPUT_CLASS,
  PRIMARY_BUTTON_CLASS,
  SECONDARY_BUTTON_CLASS,
} from "../components/ui";
import { useToast } from "../components/toast";
import { ApiError, apiData, projectsApi } from "../lib/api-client";
import {
  PROJECT_ID_PATTERN,
  canonicalRepositorySlug,
  connectedProjectIdFromConflict,
  proposeProjectId,
} from "../lib/project";
import { useProjects } from "../lib/queries";
import { queryKeys } from "../lib/query-keys";

export function meta() {
  return [{ title: "Connect a project — Fabro" }];
}

export const handle = { hideHeader: true };

export default function ProjectsNew() {
  const navigate = useNavigate();
  const { mutate } = useSWRConfig();
  const toast = useToast();
  const projectsQuery = useProjects();
  const projects = projectsQuery.data?.data ?? [];

  const [repository, setRepository] = useState("");
  const [name, setName] = useState("");
  const [id, setId] = useState("");
  const idTouched = useRef(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [conflictProjectId, setConflictProjectId] = useState<string | null>(null);

  const canonicalRepository = canonicalRepositorySlug(repository);
  const repositoryFieldError =
    repository.trim() !== "" && canonicalRepository === null
      ? "Enter the repository as owner/repo."
      : null;
  const idFieldError = id !== "" && !PROJECT_ID_PATTERN.test(id)
    ? "Use lower-case letters, digits, and dashes, starting with a letter or digit."
    : null;
  const canSubmit =
    canonicalRepository !== null &&
    name.trim() !== "" &&
    PROJECT_ID_PATTERN.test(id) &&
    !submitting;

  function onRepositoryChange(next: string) {
    setRepository(next);
    if (idTouched.current) return;
    const slug = canonicalRepositorySlug(next);
    setId(slug ? proposeProjectId(slug.split("/")[1]) : "");
  }

  function onNameChange(next: string) {
    setName(next);
    if (!idTouched.current) setId(proposeProjectId(next));
  }

  async function onSubmit(event: React.FormEvent) {
    event.preventDefault();
    if (!canSubmit || !canonicalRepository) return;
    setSubmitting(true);
    setError(null);
    setConflictProjectId(null);
    const trimmedName = name.trim();
    const projectId = id.trim();
    try {
      const project = await apiData(() =>
        projectsApi.createProject({
          id:          projectId,
          name:        trimmedName,
          repository:  canonicalRepository,
        }),
      );
      await mutate(queryKeys.projects.list());
      toast.push({ message: `Project “${trimmedName}” connected.` });
      navigate(`/projects/${encodeURIComponent(project.id)}`);
    } catch (cause) {
      if (cause instanceof ApiError) {
        setError(cause.message);
        setConflictProjectId(connectedProjectIdFromConflict(cause));
      } else {
        setError("Couldn't connect the project. Please try again.");
      }
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={onSubmit} className="space-y-6">
      <div>
        <nav className="mb-4 flex items-center gap-1 text-sm text-fg-muted">
          <Link to="/projects" className="text-fg-3 hover:text-fg">
            Projects
          </Link>
          <ChevronRightIcon className="size-3" aria-hidden="true" />
          <span>Connect a project</span>
        </nav>
        <h2 className="text-xl font-semibold text-fg">Connect a project</h2>
        <p className="mt-2 max-w-prose text-sm leading-relaxed text-fg-3">
          Connecting an existing GitHub repository records it in Fabro so work
          can point at it. Fabro reads the repository with its own GitHub
          credentials; nothing is cloned, deployed, or scheduled by connecting
          it.
        </p>
      </div>

      <Panel title="Repository">
        <div className="space-y-2 px-4 py-3.5">
          <p className="text-sm text-fg-2">
            Repositories available to this Fabro server
          </p>
          <RepositoryPicker
            value={canonicalRepository ?? ""}
            onChange={onRepositoryChange}
            projects={projects}
          />
          {projectsQuery.error ? (
            <p role="alert" className="text-xs/5 text-coral">
              Couldn&apos;t load the projects already connected, so connected
              repositories are not marked here. The server still rejects a
              duplicate connection.
            </p>
          ) : null}
        </div>
        <Row
          title={<Label required>owner/repo</Label>}
          help="Canonical repository slug. Use this when a repository is missing from the list above; both paths save the same thing."
        >
          <div className="space-y-1">
            <input
              type="text"
              name="repository"
              aria-label="Repository owner and name"
              aria-invalid={repositoryFieldError !== null}
              value={repository}
              onChange={(event) => onRepositoryChange(event.target.value)}
              placeholder="acme/orders-api"
              autoComplete="off"
              spellCheck={false}
              className={`${INPUT_CLASS} font-mono`}
            />
            <p className="text-xs/5 text-fg-muted">
              {canonicalRepository
                ? `Saves as ${canonicalRepository}, re-read from GitHub on the server.`
                : "The server re-reads this repository's identity and default branch before saving."}
            </p>
          </div>
        </Row>
      </Panel>

      <Panel title="Project">
        <Row
          title={<Label required>Name</Label>}
          help="Shown wherever this project is listed."
        >
          <input
            type="text"
            name="name"
            aria-label="Project name"
            value={name}
            onChange={(event) => onNameChange(event.target.value)}
            placeholder="Orders API"
            autoComplete="off"
            className={INPUT_CLASS}
          />
        </Row>
        <Row
          title={<Label required>Project id</Label>}
          help={
            <>
              Identifier used in the URL:{" "}
              <span className="font-mono text-fg-2">
                /projects/{id || "<id>"}
              </span>
            </>
          }
        >
          <div className="space-y-1">
            <input
              type="text"
              name="project_id"
              aria-label="Project id"
              aria-invalid={idFieldError !== null}
              value={id}
              onChange={(event) => {
                idTouched.current = true;
                setId(event.target.value.trim().toLowerCase());
              }}
              placeholder="orders-api"
              autoComplete="off"
              spellCheck={false}
              className={`${INPUT_CLASS} font-mono`}
            />
            {idFieldError ? (
              <p className="text-xs/5 text-coral">{idFieldError}</p>
            ) : null}
          </div>
        </Row>
        <Row
          title="Setup after connecting"
          help="Feature intake and automations are enabled separately, once the project exists."
        >
          <p className="text-xs/5 text-fg-3">
            {repositoryFieldError
              ? "Pick a repository to continue."
              : canonicalRepository
                ? `Connect ${canonicalRepository}, then choose automations and feature intake on the project page.`
                : "Nothing is cloned, deployed, or scheduled by connecting."}
          </p>
        </Row>
      </Panel>

      {error ? <ErrorMessage message={error} /> : null}
      {conflictProjectId ? (
        <p className="text-sm text-fg-3">
          <Link
            to={`/projects/${encodeURIComponent(conflictProjectId)}`}
            className="text-mint underline hover:text-fg"
          >
            Open project {conflictProjectId}
          </Link>
        </p>
      ) : null}

      <div className="flex items-center justify-end gap-3 pt-2">
        <button
          type="button"
          onClick={() => navigate("/projects")}
          disabled={submitting}
          className={SECONDARY_BUTTON_CLASS}
        >
          Cancel
        </button>
        <button type="submit" disabled={!canSubmit} className={PRIMARY_BUTTON_CLASS}>
          {submitting ? "Connecting…" : "Connect project"}
        </button>
      </div>
    </form>
  );
}
