import { useState } from "react";
import { useNavigate } from "react-router";
import {
  Combobox,
  ComboboxInput,
  ComboboxOption,
  ComboboxOptions,
} from "@headlessui/react";
import type {
  GithubRepository,
  Project,
} from "@qltysh/fabro-api-client";

import { useGithubRepositories } from "../lib/queries";
import { repositoryBlockReason } from "../lib/project";
import { Spinner } from "./state";
import { COMPACT_SECONDARY_BUTTON_CLASS, INPUT_CLASS } from "./ui";

const OPTION_CLASS =
  "cursor-default rounded-md px-2.5 py-2 text-sm text-fg-3 transition-colors data-focus:bg-overlay data-focus:text-fg";

const STATUS_CLASS = "text-xs/5 text-fg-muted";

/**
 * Repository picker for the server's GitHub credentials. Pages load one at a
 * time through the server cursor; the visible list only ever contains pages
 * that already loaded successfully.
 */
export function RepositoryPicker({
  value,
  onChange,
  projects,
}: {
  value: string;
  onChange: (repository: string) => void;
  projects: Project[];
}) {
  const navigate = useNavigate();
  const {
    data: pages = [],
    error,
    isLoading,
    isValidating,
    setSize,
    mutate,
  } = useGithubRepositories();
  const [query, setQuery] = useState("");
  const repositories: GithubRepository[] = [];
  const seen = new Set<string>();
  for (const page of pages) {
    for (const repository of page.data) {
      if (seen.has(repository.id)) continue;
      seen.add(repository.id);
      repositories.push(repository);
    }
  }
  const lastPage = pages.at(-1);
  const nextCursor = lastPage?.next_cursor ?? null;

  const needle = query.trim().toLowerCase();
  const matches = needle
    ? repositories.filter((repository) =>
        repository.full_name.toLowerCase().includes(needle),
      )
    : repositories;

  function connectedProjectFor(repository: string): Project | undefined {
    const wanted = repository.toLowerCase();
    return projects.find((project) => project.repository.toLowerCase() === wanted);
  }

  return (
    <Combobox
      value={value || null}
      onChange={(next) => {
        const connected = next ? connectedProjectFor(next) : undefined;
        if (connected) {
          navigate(`/projects/${encodeURIComponent(connected.id)}`);
          return;
        }
        onChange(next ?? "");
      }}
      immediate
    >
      <ComboboxInput
        aria-label="Filter repositories"
        placeholder="Filter by owner or name…"
        autoComplete="off"
        spellCheck={false}
        displayValue={() => query}
        onChange={(event) => setQuery(event.target.value)}
        className={INPUT_CLASS}
      />
      <div className="mt-2 overflow-hidden rounded-md border border-line bg-panel/60">
        <ComboboxOptions static className="max-h-72 overflow-y-auto p-1">
          {matches.map((repository) => {
            const connected = connectedProjectFor(repository.full_name);
            const blocked = repositoryBlockReason(repository);
            return (
              <ComboboxOption
                key={repository.id}
                value={repository.full_name}
                className={OPTION_CLASS}
              >
                <span className="flex flex-wrap items-baseline justify-between gap-x-3">
                  <span className="truncate font-mono text-xs text-fg-2">
                    {repository.full_name}
                  </span>
                  <span className="text-[11px] text-fg-muted">
                    {repository.private ? "Private" : "Public"}
                    {repository.archived ? " · Archived" : ""}
                    {repository.disabled ? " · Disabled" : ""}
                    {repository.default_branch ? "" : " · No commits"}
                  </span>
                </span>
                {connected ? (
                  <span className="mt-0.5 block text-xs/5 text-teal-300">
                    Already connected to {connected.name} — selecting opens that
                    project
                  </span>
                ) : blocked ? (
                  <span className="mt-0.5 block text-xs/5 text-fg-muted">
                    {blocked}
                  </span>
                ) : null}
              </ComboboxOption>
            );
          })}
        </ComboboxOptions>

        <div className="space-y-1 border-t border-line px-2.5 py-2">
          {error ? (
            <p role="alert" className="text-xs/5 text-coral">
              {error instanceof Error && error.message
                ? error.message
                : "Couldn't load repositories from this Fabro server."}{" "}
              <button
                type="button"
                onClick={() => void mutate()}
                className="text-mint underline hover:text-fg"
              >
                Retry this page
              </button>
            </p>
          ) : isLoading || isValidating ? (
            <p role="status" className="flex items-center gap-2 text-xs/5 text-fg-muted">
              <Spinner className="size-3" />
              Loading repositories…
            </p>
          ) : null}

          {repositories.length === 0 && lastPage !== undefined ? (
            <p role="status" className={STATUS_CLASS}>
              No repositories are available to this Fabro server. Enter
              owner/repo directly below.
            </p>
          ) : null}

          {repositories.length > 0 && matches.length === 0 ? (
            <p role="status" className={STATUS_CLASS}>
              No repository in the loaded pages matches “{query.trim()}”. Load
              more, or enter owner/repo directly below.
            </p>
          ) : null}

          {repositories.length > 0 && matches.length > 0 ? (
            <p className={STATUS_CLASS}>
              Showing {matches.length} of {repositories.length} loaded{" "}
              repositories available to this Fabro server.
            </p>
          ) : null}

          {nextCursor ? (
            <button
              type="button"
              disabled={isValidating || Boolean(error)}
              onClick={() => void setSize((current) => current + 1)}
              className={COMPACT_SECONDARY_BUTTON_CLASS}
            >
              Load more repositories
            </button>
          ) : repositories.length > 0 ? (
            <p className={STATUS_CLASS}>
              All repositories available to this Fabro server are loaded.
            </p>
          ) : null}
        </div>
      </div>
    </Combobox>
  );
}
