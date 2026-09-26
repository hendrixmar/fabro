import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router";
import {
  Combobox,
  ComboboxInput,
  ComboboxOption,
  ComboboxOptions,
} from "@headlessui/react";
import type {
  GithubRepository,
  GithubRepositoryListResponse,
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
  const [cursors, setCursors] = useState<Array<string | null>>([null]);
  const [pages, setPages] = useState<
    Array<{ cursor: string | null; response: GithubRepositoryListResponse }>
  >([]);
  const [query, setQuery] = useState("");

  const handleLoaded = useCallback(
    (cursor: string | null, response: GithubRepositoryListResponse) => {
      setPages((current) =>
        current.some(
          (page) => page.cursor === cursor && page.response === response,
        )
          ? current
          : [
              ...current.filter((page) => page.cursor !== cursor),
              { cursor, response },
            ],
      );
    },
    [],
  );

  const repositories: GithubRepository[] = [];
  const seen = new Set<string>();
  for (const cursor of cursors) {
    const page = pages.find((candidate) => candidate.cursor === cursor);
    for (const repository of page?.response.data ?? []) {
      if (seen.has(repository.id)) continue;
      seen.add(repository.id);
      repositories.push(repository);
    }
  }
  const lastCursor = cursors[cursors.length - 1];
  const lastPage = pages.find((page) => page.cursor === lastCursor);
  const nextCursor = lastPage?.response.next_cursor ?? null;

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
          {cursors.map((cursor) => (
            <RepositoryPageLoader
              key={cursor ?? "first-page"}
              cursor={cursor}
              onLoaded={handleLoaded}
            />
          ))}

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
              onClick={() =>
                setCursors((current) => [...current, nextCursor])
              }
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

function RepositoryPageLoader({
  cursor,
  onLoaded,
}: {
  cursor: string | null;
  onLoaded: (
    cursor: string | null,
    response: GithubRepositoryListResponse,
  ) => void;
}) {
  const { data, error, isLoading, mutate } = useGithubRepositories(cursor);

  useEffect(() => {
    if (data) onLoaded(cursor, data);
  }, [cursor, data, onLoaded]);

  if (error) {
    return (
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
    );
  }

  if (isLoading && !data) {
    return (
      <p className="flex items-center gap-2 text-xs/5 text-fg-muted">
        <Spinner className="size-3" />
        Loading repositories…
      </p>
    );
  }

  return null;
}
