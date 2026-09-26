import { afterEach, beforeEach, describe, expect, mock, test } from "bun:test";
import TestRenderer, { act } from "react-test-renderer";
import { createMemoryRouter, RouterProvider } from "react-router";
import type { PaginatedRunList, Run } from "@qltysh/fabro-api-client";

import { ToastProvider } from "../toast";
import { TEST_PRINCIPAL } from "../../lib/test-fixtures";
import { setupReactTestEnv, textContent } from "../../lib/test-utils";
import type { RunWithStatus } from "../../data/runs";
import { RunsListView } from "./runs-list-view";

mock.module("swr", () => ({
  useSWRConfig: () => ({ mutate: () => Promise.resolve(undefined) }),
}));

function run(id: string, repo: string): Run {
  return {
    id,
    goal:             `Run ${id}`,
    title:            `Run ${id}`,
    workflow:         { slug: "release", name: "release", graph_name: null, node_count: 0, edge_count: 0 },
    automation:       null,
    repository:       { name: repo, origin_url: null, provider: "github" },
    created_by:       TEST_PRINCIPAL,
    origin:           { kind: "api" },
    labels:           {},
    lifecycle:        {
      status:          { kind: "succeeded", reason: "completed" },
      approval:        null,
      pending_control: null,
      queue_position:  null,
      error:           null,
      archived:        false,
      archived_at:     null,
    },
    sandbox:          null,
    models:           [],
    source_directory: null,
    timestamps:       {
      created_at:     "2026-04-19T12:00:00Z",
      started_at:     "2026-04-19T12:01:00Z",
      last_event_at:  "2026-04-19T12:04:00Z",
      completed_at:   "2026-04-19T12:05:00Z",
    },
    billing:          null,
    size:             "XS",
    diff:             null,
    pull_request:     null,
    current_question: null,
    superseded_by:    null,
    retried_from:     null,
    links:            { web: null },
  };
}

let teardownReactEnv: (() => void) | undefined;
const mountedRenderers: TestRenderer.ReactTestRenderer[] = [];

beforeEach(() => {
  teardownReactEnv = setupReactTestEnv();
});

afterEach(() => {
  for (const renderer of mountedRenderers.splice(0)) {
    act(() => renderer.unmount());
  }
  teardownReactEnv?.();
  teardownReactEnv = undefined;
});

async function renderView(
  data: PaginatedRunList,
  rowFilter?: (run: RunWithStatus) => boolean,
) {
  const router = createMemoryRouter(
    [
      {
        path: "/",
        element: (
          <RunsListView
            data={data}
            isLoading={false}
            emptyState={<div>No runs yet. When this automation runs, the runs will appear here.</div>}
            sort="created_at"
            direction="desc"
            page={1}
            pageSize={25}
            hiddenColumns={new Set()}
            onSortClick={() => {}}
            onPageChange={() => {}}
            onPageSizeChange={() => {}}
            query=""
            workflowFilter="all"
            createdCutoffMs={null}
            rowFilter={rowFilter}
          />
        ),
      },
    ],
    { initialEntries: ["/"] },
  );
  let renderer!: TestRenderer.ReactTestRenderer;
  await act(async () => {
    renderer = TestRenderer.create(
      <ToastProvider>
        <RouterProvider router={router} />
      </ToastProvider>,
    );
  });
  mountedRenderers.push(renderer);
  return renderer;
}

describe("RunsListView row filtering", () => {
  // Regression test: automation-detail.tsx used to pre-filter `data` by repo
  // before handing it to RunsListView, which made a non-empty page whose rows
  // the repo filter hid look server-empty and show the caller's `emptyState`
  // ("No runs yet…") instead of "No matching runs". The `rowFilter` prop lets
  // a caller filter rendered rows while RunsListView still derives emptiness
  // from the unfiltered `data`.
  test("shows \"No matching runs\", not the passed emptyState, when a row filter hides every row on a non-empty page", async () => {
    const data: PaginatedRunList = {
      data: [run("run-1", "qlty/fabro"), run("run-2", "qlty/docs")],
      meta: { has_more: false, total: 2 },
    };

    const renderer = await renderView(data, () => false);

    const text = textContent(renderer.root);
    expect(text).toContain("No matching runs");
    expect(text).not.toContain("No runs yet");
  });

  test("shows the passed emptyState when the server page itself is empty", async () => {
    const data: PaginatedRunList = { data: [], meta: { has_more: false, total: 0 } };

    const renderer = await renderView(data);

    const text = textContent(renderer.root);
    expect(text).toContain("No runs yet");
  });

  test("with no row filter, rows from a non-empty page render normally", async () => {
    const data: PaginatedRunList = {
      data: [run("run-1", "qlty/fabro")],
      meta: { has_more: false, total: 1 },
    };

    const renderer = await renderView(data);

    const text = textContent(renderer.root);
    expect(text).toContain("Run run-1");
    expect(text).not.toContain("No matching runs");
    expect(text).not.toContain("No runs yet");
  });
});
