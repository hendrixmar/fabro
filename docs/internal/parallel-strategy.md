# Shared-checkout parallel execution strategy

Status: implemented.

This document defines Fabro's parallel fan-out (`shape=component`) and fan-in
(`shape=tripleoctagon`) behavior.

## 1. Execution model

A static parallel node dispatches one branch for each outgoing edge. A
`for_each` parallel node has one outgoing template edge and dispatches one
branch for each item in a runtime JSON array. A branch executes the single
target node on that edge; parallel branches are not subgraph walks.
Every branch:

- receives an independent fork of the parent workflow context;
- receives the same `Arc<RunSandbox>` as the parent run;
- inherits the same sandbox working directory and `internal.work_dir`;
- runs through the normal handler dispatch path, including dry-run behavior;
- retains its branch identity, lifecycle events, and hook scope.

Branches execute concurrently. `max_parallel` limits the number that may run at
once and defaults to 4. The parallel node always waits for every branch task,
even when a branch fails or run cancellation begins. There is no early-success
join mode.

`for_each` sources use flat context lookup: try the declared key, then strip a
leading `context.` and try again. Inline arrays and managed `blob://` or
`file://` JSON references are accepted. The template target is limited to an
agent or prompt node, and nested `for_each` is rejected.

A source array above 1000 items fails deterministically before
`parallel.started`. The array is runtime data, often model-produced, so its
length is not something a workflow author reviewed. Each branch forks the
parent context once it holds a `max_parallel` slot, so live memory tracks
`max_parallel` rather than item count — the limit guards the queue of pending
branch tasks and the plan itself.

The parent context is not used as shared mutable branch state. A branch can
change its context fork without exposing those changes as top-level values to
other branches or to the parent.

## 2. Shared checkout

All branches use the run's existing sandbox and checkout. Parallel execution
creates no branch-specific:

- Git refs or branches;
- worktrees;
- base checkpoints;
- commits;
- cleanup operations;
- merges or fast-forwards.

Normal run-level checkpointing still occurs after the parallel node. Any files
left in the shared checkout by its branches are captured together by that
checkpoint.

Read-only parallel work is best effort: an agent or command can still write if
its configured capabilities permit it. Concurrent writes are allowed and are
entirely user-managed. Fabro does not lock files, enforce read-only access,
detect overlapping edits, or warn about races. Workflows that write in parallel
should coordinate externally or assign disjoint paths.

## 3. Branch results

The shared result type is:

```rust
ParallelBranchResult {
    id: String,
    index: Option<usize>,
    item_label: Option<String>,
    status: StageOutcome,
    context_updates: BTreeMap<String, serde_json::Value>,
}
```

The parallel handler stores one result per outgoing edge or runtime item in
`parallel.results`. Results preserve outgoing-edge or input order, independent
of branch completion order. New results always contain `index`; it is optional
only so records written before indexed identity still deserialize.
`item_label` is set for `for_each` from item `name`, then `label`, then index.
`parallel.branch_count` stores the number of dispatched branches.

`context_updates` includes changes made in the branch context and updates
returned by the branch outcome. This applies to successful and failed branches,
including structured values, `response.<node_id>`, and `command.output`.
Engine-internal context keys are omitted. A task failure or panic cannot provide
updates that were never returned, but its result still preserves the original
branch ID and index.

Branch updates remain nested in their result. Fabro never merges them into the
parent's top-level context, so branches cannot collide through context keys.

The parallel stage outcome is:

- `succeeded` when every branch succeeds;
- `failed` when every branch fails;
- `partially_succeeded` for mixed outcomes, partial outcomes, and a static
  fan-out with zero branches;
- `succeeded` for a valid `for_each` source with zero items.

For `for_each`, a missing key, missing blob, invalid JSON, or non-array fails
before `parallel.started`. A valid empty array emits paired parallel events
with count zero and jumps directly to the template target's fan-in.

A dry run reaches the fan-out before any upstream node has produced the array,
so an absent or unusable source stands in one placeholder item and the template
target is simulated once. Graph-shape mistakes — a non-string source, the wrong
edge count, a non-LLM target, nested `for_each` — still fail under `--dry-run`,
because catching those is what a dry run is for.

## 4. Artifacts and downstream context

Large context values use the normal artifact store. Offloading replaces
oversized values within each branch's `context_updates` while retaining the
outer object and array structure of `parallel.results`.

When Fabro constructs execution or prompt context, it resolves nested textual
blob references under `response.*` and `command.output`, including those keys
inside a branch result's `context_updates`. This lets a prompted fan-in inspect
complete branch text without flattening branch state into the parent context.

`parallel.results` is runtime context. Fabro does not materialize a
`parallel_results.json` file in the workspace. Diagnostic run dumps may export
stage projection data, but that export is not a workflow handoff mechanism and
is not visible as a checkout file to downstream nodes.

## 5. Fan-in

Fan-in is an explicit join node.

A fan-in node without a nonblank prompt validates that `parallel.results`
exists and deserializes as typed branch results. It then succeeds with a joined
branches note. It is a no-op barrier: it does not alter context or workspace
state.

A fan-in node with a prompt delegates to the standard prompt handler. It sees
the aggregated runtime context in the normal prompt preamble and records the
normal prompt-stage outputs:

- `response.<fan_in_id>`;
- `last_response`;
- model usage and timing;
- prompt and response events.

A prompted fan-in synthesizes results. It does not rank branches, select a
winner, restore files, or choose workspace state.

## 6. Events and projections

Parallel execution emits:

- `parallel.started` with `visit` and `branch_count`;
- `parallel.branch.started` with stable branch identity, index, and optional
  item label;
- `parallel.branch.completed` with index, optional item label, duration, and
  status;
- `parallel.completed` with counts and the ordered typed result array.

The final typed array is also projected into
`StageProjection.parallel_results`. Raw runtime items are recorded once in the
existing `stage.prompt` event and are not duplicated in branch events or
results.

Branch attempts use the same artifact, panic, and executor-timeout envelope as
ordinary nodes, wrapped in a branch-local retry loop. A retry preserves its
branch identity, stage scope, item label, context fork, and result index. It
releases the concurrency permit during backoff and reacquires it before the
next attempt. Generic graph lifecycle callbacks, edge selection, thread reuse,
and per-item checkpoints are intentionally excluded.

## 7. Cancellation

Semaphore acquisition observes the run cancellation token. Branches still
waiting for a permit when cancellation fires stop without executing; because
`StageOutcome` has no cancelled variant, their results record a failed outcome
(reason `branch cancelled`). Branches already executing continue through their
handler's cooperative cancellation path. The parallel handler joins every task
before returning `Error::Cancelled` to the run executor.

Cancellation does not trigger branch Git cleanup because no branch Git state is
created.

## 8. Product constraints

- Branches remain single-node executions.
- `max_parallel` remains supported.
- Results are deterministic in outgoing-edge order.
- There is no branch-selection score, SHA, model-usage mode, notice, or UI.
- Server-owned independent checkout/worktree behavior is separate and remains
  unchanged.
