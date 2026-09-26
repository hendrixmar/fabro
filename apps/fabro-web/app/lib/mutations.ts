import useSWRMutation from "swr/mutation";
import { useSWRConfig, type ScopedMutator } from "swr";
import type {
  IntakeActionResponse,
  IntakeCreateRequest,
  IntakeCreateResponse,
  IntakeImportResponse,
  IntakePauseStatus,
  IntakeReadinessReport,
  IntakeStageRequest,
  PreviewUrlResponse,
  Project,
  Run,
  SteerRunRequest,
  SubmitAnswerRequest,
  UpdateRunRequest,
} from "@qltysh/fabro-api-client";

import {
  apiData,
  authApi,
  featureIntakeApi,
  humanInTheLoopApi,
  runsApi,
} from "./api-client";
import { mutateRunListCaches } from "./board-cache";
import { queryKeys } from "./query-keys";
import type { LifecycleAction, LifecycleActionError } from "./run-actions";
import {
  approveRun,
  archiveRun,
  cancelRun,
  denyRun,
  isLifecycleActionError,
  retryRun,
  unarchiveRun,
} from "./run-actions";

export type PreviewRunArg = {
  port: number;
  expires_in_secs: number;
  signed?: boolean;
};

export type PreviewMutationResult = {
  intent: "preview";
  url: string;
};

export type LifecycleMutationResult =
  | {
      intent: LifecycleAction;
      ok: true;
      run: Run;
    }
  | {
      intent: LifecycleAction;
      ok: false;
      error: LifecycleActionError | null;
    };

export function usePreviewRun(id: string | undefined) {
  return useSWRMutation(
    id ? queryKeys.runs.preview(id) : null,
    async (_key, { arg }: { arg: PreviewRunArg }): Promise<PreviewMutationResult> => {
      const result = await apiData<PreviewUrlResponse>(() =>
        humanInTheLoopApi.generatePreviewUrl(id!, arg),
      );
      return { intent: "preview", url: result.url };
    },
  );
}

export function useCancelRun(id: string | undefined) {
  return useLifecycleMutation(id, "cancel", cancelRun);
}

export function useApproveRun(id: string | undefined) {
  return useLifecycleMutation(id, "approve", approveRun);
}

export function useDenyRun(id: string | undefined) {
  return useLifecycleMutation(id, "deny", denyRun);
}

export function useArchiveRun(id: string | undefined) {
  return useLifecycleMutation(id, "archive", archiveRun);
}

export function useUnarchiveRun(id: string | undefined) {
  return useLifecycleMutation(id, "unarchive", unarchiveRun);
}

export function useRetryRun(id: string | undefined) {
  return useLifecycleMutation(id, "retry", retryRun, (run, mutate) => {
    void mutate(queryKeys.runs.detail(run.id), run, { revalidate: false });
    if (run.parent_id) {
      mutateRunListCaches(mutate);
    }
  });
}

function useLifecycleMutation(
  id: string | undefined,
  intent: LifecycleAction,
  action: (id: string) => Promise<Run>,
  onSuccessExtra?: (run: Run, mutate: ScopedMutator) => void,
) {
  const { mutate } = useSWRConfig();
  const key = id ? queryKeys.runs[intent](id) : null;
  return useSWRMutation(
    key,
    async (): Promise<LifecycleMutationResult> => {
      if (!id) {
        return { intent, ok: false, error: null };
      }
      try {
        return { intent, ok: true, run: await action(id) };
      } catch (error) {
        return {
          intent,
          ok: false,
          error: isLifecycleActionError(error) ? error : null,
        };
      }
    },
    {
      onSuccess: (result) => {
        if (!id || !result.ok) return;
        if (intent !== "retry") {
          // Keep the returned lifecycle state visible while revalidation
          // observes the durable follow-up event (notably a 202 cancel).
          void mutate(queryKeys.runs.detail(id), result.run, { revalidate: true });
          void mutate(queryKeys.runs.billing(id));
        }
        mutateRunListCaches(mutate);
        onSuccessExtra?.(result.run, mutate);
      },
    },
  );
}

export function useUpdateRunTitle(id: string | undefined) {
  const { mutate } = useSWRConfig();
  return useSWRMutation(
    id ? queryKeys.runs.updateTitle(id) : null,
    async (_key, { arg }: { arg: UpdateRunRequest }): Promise<Run> => {
      if (!id) throw new Error("id is required");
      return apiData(() => runsApi.updateRun(id, arg));
    },
    {
      onSuccess: (run) => {
        if (!id) return;
        void mutate(queryKeys.runs.detail(id), run, { revalidate: false });
        mutateRunListCaches(mutate);
      },
    },
  );
}

export type SubmitInterviewAnswerArg = {
  questionId: string;
  answer: SubmitAnswerRequest;
};

export function useSubmitInterviewAnswer(runId: string | undefined) {
  const { mutate } = useSWRConfig();
  return useSWRMutation(
    runId ? `interview-answer:${runId}` : null,
    async (_key: string, { arg }: { arg: SubmitInterviewAnswerArg }) => {
      if (!runId) throw new Error("runId is required");
      await apiData(() =>
        humanInTheLoopApi.submitRunAnswer(runId, arg.questionId, arg.answer),
      );
    },
    {
      onSuccess: () => {
        if (!runId) return;
        void mutate(queryKeys.runs.questions(runId, 25, 0));
        void mutate(queryKeys.runs.detail(runId));
      },
    },
  );
}

export function useInterruptRun(runId: string | undefined) {
  const { mutate } = useSWRConfig();
  return useSWRMutation(
    runId ? `interrupt-run:${runId}` : null,
    async (_key: string) => {
      if (!runId) throw new Error("runId is required");
      await apiData(() => humanInTheLoopApi.interruptRun(runId));
    },
    {
      onSuccess: () => {
        if (!runId) return;
        void mutate(queryKeys.runs.detail(runId));
      },
    },
  );
}

export function useSteerRun(runId: string | undefined) {
  const { mutate } = useSWRConfig();
  return useSWRMutation(
    runId ? `steer-run:${runId}` : null,
    async (_key: string, { arg }: { arg: SteerRunRequest }) => {
      if (!runId) throw new Error("runId is required");
      await apiData(() => humanInTheLoopApi.steerRun(runId, arg));
    },
    {
      onSuccess: () => {
        if (!runId) return;
        void mutate(queryKeys.runs.detail(runId));
      },
    },
  );
}

// Feature intake. Every mutation invalidates only the intake keys it can
// change, plus the project it belongs to, so a provider error elsewhere on the
// page never drops the global project or automation lists.

function invalidateIntakeStatus(mutate: ScopedMutator, projectId: string) {
  void mutate(queryKeys.intake.status(projectId));
}

function invalidateIntakeProject(mutate: ScopedMutator, projectId: string) {
  invalidateIntakeStatus(mutate, projectId);
  void mutate(queryKeys.intake.initiatives(projectId));
}

function invalidateIntakeInitiative(
  mutate: ScopedMutator,
  projectId: string,
  issue: string,
) {
  void mutate(queryKeys.intake.initiative(projectId, issue));
  void mutate(queryKeys.intake.history(projectId, issue));
  void mutate(queryKeys.intake.initiatives(projectId));
}

function useIntakeAction<TArg, TResult>(
  key: readonly unknown[],
  run: (arg: TArg) => Promise<TResult>,
  invalidate: (mutate: ScopedMutator) => void,
) {
  const { mutate } = useSWRConfig();
  return useSWRMutation(key, (_key, { arg }: { arg: TArg }) => run(arg), {
    onSuccess: () => invalidate(mutate),
  });
}

export interface SetupProjectIntakeArgs {
  plane_project_id: string;
  /** Current project revision, sent as `If-Match`. */
  revision: string;
}

/**
 * Set up feature intake. The server re-reads GitHub and Plane identity and
 * links the binding only after it can read the registration back, so a stale
 * `If-Match` fails with the project untouched.
 */
export function useSetupProjectIntake(projectId: string | undefined) {
  const { mutate } = useSWRConfig();
  return useIntakeAction<SetupProjectIntakeArgs, Project>(
    projectId ? queryKeys.intake.status(projectId) : [],
    (arg) =>
      apiData(() =>
        featureIntakeApi.setupProjectIntake(projectId!, arg.revision, {
          plane_project_id: arg.plane_project_id,
        }),
      ),
    () => {
      if (!projectId) return;
      void mutate(queryKeys.projects.detail(projectId));
      invalidateIntakeStatus(mutate, projectId);
    },
  );
}

export function useDetachProjectIntake(projectId: string | undefined) {
  const { mutate } = useSWRConfig();
  return useIntakeAction<void, Project>(
    projectId ? [...queryKeys.intake.status(projectId), "detach"] : [],
    () => apiData(() => featureIntakeApi.detachProjectIntake(projectId!)),
    () => {
      if (!projectId) return;
      void mutate(queryKeys.projects.detail(projectId));
      invalidateIntakeStatus(mutate, projectId);
    },
  );
}

export function useSetProjectIntakePause(projectId: string | undefined) {
  return useIntakeAction<{ paused: boolean; reason: string }, IntakePauseStatus>(
    projectId ? [...queryKeys.intake.status(projectId), "pause"] : [],
    (arg) =>
      apiData(() => featureIntakeApi.setProjectIntakePause(projectId!, arg)),
    (mutate) => {
      if (projectId) invalidateIntakeStatus(mutate, projectId);
    },
  );
}

export function useRecheckProjectIntakeReadiness(projectId: string | undefined) {
  return useIntakeAction<void, IntakeReadinessReport>(
    projectId ? [...queryKeys.intake.status(projectId), "recheck"] : [],
    () => apiData(() => featureIntakeApi.recheckProjectIntakeReadiness(projectId!)),
    (mutate) => {
      if (projectId) invalidateIntakeStatus(mutate, projectId);
    },
  );
}

export function useCreateProjectIntakeInitiative(projectId: string | undefined) {
  return useIntakeAction<IntakeCreateRequest, IntakeCreateResponse>(
    projectId ? [...queryKeys.intake.initiatives(projectId), "create"] : [],
    (arg) =>
      apiData(() => featureIntakeApi.createProjectIntakeInitiative(projectId!, arg)),
    (mutate) => {
      if (projectId) invalidateIntakeProject(mutate, projectId);
    },
  );
}

export function useCommentProjectIntakeInitiative(
  projectId: string | undefined,
  issue: string | undefined,
) {
  return useIntakeAction<{ text: string }, IntakeActionResponse>(
    projectId && issue ? queryKeys.intake.initiative(projectId, issue) : [],
    (arg) =>
      apiData(() =>
        featureIntakeApi.commentProjectIntakeInitiative(projectId!, issue!, arg),
      ),
    (mutate) => {
      if (projectId && issue) invalidateIntakeInitiative(mutate, projectId, issue);
    },
  );
}

export function useApproveProjectIntakeInitiative(
  projectId: string | undefined,
  issue: string | undefined,
) {
  return useIntakeAction<{ supervised: boolean }, IntakeActionResponse>(
    projectId && issue ? [...queryKeys.intake.initiative(projectId, issue), "approve"] : [],
    (arg) =>
      apiData(() =>
        featureIntakeApi.approveProjectIntakeInitiative(projectId!, issue!, arg),
      ),
    (mutate) => {
      if (projectId && issue) invalidateIntakeInitiative(mutate, projectId, issue);
    },
  );
}

export function useReviseProjectIntakeInitiative(
  projectId: string | undefined,
  issue: string | undefined,
) {
  return useIntakeAction<{ text: string; supervised?: boolean }, IntakeActionResponse>(
    projectId && issue ? [...queryKeys.intake.initiative(projectId, issue), "revise"] : [],
    (arg) =>
      apiData(() =>
        featureIntakeApi.reviseProjectIntakeInitiative(projectId!, issue!, arg),
      ),
    (mutate) => {
      if (projectId && issue) invalidateIntakeInitiative(mutate, projectId, issue);
    },
  );
}

export function useCancelProjectIntakeInitiative(
  projectId: string | undefined,
  issue: string | undefined,
) {
  return useIntakeAction<{ supervised: boolean }, IntakeActionResponse>(
    projectId && issue ? [...queryKeys.intake.initiative(projectId, issue), "cancel"] : [],
    (arg) =>
      apiData(() =>
        featureIntakeApi.cancelProjectIntakeInitiative(projectId!, issue!, arg),
      ),
    (mutate) => {
      if (projectId && issue) invalidateIntakeInitiative(mutate, projectId, issue);
    },
  );
}

export function useRunProjectIntakeStage(
  projectId: string | undefined,
  issue: string | undefined,
) {
  return useIntakeAction<IntakeStageRequest, IntakeActionResponse>(
    projectId && issue ? [...queryKeys.intake.initiative(projectId, issue), "run"] : [],
    (arg) =>
      apiData(() => featureIntakeApi.runProjectIntakeStage(projectId!, issue!, arg)),
    (mutate) => {
      if (projectId && issue) invalidateIntakeInitiative(mutate, projectId, issue);
    },
  );
}

export function useReconcileProjectIntakeInitiative(
  projectId: string | undefined,
  issue: string | undefined,
) {
  return useIntakeAction<void, IntakeActionResponse>(
    projectId && issue ? [...queryKeys.intake.initiative(projectId, issue), "reconcile"] : [],
    () =>
      apiData(() =>
        featureIntakeApi.reconcileProjectIntakeInitiative(projectId!, issue!),
      ),
    (mutate) => {
      if (projectId && issue) invalidateIntakeInitiative(mutate, projectId, issue);
    },
  );
}

/**
 * Explicit provisioning. Never implied by a document approval: it may configure
 * staging and CI and create implementation tickets.
 */
export function useExecuteProjectIntakeInitiative(
  projectId: string | undefined,
  issue: string | undefined,
) {
  return useIntakeAction<{ supervised: boolean }, IntakeActionResponse>(
    projectId && issue ? [...queryKeys.intake.initiative(projectId, issue), "execute"] : [],
    (arg) =>
      apiData(() =>
        featureIntakeApi.executeProjectIntakeInitiative(projectId!, issue!, arg),
      ),
    (mutate) => {
      if (projectId && issue) invalidateIntakeInitiative(mutate, projectId, issue);
    },
  );
}

export function useImportProjectIntakeBindings() {
  const { mutate } = useSWRConfig();
  return useSWRMutation(
    ["intake", "import"] as const,
    async (): Promise<IntakeImportResponse> =>
      apiData(() => featureIntakeApi.importProjectIntakeBindings()),
    {
      onSuccess: () => {
        void mutate(queryKeys.projects.list());
      },
    },
  );
}

export function useLoginDevToken() {
  return useSWRMutation(
    queryKeys.auth.loginDevToken(),
    async (_key, { arg }: { arg: { token: string } }) => {
      return apiData(() => authApi.loginDevToken(arg), {
        redirectOnUnauthorized: false,
      });
    },
  );
}
