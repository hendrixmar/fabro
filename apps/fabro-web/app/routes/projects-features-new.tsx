import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { useSWRConfig } from "swr";
import type { Project } from "@qltysh/fabro-api-client";

import { IntakeAdvisor } from "../components/intake-advisor";
import { SupervisedField } from "../components/intake-panels";
import { Markdown } from "../components/stage-renderers/primitives";
import { Panel } from "../components/settings-panel";
import { ErrorState, LoadingState } from "../components/state";
import { useToast } from "../components/toast";
import {
  ErrorMessage,
  INPUT_CLASS,
  PRIMARY_BUTTON_CLASS,
  SECONDARY_BUTTON_CLASS,
} from "../components/ui";
import { ApiError } from "../lib/api-client";
import { intakeErrorMessage } from "../lib/intake";
import { newRequestChatKey } from "../lib/intake-chat";
import { useCreateProjectIntakeInitiative } from "../lib/mutations";
import { useProject, useProjectIntake, useProjectIntakeTemplate } from "../lib/queries";
import { queryKeys } from "../lib/query-keys";

export function meta() {
  return [{ title: "New feature request — Fabro" }];
}

export default function ProjectFeatureNew() {
  const { id } = useParams<{ id: string }>();
  const { mutate } = useSWRConfig();
  const projectQuery = useProject(id);

  if (projectQuery.isLoading && !projectQuery.data) {
    return <LoadingState label="Loading project…" />;
  }

  if (!projectQuery.data) {
    return (
      <ErrorState
        title="Couldn't load this project"
        description="The project could not be read, so a feature request cannot be drafted here."
      />
    );
  }

  const project = projectQuery.data;
  const statusQuery = useProjectIntake(project.id);

  if (statusQuery.isLoading && !statusQuery.data) {
    return <LoadingState label="Loading feature intake…" />;
  }

  if (!statusQuery.data) {
    return (
      <ErrorState
        title="Couldn't load feature intake"
        description={intakeErrorMessage(
          statusQuery.error,
          "Feature intake could not be read, so a draft cannot be started.",
        )}
        onRetry={() => mutate(queryKeys.intake.status(project.id))}
      />
    );
  }

  if (statusQuery.data.setup_required || !statusQuery.data.binding) {
    return (
      <div className="space-y-3">
        <ErrorState
          title="Feature intake is not set up"
          description="Drafting needs a registered intake binding and the intake Plane states for this project."
        />
        <p className="text-center text-sm">
          <Link
            to={`/projects/${encodeURIComponent(project.id)}/features`}
            className="text-mint hover:text-fg hover:underline"
          >
            Set up feature intake
          </Link>
        </p>
      </div>
    );
  }

  // Keying the draft by project id keeps typed text across a refresh of the same
  // project and deliberately starts a fresh draft context when the project changes.
  return <DraftForm key={project.id} project={project} />;
}

function DraftForm({ project }: { project: Project }) {
  const templateQuery = useProjectIntakeTemplate(project.id, true);
  const create = useCreateProjectIntakeInitiative(project.id);
  const toast = useToast();
  const navigate = useNavigate();

  const headings = templateQuery.data?.headings ?? [];
  const [name, setName] = useState("");
  const [fields, setFields] = useState<Record<string, string>>({});
  const [supervised, setSupervised] = useState(false);
  const [activeHeading, setActiveHeading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const targetHeading = activeHeading ?? headings[0] ?? null;
  const missing = headings.filter((heading) => (fields[heading] ?? "").trim() === "");
  const canSubmit =
    name.trim() !== "" && headings.length > 0 && missing.length === 0 && supervised;

  function setField(heading: string, value: string) {
    setFields((current) => ({ ...current, [heading]: value }));
  }

  function applyReply(section: string, reply: string) {
    setFields((current) => {
      const existing = current[section] ?? "";
      return {
        ...current,
        [section]: existing.trim() === "" ? reply : `${existing.trimEnd()}\n\n${reply}`,
      };
    });
  }

  async function submit() {
    if (!canSubmit || create.isMutating) return;
    setError(null);
    try {
      const created = await create.trigger({ name: name.trim(), fields, supervised });
      toast.push({
        message:
          "Feature request submitted. Fabro drafts the PRD and leaves it for your review.",
      });
      await navigate(
        created.issue
          ? `/projects/${encodeURIComponent(project.id)}/features/${encodeURIComponent(created.issue)}`
          : `/projects/${encodeURIComponent(project.id)}/features`,
      );
    } catch (cause) {
      // The typed draft stays exactly as it is; only the error is added.
      setError(
        cause instanceof ApiError && cause.status === 409
          ? `${intakeErrorMessage(cause, "The request was refused.")} Your draft was kept.`
          : intakeErrorMessage(cause, "The feature request could not be submitted. Your draft was kept."),
      );
    }
  }

  if (templateQuery.isLoading && !templateQuery.data) {
    return <LoadingState label="Loading the feature-request template…" />;
  }

  if (!templateQuery.data) {
    return (
      <ErrorState
        title="Couldn't load the template"
        description={intakeErrorMessage(
          templateQuery.error,
          "The template sections could not be read, so there is nothing to draft against.",
        )}
      />
    );
  }

  return (
    <div className="space-y-4">
      <div>
        <h3 className="text-base font-semibold text-fg">New feature request</h3>
        <p className="mt-1 max-w-prose text-sm/6 text-fg-3">
          Answer the template sections and Fabro drafts the PRD. Approving
          documents and provisioning infrastructure are separate, later actions.
        </p>
      </div>

      <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(0,24rem)]">
        <form
          onSubmit={(event) => {
            event.preventDefault();
            void submit();
          }}
          className="min-w-0 space-y-6"
        >
          <Panel title="Request">
            <div className="px-4 py-3.5">
              <label htmlFor="intake-name" className="text-sm text-fg-2">
                Title <span aria-label="required" className="text-coral">*</span>
              </label>
              <input
                id="intake-name"
                type="text"
                value={name}
                onChange={(event) => setName(event.target.value)}
                autoComplete="off"
                className={`${INPUT_CLASS} mt-1.5`}
              />
              <p className="mt-1 text-xs/5 text-fg-muted">
                Becomes the feature-request name in Plane.
              </p>
            </div>
          </Panel>

          <Panel title="Template sections">
            {headings.map((heading) => (
              <div key={heading} className="px-4 py-3.5">
                <label
                  htmlFor={`intake-field-${heading}`}
                  className="text-sm text-fg-2"
                >
                  {heading} <span aria-label="required" className="text-coral">*</span>
                </label>
                <textarea
                  id={`intake-field-${heading}`}
                  value={fields[heading] ?? ""}
                  onChange={(event) => setField(heading, event.target.value)}
                  onFocus={() => setActiveHeading(heading)}
                  rows={4}
                  className={`${INPUT_CLASS} mt-1.5`}
                />
              </div>
            ))}
          </Panel>

          <Panel title="Submit">
            <div className="space-y-3 px-4 py-3.5">
              <SupervisedField
                id="intake-create-supervised"
                checked={supervised}
                onChange={setSupervised}
                hint="Required. Each stage pauses for your review; nothing is provisioned by submitting this request."
              />
              {missing.length > 0 ? (
                <p className="text-xs/5 text-amber">
                  Still empty: {missing.join(", ")}
                </p>
              ) : null}
              {error ? <ErrorMessage message={error} /> : null}
              <div className="flex flex-wrap items-center gap-3">
                <button
                  type="submit"
                  disabled={!canSubmit || create.isMutating}
                  className={PRIMARY_BUTTON_CLASS}
                >
                  {create.isMutating ? "Submitting…" : "Create feature request"}
                </button>
                <Link
                  to={`/projects/${encodeURIComponent(project.id)}/features`}
                  className={SECONDARY_BUTTON_CLASS}
                >
                  Cancel
                </Link>
              </div>
            </div>
          </Panel>

          <details className="rounded-md border border-line bg-panel/40">
            <summary className="cursor-pointer px-4 py-2.5 text-xs font-medium uppercase tracking-wider text-fg-muted">
              Full template
            </summary>
            <div className="border-t border-line px-4 py-3">
              <Markdown content={templateQuery.data.template} />
            </div>
          </details>
        </form>

        <div className="min-w-0 space-y-3">
          <IntakeAdvisor
            projectId={project.id}
            sessionKey={newRequestChatKey(project.id)}
            onUseInForm={(reply) => {
              if (targetHeading) applyReply(targetHeading, reply);
            }}
            placeholder="Ask what to write in a section…"
          />
          {targetHeading ? (
            <p className="text-xs text-fg-muted">
              <span className="font-medium text-fg-3">Use in form</span> appends the
              reply to <span className="font-medium text-fg-3">{targetHeading}</span>.
              Focus another section to change the target.
            </p>
          ) : null}
        </div>
      </div>
    </div>
  );
}
