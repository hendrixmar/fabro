import { useRef, useState } from "react";
import { PaperAirplaneIcon } from "@heroicons/react/20/solid";

import { Markdown } from "./stage-renderers/primitives";
import { Spinner } from "./state";
import {
  ErrorMessage,
  INPUT_CLASS,
  SECONDARY_BUTTON_CLASS,
} from "./ui";
import { ApiError } from "../lib/api-client";
import { useMountEffect } from "../hooks/effects";
import { streamIntakeAdvisor } from "../lib/intake-chat";
import { intakeErrorMessage } from "../lib/intake";

const COMPACT_BUTTON_CLASS =
  "inline-flex items-center gap-1.5 rounded-md border border-line bg-overlay px-2.5 py-1 text-xs text-fg-2 transition-colors hover:bg-overlay-strong hover:text-fg disabled:cursor-not-allowed disabled:opacity-50";

/**
 * Read-only refinement advisor.
 *
 * It streams model output for the operator's own session and nothing else: the
 * reply is a suggestion until the operator applies it, and it can never approve
 * a document or launch a run. Closing the page or the stream publishes nothing.
 */
export function IntakeAdvisor({
  projectId,
  sessionKey,
  onUseInForm,
  onPostComment,
  postCommentPending = false,
  placeholder = "Ask the advisor to sharpen an answer…",
}: {
  projectId: string;
  sessionKey: string;
  onUseInForm?: (reply: string) => void;
  onPostComment?: (reply: string) => void;
  postCommentPending?: boolean;
  placeholder?: string;
}) {
  const [input, setInput] = useState("");
  const [reply, setReply] = useState("");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useMountEffect(() => () => abortRef.current?.abort());

  async function send() {
    const text = input.trim();
    if (text === "" || pending) return;
    setPending(true);
    setError(null);
    setInput("");
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      await streamIntakeAdvisor({
        projectId,
        key:      sessionKey,
        text,
        signal:   controller.signal,
        onChunk:  (chunk) => setReply((current) => current + chunk),
      });
    } catch (cause) {
      // Aborting is a normal way to close the stream; it is not an error.
      if (!controller.signal.aborted) {
        setError(
          cause instanceof ApiError && cause.status === 503
            ? `${intakeErrorMessage(cause, "The advisor is unavailable.")} Nothing was approved or published.`
            : intakeErrorMessage(cause, "The advisor could not answer. Nothing was published."),
        );
      }
    } finally {
      setPending(false);
      abortRef.current = null;
    }
  }

  const hasReply = reply.trim() !== "";
  const actionsDisabled = !hasReply || pending || postCommentPending;

  return (
    <section
      aria-labelledby="intake-advisor-heading"
      className="flex min-w-0 flex-col rounded-md border border-line bg-panel/40"
    >
      <header className="border-b border-line bg-overlay px-4 py-2.5">
        <h2
          id="intake-advisor-heading"
          className="text-xs font-medium uppercase tracking-wider text-fg-muted"
        >
          Refinement advisor
        </h2>
      </header>
      <div className="flex min-w-0 flex-1 flex-col gap-3 p-4">
        <p className="text-xs/5 text-fg-3">
          Read-only advisor. Its reply is a suggestion: applying it to the form
          or posting it as a comment is your explicit action, and it can never
          approve a document or start a run.
        </p>

        <form
          onSubmit={(event) => {
            event.preventDefault();
            void send();
          }}
          className="space-y-2"
        >
          <label htmlFor="intake-advisor-input" className="sr-only">
            Ask the refinement advisor
          </label>
          <textarea
            id="intake-advisor-input"
            value={input}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
                event.preventDefault();
                void send();
              }
            }}
            rows={3}
            placeholder={placeholder}
            className={INPUT_CLASS}
          />
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="submit"
              disabled={pending || input.trim() === ""}
              className={SECONDARY_BUTTON_CLASS}
            >
              {pending ? (
                <Spinner className="size-4" />
              ) : (
                <PaperAirplaneIcon className="size-4" aria-hidden="true" />
              )}
              {pending ? "Asking…" : "Ask advisor"}
            </button>
            {hasReply || pending ? (
              <button
                type="button"
                onClick={() => {
                  abortRef.current?.abort();
                  setReply("");
                  setError(null);
                }}
                className={SECONDARY_BUTTON_CLASS}
              >
                Clear reply
              </button>
            ) : null}
          </div>
        </form>

        {error ? <ErrorMessage message={error} /> : null}

        <div
          aria-live="polite"
          aria-busy={pending}
          className="min-h-16 rounded-md border border-line bg-panel-alt px-3 py-2"
        >
          {hasReply ? (
            <Markdown content={reply} />
          ) : pending ? (
            <p className="text-sm text-fg-muted">Waiting for the advisor…</p>
          ) : (
            <p className="text-sm text-fg-muted">
              No reply yet. Ask a question to get a suggestion you can review.
            </p>
          )}
        </div>

        {hasReply ? (
          <div className="flex flex-wrap items-center gap-2">
            {onUseInForm ? (
              <button
                type="button"
                disabled={actionsDisabled}
                onClick={() => onUseInForm(reply)}
                className={COMPACT_BUTTON_CLASS}
              >
                Use in form
              </button>
            ) : null}
            {onPostComment ? (
              <button
                type="button"
                disabled={actionsDisabled}
                onClick={() => onPostComment(reply)}
                className={COMPACT_BUTTON_CLASS}
              >
                Post comment
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
    </section>
  );
}
