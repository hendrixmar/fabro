import { FeatureIntakeApiAxiosParamCreator } from "@qltysh/fabro-api-client";

import { apiErrorFromFetchResponse, generatedApiConfiguration } from "./api-client";

/** Advisor session key for a project's unsaved feature request. */
export function newRequestChatKey(projectId: string): string {
  return `nueva-${projectId}`;
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export interface StreamIntakeAdvisorOptions {
  projectId: string;
  /** Advisor session key: a project draft key or an existing feature-request id. */
  key: string;
  text: string;
  signal?: AbortSignal;
  fetchImpl?: FetchLike;
  onChunk: (chunk: string) => void;
}

/**
 * Stream the read-only refinement advisor.
 *
 * The stream is text-only output from the model: closing or aborting it
 * approves and publishes nothing, so callers never need a compensating write.
 */
export async function streamIntakeAdvisor({
  projectId,
  key,
  text,
  signal,
  fetchImpl = fetch,
  onChunk,
}: StreamIntakeAdvisorOptions): Promise<void> {
  const request = await FeatureIntakeApiAxiosParamCreator(
    generatedApiConfiguration,
  ).streamProjectIntakeChat(projectId, key, { text }, { signal });
  const response = await fetchImpl(request.url, {
    method:      "POST",
    credentials: "same-origin",
    headers:     request.options.headers as HeadersInit,
    body:        request.options.data as string | undefined,
    signal,
  });
  const error = await apiErrorFromFetchResponse(response);
  if (error) throw error;

  const body = response.body;
  if (!body) return;

  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let done = false;

  while (!done) {
    // Streaming readers must consume chunks sequentially to preserve SSE order
    // and decoder state.
    // react-doctor-disable-next-line react-doctor/async-await-in-loop
    const chunk = await reader.read();
    if (chunk.done) break;
    buffer += decoder.decode(chunk.value, { stream: true });
    ({ buffer, done } = drainAdvisorFrames(buffer, onChunk));
  }

  buffer += decoder.decode();
  drainAdvisorFrames(`${buffer}\n\n`, onChunk);
}

/** Consume complete `data:` frames, returning the unparsed tail. */
function drainAdvisorFrames(
  buffer: string,
  onChunk: (chunk: string) => void,
): { buffer: string; done: boolean } {
  let cursor = 0;
  let done = false;
  while (true) {
    const match = /\r?\n\r?\n/g.exec(buffer.slice(cursor));
    if (!match) break;
    const next = cursor + match.index;
    const frame = buffer.slice(cursor, next);
    cursor = next + match[0].length;

    const lines = frame.split(/\r?\n/);
    if (lines.some((line) => line.trim() === "event: done")) {
      done = true;
      continue;
    }
    const data = lines
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice("data:".length).trimStart())
      .join("\n");
    if (!data) continue;

    let payload: unknown;
    try {
      payload = JSON.parse(data);
    } catch {
      continue;
    }
    if (typeof payload === "string" && payload !== "") onChunk(payload);
  }
  return { buffer: buffer.slice(cursor), done };
}
