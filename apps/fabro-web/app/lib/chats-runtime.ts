import type {
  ChatModelAdapter,
  ChatModelRunResult,
  ThreadAssistantMessagePart,
  ThreadMessageLike,
} from "@assistant-ui/react";

import type {
  Chat,
  ChatContentPart,
  ChatMessage,
  JsonValue,
} from "./chats-types";
import { pickReply } from "./chats-script";

const STREAM_CHUNK_CHARS = 28;
const STREAM_CHUNK_INTERVAL_MS = 55;

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new DOMException("Aborted", "AbortError"));
      return;
    }
    const handle = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(handle);
        reject(new DOMException("Aborted", "AbortError"));
      },
      { once: true },
    );
  });
}

function toolResultValue(content: readonly ChatContentPart[]): JsonValue {
  const texts = content.flatMap((part) =>
    part.type === "text" ? [part.text] : [],
  );
  return texts.length === content.length
    ? texts.join("")
    : (JSON.parse(JSON.stringify(content)) as JsonValue);
}

function toAssistantParts(
  content: readonly ChatContentPart[],
): ThreadAssistantMessagePart[] {
  const out: ThreadAssistantMessagePart[] = [];
  for (const part of content) {
    if (part.type === "text") {
      out.push({ type: "text", text: part.text });
    } else if (part.type === "tool_call") {
      out.push({
        type: "tool-call",
        toolCallId: part.id,
        toolName: part.name,
        args: part.input.arguments,
        argsText: JSON.stringify(part.input.arguments),
      });
    } else if (part.type === "tool_result") {
      for (let i = out.length - 1; i >= 0; i--) {
        const candidate = out[i];
        if (
          candidate?.type === "tool-call" &&
          candidate.toolCallId === part.tool_call_id
        ) {
          out[i] = { ...candidate, result: toolResultValue(part.content) };
          break;
        }
      }
    }
  }
  return out;
}

export function createScriptedAdapter(args: {
  getChat: () => Chat | undefined;
  onReplyComplete: (reply: ChatMessage) => void;
}): ChatModelAdapter {
  return {
    async *run({ abortSignal }) {
      const chat = args.getChat();
      const reply = pickReply(chat?.scriptIndex ?? 0);
      const accumulated: ChatContentPart[] = [];

      for (const part of reply.content) {
        if (part.type === "text") {
          const text = part.text;
          let cursor = 0;
          accumulated.push({ type: "text", text: "" });
          const accIndex = accumulated.length - 1;
          while (cursor < text.length) {
            cursor = Math.min(cursor + STREAM_CHUNK_CHARS, text.length);
            accumulated[accIndex] = {
              type: "text",
              text: text.slice(0, cursor),
            };
            yield buildUpdate(accumulated);
            if (cursor < text.length) {
              await sleep(STREAM_CHUNK_INTERVAL_MS, abortSignal);
            }
          }
        } else {
          accumulated.push(part);
          yield buildUpdate(accumulated);
          await sleep(STREAM_CHUNK_INTERVAL_MS * 3, abortSignal);
        }
      }

      args.onReplyComplete(reply);
    },
  };
}

function buildUpdate(parts: ChatContentPart[]): ChatModelRunResult {
  return { content: toAssistantParts(parts) };
}

export function toThreadMessages(
  messages: readonly ChatMessage[],
): ThreadMessageLike[] {
  return messages.map((msg) => {
    if (msg.role === "user") {
      const content = [];
      for (const part of msg.content) {
        if (part.type === "text") {
          content.push({ type: "text", text: part.text } as const);
        }
      }
      return {
        role: "user",
        content,
      };
    }
    if (msg.role === "assistant") {
      return {
        role: "assistant",
        content: toAssistantParts(msg.content),
      };
    }
    return { role: "system", content: [] };
  });
}
