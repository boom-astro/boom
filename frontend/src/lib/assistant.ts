/**
 * Client for the Babamul assistant service (boom-mcp's `/assistant` routes).
 *
 * The service runs the model and its tool calls server side, with the caller's
 * own token, and streams what happens as server-sent events. When it is not
 * deployed, `/health` does not answer and the client hides the assistant.
 */

export type ChatRole = "user" | "assistant";

export type ChatMessage = { role: ChatRole; content: string };

export type ChatContext = {
  path?: string;
  survey?: string;
  object_id?: string;
};

export type ChatRequest = {
  messages: ChatMessage[];
  context?: ChatContext;
};

export type ToolImage = { mime_type: string; data: string };

export type AssistantEvent =
  | { type: "status"; message: string }
  | { type: "text"; delta: string }
  | { type: "tool_call"; id: string; name: string; arguments: Record<string, unknown> }
  | {
      type: "tool_result";
      id: string;
      name: string;
      ok: boolean;
      summary: string;
      images: ToolImage[];
    }
  | { type: "done"; content: string }
  | { type: "error"; message: string };

export type AssistantHealth = { model: string; vision: boolean };

/** The most messages sent with a question, counting the question. */
export const MAX_HISTORY = 30;

export function assistantBase(): string {
  const configured = import.meta.env.VITE_ASSISTANT_URL as string | undefined;
  return (configured || "/api/assistant").replace(/\/+$/, "");
}

/** Resolves to null when the assistant is not deployed or not healthy; never throws. */
export async function checkAssistant(signal?: AbortSignal): Promise<AssistantHealth | null> {
  try {
    const res = await fetch(`${assistantBase()}/health`, { signal });
    // A SPA fallback answers unknown paths with index.html and a 200, so the
    // body has to say it is the assistant.
    if (!res.ok) return null;
    const body: unknown = await res.json();
    if (!isObject(body) || body.status !== "ok") return null;
    return {
      model: typeof body.model === "string" ? body.model : "",
      vision: body.vision === true,
    };
  } catch {
    return null;
  }
}

export type AssistantErrorKind =
  | "auth"
  | "busy"
  | "unavailable"
  | "bad_request"
  | "interrupted"
  | "aborted";

const DEFAULT_MESSAGES: Record<AssistantErrorKind, string> = {
  auth: "Your session has expired. Log in again.",
  busy: "One question at a time. Try again in a moment.",
  unavailable: "The assistant is unavailable right now. Try again later.",
  bad_request: "The assistant could not read that request.",
  interrupted: "The answer was cut off. Try again.",
  aborted: "Stopped.",
};

export class AssistantError extends Error {
  readonly kind: AssistantErrorKind;
  readonly status?: number;

  constructor(kind: AssistantErrorKind, message?: string, status?: number) {
    super(message || DEFAULT_MESSAGES[kind]);
    this.name = "AssistantError";
    this.kind = kind;
    this.status = status;
  }
}

/** Maps a response that failed before streaming to what the panel shows. */
export function errorForStatus(status: number, serverMessage?: string): AssistantError {
  if (status === 401) return new AssistantError("auth", undefined, status);
  // The server tells "already running" apart from "too many", so its words win.
  if (status === 429) return new AssistantError("busy", serverMessage, status);
  if (status === 400 || status === 413 || status === 422) {
    return new AssistantError("bad_request", serverMessage, status);
  }
  return new AssistantError("unavailable", undefined, status);
}

/**
 * Incremental parser for a `text/event-stream` body.
 *
 * Feed it decoded text in whatever pieces the network delivers; it calls
 * `onData` once per complete event with the event's joined `data:` lines.
 * `end()` dispatches a final event that was not followed by a blank line.
 */
export function createSSEParser(onData: (data: string) => void) {
  let buffer = "";
  let dataLines: string[] = [];

  const dispatch = () => {
    if (dataLines.length > 0) onData(dataLines.join("\n"));
    dataLines = [];
  };

  const processLine = (line: string) => {
    if (line === "") {
      dispatch();
      return;
    }
    if (line.startsWith(":")) return;
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "data") dataLines.push(value);
  };

  return {
    push(text: string) {
      buffer += text;
      let start = 0;
      for (let i = 0; i < buffer.length; i++) {
        const ch = buffer[i];
        if (ch !== "\n" && ch !== "\r") continue;
        // A trailing CR may be half of a CRLF still in flight.
        if (ch === "\r" && i === buffer.length - 1) break;
        processLine(buffer.slice(start, i));
        if (ch === "\r" && buffer[i + 1] === "\n") i++;
        start = i + 1;
      }
      buffer = buffer.slice(start);
    },
    end() {
      if (buffer.endsWith("\r")) buffer = buffer.slice(0, -1);
      if (buffer !== "") processLine(buffer);
      buffer = "";
      dispatch();
    },
  };
}

/** Reads one event's JSON, or null for anything this client does not know. */
export function parseAssistantEvent(data: string): AssistantEvent | null {
  let body: unknown;
  try {
    body = JSON.parse(data);
  } catch {
    return null;
  }
  if (!isObject(body)) return null;
  const str = (v: unknown) => (typeof v === "string" ? v : "");
  switch (body.type) {
    case "status":
      return { type: "status", message: str(body.message) };
    case "text":
      return { type: "text", delta: str(body.delta) };
    case "tool_call":
      return {
        type: "tool_call",
        id: str(body.id),
        name: str(body.name),
        arguments: isObject(body.arguments) ? body.arguments : {},
      };
    case "tool_result":
      return {
        type: "tool_result",
        id: str(body.id),
        name: str(body.name),
        ok: body.ok === true,
        summary: str(body.summary),
        images: Array.isArray(body.images) ? body.images.filter(isToolImage) : [],
      };
    case "done":
      return { type: "done", content: str(body.content) };
    case "error":
      return { type: "error", message: str(body.message) || DEFAULT_MESSAGES.interrupted };
    default:
      return null;
  }
}

/**
 * POSTs a question and calls `onEvent` for each event as it arrives.
 *
 * Resolves once the stream has ended with a `done` or `error` event. Rejects
 * with an `AssistantError`: an HTTP failure mapped by `errorForStatus`,
 * "unavailable" when the service cannot be reached, "interrupted" when the
 * stream ends early, and "aborted" when `signal` fires.
 */
export async function streamChat(
  body: ChatRequest,
  token: string,
  onEvent: (event: AssistantEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  let res: Response;
  try {
    res = await fetch(`${assistantBase()}/chat`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        // Also tells compressing proxies (Traefik) to pass the stream through.
        Accept: "text/event-stream",
      },
      body: JSON.stringify(body),
      signal,
    });
  } catch (err) {
    throw transportError(err, signal);
  }

  if (!res.ok) {
    throw errorForStatus(res.status, await readErrorMessage(res));
  }
  if (!res.body) throw new AssistantError("unavailable");

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let finished = false;
  const parser = createSSEParser((data) => {
    const event = parseAssistantEvent(data);
    if (!event) return;
    if (event.type === "done" || event.type === "error") finished = true;
    onEvent(event);
  });

  try {
    // `done` and `error` are the last events, even if a proxy holds the connection open.
    while (!finished) {
      if (signal?.aborted) throw new AssistantError("aborted");
      const { value, done } = await reader.read();
      if (done) break;
      parser.push(decoder.decode(value, { stream: true }));
    }
    parser.push(decoder.decode());
    parser.end();
  } catch (err) {
    reader.cancel().catch(() => {});
    if (err instanceof AssistantError) throw err;
    throw transportError(err, signal, "interrupted");
  }

  if (finished) {
    reader.cancel().catch(() => {});
    return;
  }
  throw new AssistantError(signal?.aborted ? "aborted" : "interrupted");
}

async function readErrorMessage(res: Response): Promise<string | undefined> {
  try {
    const body: unknown = JSON.parse(await res.text());
    if (isObject(body) && typeof body.error === "string" && body.error.trim()) return body.error;
  } catch {
    // Not JSON: a proxy's error page.
  }
  return undefined;
}

function transportError(
  err: unknown,
  signal: AbortSignal | undefined,
  fallback: AssistantErrorKind = "unavailable",
): AssistantError {
  if (signal?.aborted || (err instanceof DOMException && err.name === "AbortError")) {
    return new AssistantError("aborted");
  }
  return new AssistantError(fallback);
}

function isObject(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === "object" && !Array.isArray(v);
}

const IMAGE_TYPES = new Set(["image/png", "image/jpeg", "image/gif", "image/webp"]);

function isToolImage(v: unknown): v is ToolImage {
  return (
    isObject(v) &&
    typeof v.mime_type === "string" &&
    IMAGE_TYPES.has(v.mime_type) &&
    typeof v.data === "string" &&
    v.data.length > 0
  );
}

export function imageSrc(image: ToolImage): string {
  return `data:${image.mime_type};base64,${image.data}`;
}

// ---------------------------------------------------------------------------
// Conversation state, kept apart from React so it can be tested directly.

export type ToolStep = {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
  state: "running" | "ok" | "failed";
  summary?: string;
  images: ToolImage[];
};

export type AnswerPart = { kind: "text"; text: string } | { kind: "tool"; step: ToolStep };

export type UserTurn = { id: string; role: "user"; content: string; context?: ChatContext };

export type AssistantTurn = {
  id: string;
  role: "assistant";
  /** Text and tool calls in the order they streamed. */
  parts: AnswerPart[];
  /** The final answer, from `done`; what is sent back as history. */
  content: string;
  state: "running" | "done" | "stopped" | "error";
  error?: { kind: AssistantErrorKind; message: string };
};

export type Turn = UserTurn | AssistantTurn;

/** Folds one streamed event into the answer it belongs to. */
export function applyEvent(turn: AssistantTurn, event: AssistantEvent): AssistantTurn {
  switch (event.type) {
    case "status":
      return turn;
    case "text": {
      if (!event.delta) return turn;
      const parts = [...turn.parts];
      const last = parts[parts.length - 1];
      if (last?.kind === "text") {
        parts[parts.length - 1] = { kind: "text", text: last.text + event.delta };
      } else {
        parts.push({ kind: "text", text: event.delta });
      }
      return { ...turn, parts };
    }
    case "tool_call": {
      const step: ToolStep = {
        id: event.id,
        name: event.name,
        arguments: event.arguments,
        state: "running",
        images: [],
      };
      return { ...turn, parts: [...turn.parts, { kind: "tool", step }] };
    }
    case "tool_result": {
      let matched = false;
      const parts = turn.parts.map((part): AnswerPart => {
        if (matched || part.kind !== "tool" || part.step.id !== event.id) return part;
        matched = true;
        return {
          kind: "tool",
          step: {
            ...part.step,
            state: event.ok ? "ok" : "failed",
            summary: event.summary,
            images: event.images,
          },
        };
      });
      if (!matched) {
        parts.push({
          kind: "tool",
          step: {
            id: event.id,
            name: event.name,
            arguments: {},
            state: event.ok ? "ok" : "failed",
            summary: event.summary,
            images: event.images,
          },
        });
      }
      return { ...turn, parts };
    }
    case "done": {
      // `content` is the last round's text. It replaces what streamed after
      // the last tool call, in case the two differ.
      const parts = [...finishTools(turn).parts];
      if (event.content) {
        if (parts[parts.length - 1]?.kind === "text") parts.pop();
        parts.push({ kind: "text", text: event.content });
      }
      return { ...turn, parts, content: event.content, state: "done" };
    }
    case "error":
      return { ...finishTools(turn), state: "error", error: { kind: "interrupted", message: event.message } };
  }
}

/** Marks tool calls that never got a result as failed. */
export function finishTools(turn: AssistantTurn): AssistantTurn {
  if (!turn.parts.some((p) => p.kind === "tool" && p.step.state === "running")) return turn;
  return {
    ...turn,
    parts: turn.parts.map((p): AnswerPart =>
      p.kind === "tool" && p.step.state === "running" ? { kind: "tool", step: { ...p.step, state: "failed" } } : p,
    ),
  };
}

/** Text of an answer, finished or not. */
export function answerText(turn: AssistantTurn): string {
  if (turn.state === "done") return turn.content;
  return turn.parts
    .filter((p): p is Extract<AnswerPart, { kind: "text" }> => p.kind === "text")
    .map((p) => p.text)
    .join("\n\n")
    .trim();
}

/**
 * The `messages` for a new question: earlier questions with their answers,
 * text only, then the question. Questions that got no answer are left out.
 */
export function buildHistory(turns: Turn[], question: string): ChatMessage[] {
  const messages: ChatMessage[] = [];
  for (let i = 0; i < turns.length; i++) {
    const turn = turns[i];
    const next = turns[i + 1];
    if (turn.role !== "user" || next?.role !== "assistant") continue;
    const answer = answerText(next);
    if (!answer) continue;
    messages.push({ role: "user", content: turn.content }, { role: "assistant", content: answer });
  }
  messages.push({ role: "user", content: question });
  const recent = messages.slice(-MAX_HISTORY);
  while (recent.length > 1 && recent[0].role !== "user") recent.shift();
  return recent;
}

/** Restores saved turns; an answer cut off by a reload reads as stopped. */
export function parseSavedTurns(raw: string | null): Turn[] {
  if (!raw) return [];
  try {
    const turns: unknown = JSON.parse(raw);
    if (!Array.isArray(turns)) return [];
    return turns.filter(isTurn).map((turn): Turn =>
      turn.role === "assistant" && turn.state === "running" ? { ...finishTools(turn), state: "stopped" } : turn,
    );
  } catch {
    return [];
  }
}

function isTurn(v: unknown): v is Turn {
  if (!isObject(v) || typeof v.id !== "string") return false;
  if (v.role === "user") return typeof v.content === "string";
  return v.role === "assistant" && Array.isArray(v.parts) && typeof v.content === "string";
}

// ---------------------------------------------------------------------------
// Object ids in answers become links to their pages.

const ZTF_OBJECT_ID = /\bZTF\d{2}[a-z]{7}\b/g;

export type TextPiece = { text: string; objectId?: string };

export function splitObjectIds(text: string): TextPiece[] {
  const pieces: TextPiece[] = [];
  let last = 0;
  for (const match of text.matchAll(ZTF_OBJECT_ID)) {
    const start = match.index ?? 0;
    if (start > last) pieces.push({ text: text.slice(last, start) });
    pieces.push({ text: match[0], objectId: match[0] });
    last = start + match[0].length;
  }
  if (last < text.length) pieces.push({ text: text.slice(last) });
  return pieces;
}

export function objectPath(objectId: string, survey = "ZTF"): string {
  return `/objects/${encodeURIComponent(survey)}/${encodeURIComponent(objectId)}`;
}

type MdNode = { type: string; value?: string; url?: string; children?: MdNode[] };

/**
 * A remark plugin that links bare ZTF object ids. Text already inside a link
 * or code is left alone.
 */
export function remarkObjectLinks() {
  const visit = (node: MdNode) => {
    if (!node.children || node.type === "link" || node.type === "linkReference") return;
    node.children = node.children.flatMap((child): MdNode[] => {
      if (child.type !== "text" || !child.value) {
        visit(child);
        return [child];
      }
      return splitObjectIds(child.value).map((piece) =>
        piece.objectId
          ? { type: "link", url: objectPath(piece.objectId), children: [{ type: "text", value: piece.text }] }
          : { type: "text", value: piece.text },
      );
    });
  };
  return (tree: MdNode) => visit(tree);
}

/** The in-app path a markdown link points at, or null for anything else. */
export function inAppObjectPath(href: string | undefined, origin = window.location.origin): string | null {
  if (!href) return null;
  try {
    const url = new URL(href, origin);
    if (url.origin !== origin || !url.pathname.startsWith("/objects/")) return null;
    return `${url.pathname}${url.search}${url.hash}`;
  } catch {
    return null;
  }
}
