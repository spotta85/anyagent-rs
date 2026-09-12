// The anyagent binary as a TypeScript API: spawn `anyagent serve`, write
// command lines, route reply and event lines. Every rule lives in the
// binary; this file is a pipe (ticket 13, W1–W10).

import { spawn, type ChildProcess } from "node:child_process";
import { createRequire } from "node:module";
import { createInterface } from "node:readline";

import type {
  AgentDetails,
  AgentRef,
  Answer,
  ConfigValue,
  Delivery,
  DiscoveryReport,
  ErrorBody,
  Event,
  EventKind,
  Frame1,
  PlanUsage,
  RollbackScope,
  SessionInfo,
  SessionStatus,
} from "./types.ts";

export type * from "./types.ts";

/** A command line without its `id`. */
export type Command = Frame1;
/** What `open` accepts besides the agent. */
export type OpenOptions = Omit<Extract<Command, { cmd: "open" }>, "cmd" | "agent">;
/** The variant name of an `EventKind`: `"TextDelta"`, `"TurnEnded"`, … */
export type EventKindName = EventKind extends infer K ? (K extends string ? K : keyof K) : never;

export interface StartOptions {
  /** Path to the binary; default: `ANYAGENT_BIN`, then the platform package. */
  bin?: string;
  /** A mock script (`packages/mock-scripts/*.json`): no real agents. */
  mock?: string;
  /** Environment for the binary and the agents it spawns; default: this process's. */
  env?: NodeJS.ProcessEnv;
}

type Pending = { resolve: (v: unknown) => void; reject: (e: Error) => void; settle?: (ok: unknown) => unknown };

/** The wire protocol this package speaks; the binary's hello must match. */
const PROTOCOL = 1;

/** One `anyagent serve` process. */
export class Runtime {
  private child!: ChildProcess;
  private next = 1;
  private pending = new Map<number, Pending>();
  private sessions = new Map<string, Session>();
  private dead?: AnyagentError;
  private exited!: Promise<number | null>;
  private onHello?: () => void;

  /** Spawns the binary; resolves after its hello line. */
  static async start(opts: StartOptions = {}): Promise<Runtime> {
    const rt = new Runtime();
    const args = opts.mock ? ["serve", "--mock", opts.mock] : ["serve"];
    rt.child = spawn(resolveBinary(opts.bin), args, { stdio: ["pipe", "pipe", "inherit"], env: opts.env });
    rt.child.stdin!.on("error", () => {}); // EPIPE after death; onExit reports it
    createInterface({ input: rt.child.stdout! }).on("line", (line) => rt.onLine(line));
    rt.exited = new Promise((resolve) => rt.child.once("close", (code) => resolve(code))); // after stdout drains
    rt.exited.then((code) => rt.onExit(code)); // W4
    const hello = new Promise<void>((resolve, reject) => {
      rt.onHello = resolve;
      rt.child.once("error", reject);
    });
    await Promise.race([hello, rt.exited.then(() => Promise.reject(rt.dead))]);
    return rt;
  }

  discover(): Promise<DiscoveryReport> {
    return this.call({ cmd: "discover" });
  }
  probe(agent: AgentRef): Promise<AgentDetails> {
    return this.call({ cmd: "probe", agent });
  }
  planUsage(agent: AgentRef): Promise<PlanUsage> {
    return this.call({ cmd: "plan_usage", agent });
  }
  /** One-shot text with no session to manage: titles, commit messages. */
  generate(agent: AgentRef, opts: OpenOptions, prompt: string): Promise<string> {
    return this.call({ cmd: "generate", agent, ...opts, prompt });
  }

  /** Opens a session. The Session is registered inside onLine (W1). */
  open(agent: AgentRef, opts: OpenOptions): Promise<Session> {
    return this.call({ cmd: "open", agent, ...opts }, (ok) => {
      const info = ok as SessionInfo;
      const session = new Session(this, info);
      this.sessions.set(info.id, session);
      return session;
    });
  }

  /** Graceful and idempotent (W5): close stdin, wait up to 5 s, then kill. */
  async close(): Promise<number | null> {
    if (!this.dead) {
      this.child.stdin!.end();
      const kill = setTimeout(() => this.child.kill(), 5000);
      await this.exited;
      clearTimeout(kill);
    }
    return this.exited;
  }

  /** Writes one command line; resolves with the reply's `ok`, or `settle(ok)`. */
  call<T>(cmd: Command, settle?: (ok: unknown) => T): Promise<T> {
    if (this.dead) return Promise.reject(this.dead); // W4
    const id = this.next++;
    this.child.stdin!.write(JSON.stringify({ id, ...cmd }) + "\n");
    return new Promise((resolve, reject) => this.pending.set(id, { resolve: resolve as Pending["resolve"], reject, settle }));
  }

  /** Routes one stdout line. Synchronous on purpose: W1 depends on it. */
  private onLine(line: string) {
    if (this.dead) return;
    let msg: Record<string, any> | undefined;
    try {
      msg = JSON.parse(line);
    } catch {}
    if (!msg || typeof msg !== "object") return this.abort(`not a frame: ${line}`);
    if ("hello" in msg) {
      if (msg.hello.protocol !== PROTOCOL) return this.abort(`protocol ${msg.hello.protocol}, this package speaks ${PROTOCOL}`);
      return this.onHello?.();
    }
    if (typeof msg.id === "number") {
      const p = this.pending.get(msg.id);
      this.pending.delete(msg.id);
      if (!p) return;
      if (msg.error) return p.reject(new AnyagentError(msg.error));
      return p.resolve(p.settle ? p.settle(msg.ok) : msg.ok);
    }
    if ("event" in msg) return this.sessions.get(msg.event.session_id)?.push(msg.event);
    if ("session" in msg) return this.sessions.get(msg.session)?.fail(new AnyagentError(msg.error)); // W3
    if ("closed" in msg) {
      this.sessions.get(msg.closed)?.end();
      this.sessions.delete(msg.closed);
    }
  }

  /** A binary that does not speak the protocol (W10): kill it; onExit fails the rest. */
  private abort(why: string) {
    this.dead = new AnyagentError({ kind: "ProtocolFailed", message: why });
    this.child.kill();
  }

  /** Process gone: fail everything still waiting (W4). */
  private onExit(code: number | null) {
    this.dead ??= new AnyagentError({ kind: "ProcessExited", message: `anyagent exited (${code})`, status: String(code), stderr: "" });
    for (const p of this.pending.values()) p.reject(this.dead);
    this.pending.clear();
    for (const s of this.sessions.values()) s.fail(this.dead);
    this.sessions.clear();
  }
}

/** Unread events a session may hold before it is closed as lagging (W6). */
const CAP = 4096;

/** One open session: commands in, an ordered event stream out. */
export class Session {
  readonly id: string;
  /** Live: replaced on every `SessionUpdated` (W2). */
  info: SessionInfo;
  /** Live: replaced on every `StatusChanged` (W2). */
  status: SessionStatus;
  private queue: Event[] = [];
  private waiter?: () => void;
  private done = false;
  private error?: Error;
  private rt: Runtime;

  constructor(rt: Runtime, info: SessionInfo) {
    this.rt = rt;
    this.id = info.id;
    this.info = info;
    this.status = info.status ?? "Idle";
  }

  prompt(text: string, attachments: string[] = []): Promise<Delivery> {
    return this.rt.call({ cmd: "prompt", session: this.id, text, attachments });
  }
  answer(request: string, answer: Answer): Promise<void> {
    return this.rt.call({ cmd: "answer", session: this.id, request, answer });
  }
  configure(option: string, value: ConfigValue): Promise<void> {
    return this.rt.call({ cmd: "configure", session: this.id, option, value });
  }
  cancel(clearQueue = false): Promise<void> {
    return this.rt.call({ cmd: "cancel", session: this.id, clear_queue: clearQueue });
  }
  dequeue(prompt: string): Promise<void> {
    return this.rt.call({ cmd: "dequeue", session: this.id, prompt });
  }
  rollback(turns: number, scope: RollbackScope): Promise<void> {
    return this.rt.call({ cmd: "rollback", session: this.id, turns, scope });
  }
  compact(): Promise<void> {
    return this.rt.call({ cmd: "compact", session: this.id });
  }
  close(): Promise<void> {
    return this.rt.call({ cmd: "close", session: this.id });
  }

  /**
   * This session's events in order. Ends after `closed`; throws once on a
   * session error, then ends (W3).
   */
  async *events(): AsyncGenerator<Event, void, undefined> {
    while (true) {
      if (this.queue.length) {
        yield this.queue.shift()!;
        continue;
      }
      if (this.error) {
        const error = this.error;
        this.error = undefined;
        this.done = true;
        throw error;
      }
      if (this.done) return;
      await new Promise<void>((resolve) => (this.waiter = resolve));
    }
  }

  /** From Runtime.onLine: keeps info and status live (W2), applies the cap (W6). */
  push(ev: Event) {
    if (this.error || this.done) return;
    if (typeof ev.kind === "object") {
      if ("SessionUpdated" in ev.kind) this.info = ev.kind.SessionUpdated;
      if ("StatusChanged" in ev.kind) this.status = ev.kind.StatusChanged;
    }
    this.queue.push(ev);
    if (this.queue.length > CAP) {
      this.fail(new AnyagentError({ kind: "ConsumerLagged", message: `${CAP} events unread` }));
      void this.close().catch(() => {});
      return;
    }
    this.wake();
  }
  fail(error: Error) {
    if (this.error || this.done) return;
    this.error = error;
    this.wake();
  }
  end() {
    this.done = true;
    this.wake();
  }
  private wake() {
    this.waiter?.();
    this.waiter = undefined;
  }
}

/** The variant name of `event.kind`, for both `{TextDelta: {..}}` and `"ContextCompacted"` (W7). */
export function kindOf(ev: Event): EventKindName {
  return (typeof ev.kind === "string" ? ev.kind : Object.keys(ev.kind)[0]) as EventKindName;
}

/** Narrows to one variant: `if (is(ev, "TextDelta")) ev.kind.TextDelta.text` (W7). */
export function is<K extends EventKindName>(ev: Event, kind: K): ev is Event & { kind: Extract<EventKind, K | Record<K, unknown>> } {
  return kindOf(ev) === kind;
}

/** `kind`, `message`, and every extra field from the wire in `data` (W8). */
export class AnyagentError extends Error {
  readonly kind: string;
  readonly data: Record<string, unknown>;
  constructor(body: ErrorBody) {
    super(body.message);
    const { kind, message: _, ...data } = body;
    this.name = "AnyagentError";
    this.kind = kind;
    this.data = data;
  }
}

const TARGETS = ["darwin-arm64", "darwin-x64", "linux-x64", "linux-arm64", "win32-x64"];

/** `bin`, then `ANYAGENT_BIN`, then the platform package's binary. */
function resolveBinary(bin?: string): string {
  const explicit = bin ?? process.env.ANYAGENT_BIN;
  if (explicit) return explicit;
  const target = `${process.platform}-${process.arch}`;
  const exe = process.platform === "win32" ? ".exe" : "";
  try {
    return createRequire(import.meta.url).resolve(`@anyagent-ts/${target}/bin/anyagent${exe}`);
  } catch {
    throw new Error(`no anyagent binary for ${target}: set ANYAGENT_BIN, or use one of ${TARGETS.join(", ")}`);
  }
}
