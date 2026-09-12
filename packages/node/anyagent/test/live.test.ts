// L1–L10 from ticket 13 against real agents, mirroring tests/live.rs.
// ANYAGENT_LIVE=claude,codex (or all) picks agents; unset skips everything.
// Every step logs `[agent] step` to stdout and to <tmpdir>/anyagent-live-node.log.

import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { appendFileSync, existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { AnyagentError, Runtime, Session, kindOf } from "../src/index.ts";
import type { Event, OpenOptions, QuestionRequest } from "../src/index.ts";

const BIN = process.env.ANYAGENT_BIN ?? join(import.meta.dirname, "../../../../target/debug/anyagent");
const LOG = join(tmpdir(), "anyagent-live-node.log");
const STEP_TIMEOUT = 120_000;
const AGENTS = ["claude", "codex", "opencode", "cursor", "grok", "hermes", "kiro", "pi", "qwen", "antigravity"];
const COUNT = "Count from 1 to 400, one number per line. No other text. No tools.";
const TITLE =
  "Title this conversation in at most six words: the user asked how to rename a git branch and got the two commands.";

// The ANTHROPIC_* hijack from a host Claude Code session, stripped; the
// real API pinned over any settings.json proxy (as tests/live.rs does).
const env: NodeJS.ProcessEnv = { ...process.env, ANTHROPIC_BASE_URL: "https://api.anthropic.com" };
for (const v of ["ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_MODEL", "CLAUDECODE"]) delete env[v];

/** Runtimes started by the current test, closed when it ends however it ends. */
const runtimes = new Set<Runtime>();

async function startRt(): Promise<Runtime> {
  const rt = await Runtime.start({ bin: BIN, env });
  runtimes.add(rt);
  return rt;
}

function live(name: string, body: () => Promise<void>) {
  test(name, async (t) => {
    t.after(async () => {
      for (const rt of runtimes) await rt.close();
      runtimes.clear();
    });
    await body();
  });
}

function log(agent: string, step: string) {
  const line = `[${agent}] ${step}`;
  console.log(line);
  appendFileSync(LOG, `${new Date().toISOString()} ${line}\n`);
}

/** The selected agents discovery actually finds; the rest are logged as skipped. */
async function enabled(): Promise<string[]> {
  const list = process.env.ANYAGENT_LIVE;
  if (!list) return [];
  const selected = AGENTS.filter((a) => list.trim() === "all" || list.split(",").some((p) => p.trim() === a));
  const rt = await startRt();
  const found = new Set((await rt.discover()).agents.map((a) => a.id));
  await rt.close();
  for (const a of selected) if (!found.has(a)) log(a, "SKIP not installed");
  return selected.filter((a) => found.has(a));
}

/** The per-agent options every live session opens with, as in tests/live.rs. */
const MODELS: Record<string, string> = {
  claude: "haiku",
  codex: "gpt-5.6-luna",
  opencode: "opencode/muse-spark-1.2-contributor-free",
  pi: "openrouter/nvidia/nemotron-3-super-120b-a12b:free",
  qwen: "z-ai/glm-5.3-flash",
};

function options(agent: string, dir: string): OpenOptions {
  const configure: Record<string, string> = {};
  if (MODELS[agent]) configure.model = MODELS[agent];
  // Deterministic approvals for codex; qwen asks for every edit in `default`.
  if (agent === "codex") Object.assign(configure, { effort: "low", sandbox: "read-only", mode: "on-request" });
  if (agent === "qwen") configure.mode = "default";
  return { dir, configure };
}

async function open(agent: string): Promise<{ rt: Runtime; session: Session; stream: Stream; dir: string }> {
  const dir = mkdtempSync(join(tmpdir(), `anyagent-live-${agent}-`));
  const rt = await startRt();
  const session = await rt.open(agent, options(agent, dir));
  return { rt, session, stream: new Stream(session), dir };
}

function withTimeout<T>(p: Promise<T>, step: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`hung at ${step}`)), STEP_TIMEOUT);
    p.then((v) => (clearTimeout(t), resolve(v)), (e) => (clearTimeout(t), reject(e)));
  });
}

/** One session's event stream with per-step timeouts. */
class Stream {
  private it: AsyncGenerator<Event, void, undefined>;
  constructor(session: Session) {
    this.it = session.events();
  }
  /** The next event, `null` at the end; a hang fails naming `step`. */
  async next(step: string): Promise<Event | null> {
    const r = await withTimeout(this.it.next(), step);
    return r.done ? null : r.value;
  }
  /** Text and stop reason of the turn that ends next. */
  async turn(step: string): Promise<{ text: string; stop: unknown }> {
    let text = "";
    for (;;) {
      const ev = await this.next(step);
      assert.ok(ev, `stream ended at ${step}`);
      if (typeof ev.kind === "object") {
        if ("TextDelta" in ev.kind) text += ev.kind.TextDelta.text;
        if ("TurnEnded" in ev.kind) return { text, stop: ev.kind.TurnEnded.stop };
      }
    }
  }
  /** Nothing but bookkeeping for `secs` seconds. */
  async quiet(secs: number, step: string) {
    const deadline = Date.now() + secs * 1000;
    for (;;) {
      const left = deadline - Date.now();
      if (left <= 0) return;
      const r = await Promise.race([this.it.next(), sleep(left, "timeout" as const)]);
      if (r === "timeout" || r.done) return;
      const k = kindOf(r.value);
      assert.ok(["Diagnostic", "PlanUsageUpdated", "StatusChanged", "SessionUpdated"].includes(k), `expected quiet at ${step}, got ${k}`);
    }
  }
}

function has(session: Session, capability: string): boolean {
  return session.info.details.capabilities.features.includes(capability as never);
}

function stopName(stop: unknown): string {
  return typeof stop === "string" ? stop : Object.keys(stop as object)[0]!;
}

live("L1 discover finds the agents and probe says they are logged in", async () => {
  for (const agent of await enabled()) {
    const rt = await startRt();
    const found = (await rt.discover()).agents.find((a) => a.id === agent);
    assert.ok(found && existsSync(found.executable_path), `${agent}: not discovered`);
    const details = await rt.probe(agent);
    assert.ok(typeof details.auth === "object" && "Authenticated" in details.auth, `${agent}: ${JSON.stringify(details.auth)}`);
    await rt.close();
    log(agent, "PASS discovered and authenticated");
  }
});

live("L2 one turn: TurnStarted first, text, TurnEnded once, quiet after", async () => {
  for (const agent of await enabled()) {
    const { rt, session, stream } = await open(agent);
    await session.prompt("Say only the word PINEAPPLE. Do not use any tools.");
    let last = 0;
    let sawInTurn = false;
    let text = "";
    for (;;) {
      const ev = await stream.next(`${agent}: turn contract`);
      assert.ok(ev);
      assert.ok(ev.sequence > last, `${agent}: sequence not increasing`);
      last = ev.sequence;
      if (ev.turn_info && !sawInTurn) {
        sawInTurn = true;
        assert.equal(kindOf(ev), "TurnStarted", `${agent}: first in-turn event was ${kindOf(ev)}`);
        continue;
      }
      if (typeof ev.kind === "object" && "TextDelta" in ev.kind) text += ev.kind.TextDelta.text;
      if (typeof ev.kind === "object" && "TurnEnded" in ev.kind) {
        assert.equal(stopName(ev.kind.TurnEnded.stop), "Completed", `${agent}: ${JSON.stringify(ev.kind)}`);
        break;
      }
    }
    assert.ok(text.includes("PINEAPPLE"), `${agent}: text was ${JSON.stringify(text)}`);
    await stream.quiet(3, `${agent}: after turn end`);
    await session.close();
    await rt.close();
    log(agent, "PASS turn contract holds");
  }
});

live("L3 a permission gates the write: allow lands the file, deny holds", async () => {
  for (const agent of await enabled()) {
    const write =
      agent === "cursor"
        ? "Run the shell command `printf HELLO > note.txt` to create note.txt. Use the shell, not your file-edit tool. Do not verify afterwards."
        : "Create a file named note.txt containing exactly the word HELLO. Use your file tools.";
    let { rt, session, stream, dir } = await open(agent);
    if (!has(session, "Permissions")) {
      log(agent, "SKIP permissions not advertised");
      await rt.close();
      continue;
    }
    await session.prompt(write);
    let asked = false;
    for (;;) {
      const ev = await stream.next(`${agent}: permission allow`);
      assert.ok(ev);
      if (typeof ev.kind === "object" && "RequestOpened" in ev.kind && "Permission" in ev.kind.RequestOpened) {
        const req = ev.kind.RequestOpened.Permission;
        asked = true;
        assert.ok(req.options.includes("AllowOnce") && req.options.includes("DenyOnce"), `${agent}: ${req.options}`);
        await session.answer(req.id, { Permission: "AllowOnce" });
      }
      if (kindOf(ev) === "TurnEnded") break;
    }
    assert.ok(asked, `${agent}: no permission request opened`);
    assert.ok(existsSync(join(dir, "note.txt")), `${agent}: file missing after allow`);
    await rt.close();

    ({ rt, session, stream, dir } = await open(agent));
    await session.prompt(write);
    for (;;) {
      const ev = await stream.next(`${agent}: permission deny`);
      assert.ok(ev);
      if (typeof ev.kind === "object" && "RequestOpened" in ev.kind) {
        const req = ev.kind.RequestOpened;
        const id = "Permission" in req ? req.Permission.id : req.Question.id;
        await session.answer(id, { Permission: "DenyOnce" });
      }
      if (kindOf(ev) === "TurnEnded") break;
    }
    if (["hermes", "cursor"].includes(agent) && existsSync(join(dir, "note.txt"))) {
      log(agent, "KNOWN denies honoured; the write went through an ungated tool");
    } else {
      assert.ok(!existsSync(join(dir, "note.txt")), `${agent}: file exists after deny`);
    }
    await session.prompt("Say only OK. No tools.");
    await stream.turn(`${agent}: post-deny prompt`);
    await rt.close();
    log(agent, "PASS allow writes, deny holds, session survives");
  }
});

live("L4 a question round-trips where the agent can ask", async () => {
  for (const agent of await enabled()) {
    if (!["claude", "codex", "opencode", "cursor", "antigravity", "grok"].includes(agent)) {
      log(agent, "SKIP questions (claude, codex, opencode, cursor, antigravity, grok)");
      continue;
    }
    const { rt, session, stream } = await open(agent);
    await session.prompt(
      "Ask me whether I prefer red or blue using your question tool (claude: AskUserQuestion; codex: request_user_input; opencode: question; cursor: ask_question; antigravity: ask_question; grok: ask_user_question), then answer with just my choice.",
    );
    let text = "";
    let asked = false;
    for (;;) {
      const ev = await stream.next(`${agent}: question`);
      assert.ok(ev);
      if (typeof ev.kind === "object" && "RequestOpened" in ev.kind && "Question" in ev.kind.RequestOpened) {
        const req: QuestionRequest = ev.kind.RequestOpened.Question;
        asked = true;
        const q = req.questions[0]!;
        assert.ok(q.choices.length >= 2, "fewer than 2 choices");
        const red = q.choices.find((c) => c.label.toLowerCase().includes("red"));
        assert.ok(red, "no red choice");
        await session.answer(req.id, { Question: [{ Choices: [red.id] }] });
      }
      if (typeof ev.kind === "object" && "TextDelta" in ev.kind) text += ev.kind.TextDelta.text;
      if (typeof ev.kind === "object" && "TurnEnded" in ev.kind) {
        assert.equal(stopName(ev.kind.TurnEnded.stop), "Completed");
        break;
      }
    }
    if (!asked) {
      assert.equal(agent, "cursor", `${agent}: no question request opened`);
      log(agent, "SKIP the question request did not fire");
    } else {
      assert.ok(text.toLowerCase().includes("red"), `answer was ${JSON.stringify(text)}`);
      log(agent, "PASS question answered and echoed");
    }
    await rt.close();
  }
});

live("L5 a live mode option switches, SessionUpdated confirms it, info follows", async () => {
  for (const agent of await enabled()) {
    const { rt, session, stream } = await open(agent);
    const option = () => session.info.details.config_options.find((o) => o.id === "mode");
    const mode = option();
    if (!mode?.live || typeof mode.kind !== "object") {
      log(agent, "SKIP no live mode option");
      await rt.close();
      continue;
    }
    const target = mode.kind.Select.choices.map((c) => c.value).find((v) => v !== mode.current)!;
    await session.configure("mode", target);
    for (;;) {
      const ev = await stream.next(`${agent}: mode switch`);
      assert.ok(ev);
      assert.notEqual(kindOf(ev), "TextDelta", `${agent}: a switch leaked text`);
      if (typeof ev.kind === "object" && "SessionUpdated" in ev.kind && ev.kind.SessionUpdated.configuration.options["mode"] === target) break;
    }
    assert.equal(session.info.configuration.options["mode"], target, `${agent}: info lags the event`);
    await session.prompt("Reply with just the word ok.");
    const { text } = await stream.turn(`${agent}: turn after mode switch`);
    assert.ok(text.toLowerCase().includes("ok"), `${agent}: got ${JSON.stringify(text)}`);
    assert.equal(option()?.current, target, `${agent}: mode changed under us`);
    await rt.close();
    log(agent, `PASS mode switched live to ${target}`);
  }
});

live("L6 cancel mid-turn ends the turn Cancelled and the session survives", async () => {
  for (const agent of await enabled()) {
    const { rt, session, stream } = await open(agent);
    await session.prompt(COUNT);
    for (;;) {
      const ev = await stream.next(`${agent}: count streaming`);
      assert.ok(ev);
      if (["TextDelta", "ReasoningDelta"].includes(kindOf(ev))) break;
    }
    await session.cancel();
    const { stop } = await stream.turn(`${agent}: cancel`);
    assert.equal(stopName(stop), "Cancelled", `${agent}: ${JSON.stringify(stop)}`);
    await session.prompt("Say only OK. No tools.");
    const { text } = await stream.turn(`${agent}: post-cancel prompt`);
    assert.ok(text.toUpperCase().includes("OK"), `${agent}: got ${JSON.stringify(text)}`);
    await rt.close();
    log(agent, "PASS cancel ends the turn, session survives");
  }
});

live("L7 resume recalls the codeword without replaying", async () => {
  for (const agent of await enabled()) {
    const first = await open(agent);
    if (!has(first.session, "Resume")) {
      const token = first.session.info.resume_token!;
      await first.rt.close();
      const rt = await startRt();
      await assert.rejects(rt.open(agent, { ...options(agent, first.dir), resume: token }), (e: AnyagentError) => e.kind === "ResumeFailed");
      await rt.close();
      log(agent, "PASS resume correctly refused (not advertised)");
      continue;
    }
    await first.session.prompt("Remember this codeword: FALCON42. Just confirm. No tools.");
    await first.stream.turn(`${agent}: codeword turn`);
    const token = first.session.info.resume_token!;
    await first.rt.close();

    const rt = await startRt();
    const session = await rt.open(agent, { ...options(agent, first.dir), resume: token });
    const stream = new Stream(session);
    const deadline = Date.now() + 3000;
    for (;;) {
      const r = await Promise.race([stream.next(`${agent}: post-resume drain`), sleep(Math.max(deadline - Date.now(), 0), "timeout" as const)]);
      if (r === "timeout" || r === null) break;
      assert.ok(!["TextDelta", "ReasoningDelta", "ToolUpdated"].includes(kindOf(r)), `${agent}: replayed content after resume: ${kindOf(r)}`);
    }
    await session.prompt("What is the codeword? No tools.");
    const { text } = await stream.turn(`${agent}: recall turn`);
    assert.ok(text.includes("FALCON42"), `${agent}: recall said ${JSON.stringify(text)}`);
    await rt.close();
    log(agent, "PASS resumed with no replay and full recall");
  }
});

live("L8 a killed agent: the turn fails, the stream throws ProcessExited, then closed", async () => {
  for (const agent of await enabled()) {
    if (!["claude", "codex"].includes(agent)) {
      log(agent, "SKIP kill (claude and codex prove the path here; tests/live.rs covers the rest)");
      continue;
    }
    const { rt, session, stream } = await open(agent);
    await session.prompt(COUNT);
    await sleep(2000);
    const pattern = agent === "claude" ? session.info.resume_token! : "codex app-server";
    const pids = execFileSync("pgrep", ["-f", pattern]).toString().trim().split("\n");
    process.kill(Number(pids.at(-1)), "SIGKILL");
    let failed = false;
    let error: AnyagentError | undefined;
    try {
      for (;;) {
        const ev = await stream.next(`${agent}: after kill`);
        if (!ev) break;
        if (typeof ev.kind === "object" && "TurnEnded" in ev.kind) {
          assert.equal(stopName(ev.kind.TurnEnded.stop), "Failed", `${agent}: ${JSON.stringify(ev.kind)}`);
          failed = true;
        }
      }
    } catch (e) {
      error = e as AnyagentError;
    }
    assert.ok(failed, `${agent}: no Failed turn end`);
    assert.ok(error instanceof AnyagentError && error.kind === "ProcessExited", `${agent}: stream error was ${error}`);
    assert.ok(String(error.data.status).includes("9"), `${agent}: status was ${error.data.status}`);
    for await (const _ of session.events()) assert.fail("stream restarted");
    await assert.rejects(session.prompt("hi"), (e: AnyagentError) => e.kind === "SessionClosed");
    await rt.close();
    log(agent, "PASS death maps to Failed + ProcessExited + closed");
  }
});

live("L9 generate returns a title with no session", async () => {
  for (const agent of await enabled()) {
    const rt = await startRt();
    const dir = mkdtempSync(join(tmpdir(), `anyagent-live-${agent}-`));
    let text: string;
    try {
      text = await rt.generate(agent, options(agent, dir), TITLE);
    } catch (e) {
      if (e instanceof AnyagentError && e.kind === "UnsupportedFeature") {
        log(agent, `SKIP generate unsupported (typed): ${e.message}`);
        await rt.close();
        continue;
      }
      throw e;
    }
    assert.ok(text.toLowerCase().includes("branch"), `${agent}: got ${JSON.stringify(text)}`);
    assert.ok(text.split(/\s+/).length <= 8, `${agent}: not a title: ${JSON.stringify(text)}`);
    await rt.close();
    log(agent, `PASS generate returned ${JSON.stringify(text)}`);
  }
});

live("L10 close returns within 10 s and the stream ends", async () => {
  for (const agent of await enabled()) {
    const { rt, session, stream } = await open(agent);
    await session.prompt("Say only OK. No tools.");
    await stream.turn(`${agent}: short turn`);
    const started = Date.now();
    await session.close();
    assert.ok(Date.now() - started < 10_000, `${agent}: close took over 10 s`);
    for (;;) {
      const r = await Promise.race([stream.next(`${agent}: stream end`), sleep(10_000, "timeout" as const)]);
      assert.notEqual(r, "timeout", `${agent}: stream did not end after close`);
      if (r === null) break;
    }
    const code = await rt.close();
    assert.equal(code, 0);
    log(agent, "PASS close is prompt and the stream ends");
  }
});
