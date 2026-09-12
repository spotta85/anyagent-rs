// S1–S10 from ticket 13: the wrapper against `anyagent serve --mock`.
// Needs a mock-enabled binary: `cargo build --features mock` (or ANYAGENT_BIN).

import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { AnyagentError, Runtime, Session, kindOf } from "../src/index.ts";
import type { Event } from "../src/types.ts";

const ROOT = join(import.meta.dirname, "../../../..");
const BIN = process.env.ANYAGENT_BIN ?? join(ROOT, "target/debug/anyagent");
const SCRIPTS = join(ROOT, "packages/mock-scripts");
const dir = mkdtempSync(join(tmpdir(), "anyagent-node-"));

/** A runtime over a mock script, closed when the test ends however it ends. */
async function start(t: TestContext, script: string): Promise<Runtime> {
  const rt = await Runtime.start({ bin: BIN, mock: join(SCRIPTS, `${script}.json`) });
  t.after(() => rt.close());
  return rt;
}

/** Reads the stream to its end. */
async function drain(session: Session): Promise<Event[]> {
  const seen: Event[] = [];
  for await (const ev of session.events()) seen.push(ev);
  return seen;
}

/** Events until `kind` (inclusive); the last one is the match. */
async function until(session: Session, kind: string): Promise<Event[]> {
  const seen: Event[] = [];
  for await (const ev of session.events()) {
    seen.push(ev);
    if (kindOf(ev) === kind) return seen;
  }
  throw new Error(`stream ended before ${kind}; saw ${seen.map(kindOf).join(",")}`);
}

function texts(events: Event[]): string[] {
  return events.flatMap((ev) => (typeof ev.kind === "object" && "TextDelta" in ev.kind ? [ev.kind.TextDelta.text] : []));
}

async function rejects(p: Promise<unknown>, kind: string): Promise<AnyagentError> {
  try {
    await p;
  } catch (e) {
    assert.ok(e instanceof AnyagentError, `${e}`);
    assert.equal(e.kind, kind, e.message);
    return e;
  }
  throw new Error(`resolved, expected ${kind}`);
}

test("S1 open, prompt, answer the permission, see the turn end, close", async (t) => {
  const rt = await start(t, "turn");
  const session = await rt.open("mock", { dir });
  assert.equal(session.info.id, session.id);

  const delivery = await session.prompt("hi");
  assert.ok(typeof delivery.kind === "object" && "Started" in delivery.kind, JSON.stringify(delivery));

  const opened = await until(session, "RequestOpened");
  const last = opened.at(-1)!.kind as { RequestOpened: { Permission: { id: string } } };
  await session.answer(last.RequestOpened.Permission.id, { Permission: "AllowOnce" });

  const rest = await until(session, "TurnEnded");
  assert.deepEqual(texts(rest), ["Done."]);
  await until(session, "StatusChanged");
  assert.equal(session.status, "Idle"); // W2: live

  await session.close();
  for await (const _ of session.events()) assert.fail("no events after closed");
  await rt.close();
});

test("S2 events buffered before the app iterates are all delivered", async (t) => {
  const rt = await start(t, "turn");
  const session = await rt.open("mock", { dir });
  await session.prompt("hi");
  await sleep(200);
  const seen = await until(session, "RequestOpened");
  assert.deepEqual(texts(seen), ["Let me check. "]);
  assert.ok(seen.some((ev) => kindOf(ev) === "TurnStarted"));
  await rt.close();
});

test("S3 a prompt after close rejects with SessionClosed", async (t) => {
  const rt = await start(t, "turn");
  const session = await rt.open("mock", { dir });
  await session.close();
  await rejects(session.prompt("x"), "SessionClosed");
  await rt.close();
});

test("S4 two sessions see only their own events, in order", async (t) => {
  const rt = await start(t, "chatter");
  const [a, b] = await Promise.all([rt.open("mock", { dir }), rt.open("mock", { dir })]);
  assert.notEqual(a.id, b.id);
  await Promise.all([a.prompt("x"), b.prompt("y")]);
  const [ea, eb] = await Promise.all([until(a, "TurnEnded"), until(b, "TurnEnded")]);
  for (const [session, events] of [[a, ea], [b, eb]] as const) {
    assert.ok(events.every((ev) => ev.session_id === session.id));
    const seqs = events.map((ev) => ev.sequence);
    assert.deepEqual(seqs, [...seqs].sort((x, y) => x - y));
    assert.deepEqual(texts(events), ["one", "two", "three"]);
  }
  await rt.close();
});

test("S5 a malformed line is answered with BadFrame and the runtime goes on", async (t) => {
  const rt = await start(t, "turn");
  rt["child"].stdin!.write("not json\n");
  const report = await rt.discover();
  assert.equal(report.agents[0]!.id, "mock");
  await rt.close();
});

test("S6 process death rejects pending calls, fails iterators, and later calls", async (t) => {
  const rt = await start(t, "turn");
  const session = await rt.open("mock", { dir });
  // Killed first: nothing written from here on can be answered.
  rt["child"].kill("SIGKILL");
  const pending = [rt.discover(), rt.open("mock", { dir })];
  for (const p of pending) await rejects(p, "ProcessExited");
  await rejects(drain(session), "ProcessExited");
  await rejects(rt.discover(), "ProcessExited");
  await rt.close();
});

test("S7 the agent dying mid-turn: the turn fails, the stream throws ProcessExited, then ends", async (t) => {
  const rt = await start(t, "die");
  const session = await rt.open("mock", { dir });
  await session.prompt("go");
  const ended = (await until(session, "TurnEnded")).at(-1)!.kind as { TurnEnded: { stop: unknown } };
  assert.ok(typeof ended.TurnEnded.stop === "object" && "Failed" in ended.TurnEnded.stop!, JSON.stringify(ended));
  const e = await rejects(drain(session), "ProcessExited");
  assert.equal(e.data.status, "9");
  for await (const _ of session.events()) assert.fail("stream restarted");
  await rt.close();
});

test("S8a a 20 000-event flood arrives whole and in order", async (t) => {
  const rt = await start(t, "flood");
  const session = await rt.open("mock", { dir });
  await session.prompt("go");
  let deltas = 0;
  let last = 0;
  for await (const ev of session.events()) {
    assert.ok(ev.sequence > last);
    last = ev.sequence;
    if (kindOf(ev) === "TextDelta") deltas++;
    if (kindOf(ev) === "TurnEnded") break;
  }
  assert.equal(deltas, 20_000);
  await rt.close();
});

test("S8b a consumer that stops reading gets ConsumerLagged, and the runtime survives", async (t) => {
  const rt = await start(t, "flood");
  const session = await rt.open("mock", { dir });
  await session.prompt("go");
  let n = 0;
  for await (const _ of session.events()) if (++n === 10) break;
  await sleep(2500); // 4096 events arrive in ~0.8 s at the flood's pace
  await rejects(until(session, "TurnEnded"), "ConsumerLagged");
  assert.equal((await rt.discover()).agents[0]!.id, "mock");
  await rt.close();
});

test("S9 close with a session open: closed arrives, the process exits 0 and is gone", async (t) => {
  const rt = await start(t, "turn");
  const session = await rt.open("mock", { dir });
  const pid = rt["child"].pid!;
  const code = await rt.close();
  assert.equal(code, 0);
  for await (const _ of session.events()) assert.fail("closed should end the stream");
  assert.throws(() => process.kill(pid, 0), /ESRCH/);
});

test("S10 configure sends `option` and SessionUpdated updates info", async (t) => {
  const rt = await start(t, "configure");
  const session = await rt.open("mock", { dir });
  await session.configure("model", "opus");
  await until(session, "SessionUpdated");
  assert.equal(session.info.configuration.options["model"], "opus");
  await rt.close();
});
