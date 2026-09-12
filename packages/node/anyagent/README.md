# anyagent

One API over the coding agents installed on a machine: Claude Code, Codex,
Cursor, opencode, Kiro, Grok, Hermes, Qwen, pi, Antigravity. This package
spawns the `anyagent` binary (delivered by a platform package, no
postinstall) and talks to it over JSON lines. Every rule lives in the
binary; the package is a thin, typed pipe.

```bash
npm install anyagent
```

```ts
import { Runtime, is } from "anyagent";

const rt = await Runtime.start();
const session = await rt.open("claude", { dir: process.cwd() });

await session.prompt("explain this repo");
for await (const ev of session.events()) {
  if (is(ev, "TextDelta")) process.stdout.write(ev.kind.TextDelta.text);
  if (is(ev, "RequestOpened") && "Permission" in ev.kind.RequestOpened) {
    await session.answer(ev.kind.RequestOpened.Permission.id, { Permission: "AllowOnce" });
  }
  if (is(ev, "TurnEnded")) break;
}
await session.close();
await rt.close();
```

`is(ev, "TextDelta")` narrows `ev.kind` to that variant; `kindOf(ev)` gives
the variant name for a `switch` or a log line.

`session.info` and `session.status` stay current. A session error
(`AuthRequired`, `ProcessExited`) throws from the `for await`; a reader that
falls 4096 events behind gets `ConsumerLagged` and its session is closed.

One-shot text, no session: `await rt.generate("codex", { dir }, "one-line title for this diff")`.

Test your app without agents: build the binary with `--features mock` and
pass `Runtime.start({ bin, mock: "packages/mock-scripts/turn.json" })`.

Docs: https://anyagent.mintlify.site/sidecar · Types: generated from the
binary's JSON schema, so commands and events are one contract.
