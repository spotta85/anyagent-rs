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
import { Runtime, kindOf } from "anyagent";

const rt = await Runtime.start();
const session = await rt.open("claude", { dir: process.cwd() });

await session.prompt("explain this repo");
for await (const ev of session.events()) {
  switch (kindOf(ev)) {
    case "TextDelta":
      process.stdout.write(ev.kind.TextDelta.text);
      break;
    case "RequestOpened": {
      const req = ev.kind.RequestOpened.Permission;
      await session.answer(req.id, { Permission: "AllowOnce" });
      break;
    }
  }
  if (kindOf(ev) === "TurnEnded") break;
}
await session.close();
await rt.close();
```

`session.info` and `session.status` stay current. A session error
(`AuthRequired`, `ProcessExited`) throws from the `for await`; a reader that
falls 4096 events behind gets `ConsumerLagged` and its session is closed.

One-shot text, no session: `await rt.generate("codex", { dir }, "one-line title for this diff")`.

Test your app without agents: build the binary with `--features mock` and
pass `Runtime.start({ bin, mock: "packages/mock-scripts/turn.json" })`.

Docs: https://anyagent.mintlify.site/sidecar · Types: generated from the
binary's JSON schema, so commands and events are one contract.
