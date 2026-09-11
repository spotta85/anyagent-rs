# anyagent, for agents

Rust crate: one typed API over the coding agents installed on a machine
(Claude Code, Codex, Cursor, opencode, Kiro, Grok, Hermes, Qwen, pi, Antigravity).

Read before touching code that uses or changes anyagent:

| Task | Read |
|---|---|
| Build an app on anyagent | [docs/skill.md](docs/skill.md), then [docs/core-api.mdx](docs/core-api.mdx) |
| Whole docs as text | https://anyagent.mintlify.site/llms-full.txt |
| Change anyagent itself | [docs/architecture.mdx](docs/architecture.mdx), [docs/contributing.mdx](docs/contributing.mdx) |

Rules that matter most are the `rules` list in [context7.json](context7.json).
