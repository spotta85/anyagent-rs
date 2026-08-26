# Claude Code native wire recordings

Recorded 2026-08-23 against `claude` 2.1.241 (ticket 04). One JSON object per
line, in wire order. Lines with `_sent` are what we wrote to stdin; every other
line is a frame the CLI wrote to stdout. `_t` is seconds since spawn.
Email, org id, and local paths are redacted; personal MCP/plugin lists trimmed.

All runs used:

```
claude -p --output-format stream-json --input-format stream-json --verbose
       --include-partial-messages --permission-prompt-tool stdio
       --replay-user-messages --model sonnet
```

| File | Shows |
|---|---|
| 01-single-prompt | `system/init`, `stream_event` deltas, `assistant`, `rate_limit_event`, `result` |
| 02-permission-and-steer | `can_use_tool` request + answer; a user message written mid-turn folds into the running turn; `command_lifecycle` |
| 03-interrupt-and-queue | `interrupt` with and without `cancel_queued`; `priority: later` queueing; interrupted `result`; tool moved to background |
| 04-background-task-wakes-agent | `task_started` / `task_notification` / `background_tasks_changed`; a turn that starts with no user input |
| 05-subagent | `parent_tool_use_id` + `subagent_type` on nested frames; Task tool `tool_use_result`; Write `structuredPatch` |
| 06-control-requests-and-compact | `initialize`, `get_context_usage`, `get_usage`, `list_models`, `mcp_status`, `set_model`, `set_permission_mode`, `get_binary_version`, unknown subtype error; `/compact` over stdin |
| 07-ask-user-question | `AskUserQuestion` arrives as `can_use_tool` with `requires_user_interaction` |
| 08-resume | `--resume <id>`: same session id, no history replay |
| 09-fork-at | `--resume <id> --fork-session --resume-session-at=<uuid>`: new id, resumes AT the uuid: a user-message uuid re-runs that message; cut by naming the last KEPT assistant frame (result uuids hang) |
| 10-rewind-files | `rewind_files` with `CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING=true` |

Authoritative types: `@anthropic-ai/claude-agent-sdk` `sdk.d.ts` (same version
as the CLI). Driver script: `drive.py` in the ticket notes.
