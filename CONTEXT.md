# anyagent

One Rust crate that finds coding agents installed on a machine, connects to
them over ACP or their native wire, and gives an application one typed
interface. This glossary is the shared language for the plan, the code, and
the tickets.

## Language

### Agents and connections

**Agent**:
A coding-agent CLI installed on the user's machine (Claude Code, Codex, Hermes, Grok, opencode, …).
_Avoid_: harness, provider, executor

**Profile**:
The data entry describing one supported agent: how to find it, how to launch it in protocol mode, and its known quirks.
_Avoid_: spec, descriptor, driver config

**Adapter**:
The private code that speaks one protocol (ACP, native Claude, native Codex) and translates it to the driver vocabulary.
_Avoid_: driver, harness, executor

**Login marker**:
A file or keychain item whose presence means an agent is probably logged in, readable without a network call.
_Avoid_: credential, token (anyagent never reads those)

**Capability**:
One feature an agent can do on this session (steer, images, fork, plan usage, …), reported after `open`, not assumed per agent.

### Sessions and turns

**Session**:
One live conversation with one agent process. Resumable through a resume token.
_Avoid_: thread, conversation, chat, run

**Resume token**:
Opaque agent-owned data an application stores to reopen the same session later.
_Avoid_: session id (that names the live handle)

**Fork**:
Opening a new session that starts from an existing session's history, leaving the original untouched.

**Turn**:
One stretch of agent work, started by a prompt or by the agent itself, ending exactly once.
_Avoid_: run, request, response

**Prompt**:
One piece of user input submitted to a session; it starts a turn, steers one, or waits in the queue.
_Avoid_: message, send, command

**Steer**:
Delivering a prompt into a turn that is already running so the agent changes course mid-turn.
_Avoid_: interject, follow-up, inject

**Queue**:
Prompts accepted while a turn was running that will each start a turn at the next boundary, in order.

**Agent-originated turn**:
A turn the agent started without a new prompt, typically when a background task or subagent finished and woke it.
_Avoid_: synthetic turn, self-continued turn, wake

**Background work**:
Tools still running when a turn ends (subagents, backgrounded shells). Their completion usually produces an agent-originated turn.

### Turn end

**Protocol completion**:
A turn end the agent's wire stated explicitly.

**Inferred completion**:
A turn end anyagent declared because the wire went quiet for the quiet window. Shown as "idle", never as the same checkmark as protocol completion.
_Avoid_: timeout, stall

**Quiet window**:
How long a non-deterministic wire may stay silent before anyagent infers completion. Never runs while a tool is running or a request is open.
_Avoid_: watchdog, stall timeout

**Trailing bookkeeping**:
Events arriving after a turn ended that carry no new work (late tool status, usage, a late stop frame). Applied without a turn or dropped.

### Requests and configuration

**Request**:
Something the agent needs the user to decide before it continues: a permission or a question. Answered once.
_Avoid_: approval, elicitation, input request, can_use_tool

**Permission**:
A request to allow or deny a tool action, possibly "always".

**Question**:
A request for structured user input (choices or free text).
_Avoid_: AskUserQuestion, requestUserInput, elicitation

**Config option**:
A setting the agent advertises for a session (model, effort, mode, sandbox, …), with allowed values.
_Avoid_: mode (that is one specific option), setting, preference

### Usage and accounts

**Context usage**:
How full the session's context window is (tokens used of tokens available).
_Avoid_: token usage, usage

**Plan usage**:
How much of the account's subscription quota is used per window (5-hour, weekly).
_Avoid_: rate limits, quota, usage

**Auth status**:
Whether the agent is logged in, how (subscription, API key, cloud provider), and what login methods exist.

**Login**:
Driving the agent's own sign-in flow and relaying what the user must do. Anyagent never holds the credential.

**Config home**:
A separate configuration directory for an agent, used to keep several logins of the same agent apart.
_Avoid_: profile (that means the agent entry), account slot
