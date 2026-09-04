// Codex app-server fixture agent, shaped like the recordings in this
// directory (codex 0.147.0): line-delimited JSON-RPC 2.0 both ways.
// Flags: --logged-out (no account; a turn 401s), --api-key (auth.json key
// login), --question (a requestUserInput mid-turn), --echo-config-home
// (echo the CODEX_HOME the child received). Prompt words steer scenarios:
// "write-file" (a fileChange escalates past the sandbox -> approval),
// "sleep" (a command that only an interrupt ends), "die" (exit mid-turn),
// "subagent" (a child thread runs a whole turn before the parent's ends,
// "subagent-fails" for a child turn that fails), "end-failed"/"end-aborted"
// (the turn ends via turn/failed / turn/aborted instead of turn/completed).
import { createInterface } from 'node:readline';

const flag = (name) => process.argv.includes(name);
const send = (m) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', ...m }) + '\n');
const notify = (method, params) => send({ method, params });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const THREAD = { id: 'th-1', name: null };
let turnN = 0, serverReqN = 0, itemN = 0;
let turn = null; // { id, started, interrupted, steered: [] }
const waiters = {}; // server request id -> resolver

const MODELS = [
  { id: 'gpt-6', model: 'gpt-6', displayName: 'GPT-6', description: 'Frontier model.', hidden: false, isDefault: true, defaultReasoningEffort: 'medium', supportedReasoningEfforts: [{ reasoningEffort: 'low', description: 'Fast' }, { reasoningEffort: 'medium', description: 'Balanced' }, { reasoningEffort: 'high', description: 'Deep' }], serviceTiers: [{ id: 'priority', name: 'Fast', description: '1.5x speed' }], defaultServiceTier: 'priority' },
  { id: 'gpt-6-mini', model: 'gpt-6-mini', displayName: 'GPT-6 Mini', description: 'Small model.', hidden: false, isDefault: false, defaultReasoningEffort: 'low', supportedReasoningEfforts: [{ reasoningEffort: 'low', description: 'Fast' }, { reasoningEffort: 'medium', description: 'Balanced' }] },
  { id: 'gpt-secret', model: 'gpt-secret', displayName: 'Secret', description: null, hidden: true, isDefault: false, defaultReasoningEffort: 'low', supportedReasoningEfforts: [] },
];
const RATE_LIMITS = {
  limitId: 'codex', planType: 'edu',
  primary: { usedPercent: 5, windowDurationMins: 300, resetsAt: 1787903985 },
  secondary: { usedPercent: 4, windowDurationMins: 10080, resetsAt: 1788329085 },
};

const item = (fields) => ({ id: `it-${itemN++}`, ...fields });
const itemStarted = (it) => notify('item/started', { item: it, threadId: THREAD.id, turnId: turn.id });
const itemCompleted = (it) => notify('item/completed', { item: it, threadId: THREAD.id, turnId: turn.id });
const delta = (itemId, d) => notify('item/agentMessage/delta', { threadId: THREAD.id, turnId: turn.id, itemId, delta: d });

// Awaits the client's response to one server->client request.
function ask(method, params) {
  const id = serverReqN++;
  return new Promise((r) => { waiters[id] = r; send({ method, id, params: { threadId: THREAD.id, turnId: turn.id, ...params } }); });
}

const rl = createInterface({ input: process.stdin });
rl.on('line', (line) => {
  const m = JSON.parse(line);
  if (m.method === undefined && m.id !== undefined) {
    const r = waiters[m.id];
    delete waiters[m.id];
    return r?.(m.result ?? m.error);
  }
  if (m.id !== undefined) onRequest(m).catch(() => process.exit(1));
});
rl.on('close', () => process.exit(0));

// The thread bind response, echoing creation params like the real server.
function threadResult(params) {
  const sandboxType = { 'read-only': 'readOnly', 'workspace-write': 'workspaceWrite', 'danger-full-access': 'dangerFullAccess' }[params.sandbox] ?? 'readOnly';
  return {
    thread: THREAD,
    model: 'gpt-6', // the config-file default; per-turn model rides turn/start
    reasoningEffort: null,
    approvalPolicy: params.approvalPolicy ?? 'on-request',
    sandbox: { type: sandboxType, networkAccess: false },
  };
}

async function onRequest(m) {
  const reply = (result) => send({ id: m.id, result });
  const refuse = (message) => send({ id: m.id, error: { code: -32600, message } });
  switch (m.method) {
    case 'initialize':
      return reply({ userAgent: 'anyagent/0.147.0 (Mac OS 26.5.1; arm64)', codexHome: process.env.CODEX_HOME ?? '', platformOs: 'macos' });
    case 'account/read':
      return reply(flag('--logged-out')
        ? { account: null, requiresOpenaiAuth: true }
        : flag('--api-key')
        ? { account: { type: 'apiKey' }, requiresOpenaiAuth: true }
        : { account: { type: 'chatgpt', email: 'user@example.com', planType: 'edu' }, requiresOpenaiAuth: true });
    case 'model/list':
      return reply({ data: MODELS, nextCursor: null });
    case 'skills/list':
      // Grouped by root; the same skill appears under every root (dedupe by
      // name), a nameless entry is junk, and only `review` has an interface.
      return reply({ data: [
        { cwd: process.cwd(), skills: [
          { name: 'review', description: 'A long model-facing paragraph.', interface: { shortDescription: 'Review a diff.' }, enabled: true, scope: 'repo', path: '/skills/review' },
          { name: 'release', description: 'Cut a release.', enabled: true, scope: 'user', path: '/skills/release' },
          { name: '', description: 'no name', enabled: true, scope: 'user', path: '/skills/junk' },
        ] },
        { cwd: '/other', skills: [{ name: 'review', description: 'dup', enabled: true, scope: 'user', path: '/skills/review' }] },
      ] });
    case 'account/rateLimits/read':
      if (flag('--logged-out')) return refuse('codex account authentication required to read rate limits');
      return reply({ rateLimits: RATE_LIMITS });
    case 'thread/start':
      return reply(threadResult(m.params));
    case 'thread/resume':
      THREAD.id = m.params.threadId;
      return reply(threadResult(m.params));
    case 'thread/fork':
      THREAD.id = 'th-fork-1';
      THREAD.forkPoint = m.params.lastTurnId ?? null;
      return reply({ ...threadResult(m.params), thread: { ...THREAD, forkedFromId: m.params.threadId } });
    case 'turn/start': {
      if (turn) return refuse('phantom: turn/start while a turn is running'); // adapters must steer instead
      turn = { id: `turn-${turnN++}`, started: false, interrupted: false };
      reply({ turn: { id: turn.id, status: 'inProgress' } });
      runTurn(m.params).catch(() => process.exit(1));
      return;
    }
    case 'turn/steer': {
      // Like the real server: a steer before turn/started is refused.
      if (!turn || m.params.expectedTurnId !== turn.id) return refuse(`expected active turn id \`${m.params.expectedTurnId}\` but found none`);
      if (!turn.started) return refuse('no active turn to steer');
      turn.steered.push(m.params.input[0].text);
      return reply({ turnId: turn.id });
    }
    case 'turn/interrupt': {
      if (!turn) return refuse('no active turn to interrupt');
      turn.interrupted = true;
      reply({});
      return;
    }
    default:
      return refuse(`Invalid request: unknown variant \`${m.method}\``);
  }
}

// One turn: userMessage echo, then the scenario the prompt asks for. The
// in-flight tool item gets no item/completed when interrupted (recording 04).
async function runTurn(params) {
  const prompt = params.input[0].text;
  turn.steered = [];
  await sleep(30); // the real started-window: steers before this are refused
  if (turn.interrupted) return endTurn('interrupted');
  notify('turn/started', { threadId: THREAD.id, turn: { id: turn.id, status: 'inProgress' } });
  turn.started = true;
  const user = item({ type: 'userMessage', clientId: params.clientUserMessageId, content: [{ type: 'text', text: prompt }] });
  itemStarted(user);
  itemCompleted(user);

  if (flag('--logged-out')) {
    // The 401 retries, then the turn still ends deterministically (recording 08).
    const error = { message: 'Reconnecting... 2/5', codexErrorInfo: { responseStreamDisconnected: { httpStatusCode: 401 } } };
    notify('error', { error, willRetry: true, threadId: THREAD.id, turnId: turn.id });
    notify('error', { error, willRetry: true, threadId: THREAD.id, turnId: turn.id });
    return endTurn('failed', { message: 'unexpected status 401 Unauthorized' });
  }
  if (prompt.includes('die')) { process.stderr.write('boom: fixture died\n'); process.exit(3); }
  // Some wires end a turn with these instead of turn/completed.
  if (prompt.includes('end-failed')) return endTurn('failed', { message: 'wire failed' }, 'turn/failed');
  if (prompt.includes('end-aborted')) return endTurn('aborted', null, 'turn/aborted');

  if (prompt.includes('sleep')) {
    const exec = item({ type: 'commandExecution', command: '/bin/zsh -lc "sleep 45"', cwd: process.cwd(), status: 'inProgress', aggregatedOutput: null, exitCode: null });
    itemStarted(exec);
    while (!turn.interrupted) await sleep(10);
    return endTurn('interrupted');
  }

  const reasoning = item({ type: 'reasoning', summary: [], content: [] });
  itemStarted(reasoning);
  notify('item/reasoning/summaryTextDelta', { threadId: THREAD.id, turnId: turn.id, itemId: reasoning.id, delta: 'thinking…' });
  itemCompleted(reasoning);

  const msg = item({ type: 'agentMessage', text: '', phase: 'final_answer' });
  itemStarted(msg);
  delta(msg.id, 'Hello ');
  delta(msg.id, `model=${params.model ?? 'unset'} effort=${params.effort ?? 'unset'} tier=${params.serviceTier ?? 'unset'} summary=${params.summary ?? 'unset'} `);
  if (flag('--echo-config-home')) delta(msg.id, `cfg=${process.env.CODEX_HOME ?? 'unset'} `);
  if (THREAD.forkPoint !== undefined) delta(msg.id, `fork=${THREAD.forkPoint} `);
  if (prompt.includes('Attached files:')) delta(msg.id, 'ref=1 ');

  if (flag('--question')) {
    const resp = await ask('item/tool/requestUserInput', { itemId: 'it-q', questions: [{ id: 'q1', header: 'Color', question: 'Which color?', options: [{ label: 'Red', description: 'Prefer red' }, { label: 'Blue', description: 'Prefer blue' }], isOther: false }] });
    if (turn.interrupted) return endTurn('interrupted');
    delta(msg.id, `answer=${resp?.answers?.q1?.answers?.[0] ?? 'none'} `);
  }

  const exec = item({ type: 'commandExecution', command: '/bin/zsh -lc "echo PEAR"', cwd: process.cwd(), status: 'inProgress', aggregatedOutput: null, exitCode: null });
  itemStarted(exec);
  itemCompleted({ ...exec, status: 'completed', aggregatedOutput: 'PEAR\n', exitCode: 0 });

  if (prompt.includes('write-file')) {
    // The write escalates past the sandbox (recording 02): the approval
    // request names only the item; decline leaves it `declined`.
    const change = item({ type: 'fileChange', changes: [{ path: 'fruit.txt', kind: { type: 'add' }, diff: 'PEAR\n' }], status: 'inProgress' });
    itemStarted(change);
    const resp = await ask('item/fileChange/requestApproval', { itemId: change.id, reason: null, grantRoot: null });
    if (turn.interrupted) return endTurn('interrupted');
    notify('serverRequest/resolved', { threadId: THREAD.id, requestId: serverReqN - 1 });
    const accepted = resp?.decision === 'accept' || resp?.decision === 'acceptForSession';
    itemCompleted({ ...change, status: accepted ? 'completed' : 'declined' });
    delta(msg.id, `write=${resp?.decision} `);
  }

  if (prompt.includes('subagent')) await runSubagent(prompt.includes('subagent-fails'));

  await sleep(20); // yield so a mid-turn steer on stdin gets read, like the real server
  for (const steer of turn.steered) delta(msg.id, `steered=${steer} `);
  notify('turn/plan/updated', { threadId: THREAD.id, turnId: turn.id, plan: [{ step: 'step 1', status: 'inProgress' }] });
  delta(msg.id, 'done');
  itemCompleted({ ...msg, text: 'done' });
  notify('thread/tokenUsage/updated', { threadId: THREAD.id, turnId: turn.id, tokenUsage: { total: { totalTokens: 2400 }, last: { totalTokens: 1200 }, modelContextWindow: 258400 } });
  notify('account/rateLimits/updated', { rateLimits: RATE_LIMITS });
  if (turn.interrupted) return endTurn('interrupted');
  endTurn('completed');
}

function endTurn(status, error = null, method = 'turn/completed') {
  notify(method, { threadId: THREAD.id, turn: { id: turn.id, status, error, items: [] } });
  turn = null;
}

// A spawned subagent: the parent gets a collab tool call plus the child-thread
// item, and the child then runs a whole turn — started, content, usage, and its
// own turn/completed — on its own threadId, all BEFORE the parent's turn ends.
async function runSubagent(fails) {
  const CHILD = 'th-child-1', CHILD_TURN = 'turn-child-1';
  const child = (method, params) => notify(method, { threadId: CHILD, turnId: CHILD_TURN, ...params });
  const collab = item({ type: 'collabAgentToolCall', tool: 'spawnAgent', senderThreadId: THREAD.id, receiverThreadIds: [CHILD], agentsStates: {}, status: 'inProgress', prompt: 'review the diff' });
  itemStarted(collab);
  const activity = item({ type: 'subAgentActivity', agentThreadId: CHILD, agentPath: '.codex/agents/reviewer.md', kind: 'started' });
  itemStarted(activity);

  child('turn/started', { turn: { id: CHILD_TURN, status: 'inProgress' } });
  const said = { id: 'it-child-msg', type: 'agentMessage', text: '', phase: 'final_answer' };
  child('item/started', { item: said, startedAtMs: 0 });
  child('item/agentMessage/delta', { itemId: said.id, delta: 'child text' });
  child('item/completed', { item: { ...said, text: 'child text' } });
  // Would overwrite the parent's context gauge and plan if it were not consumed.
  child('thread/tokenUsage/updated', { tokenUsage: { total: { totalTokens: 77 }, last: { totalTokens: 77 }, modelContextWindow: 1024 } });
  child('turn/plan/updated', { plan: [{ step: 'child step', status: 'inProgress' }] });
  notify('turn/completed', { threadId: CHILD, turn: { id: CHILD_TURN, status: fails ? 'failed' : 'completed', error: fails ? { message: 'child blew up' } : null, items: [] } });

  itemCompleted({ ...collab, status: 'completed', agentsStates: { [CHILD]: { status: fails ? 'errored' : 'completed' } } });
  await sleep(10);
}
