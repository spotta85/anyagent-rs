// Claude stream-json fixture agent, shaped like the recordings in this
// directory (claude 2.1.241). Flags: --question (AskUserQuestion turn),
// --eof (die mid-turn), --wake (background task wakes an agent-originated
// turn), --subagent (nested transcript with parent_tool_use_id).
import { createInterface } from 'node:readline';

const flag = (name) => process.argv.includes(name);
const send = (m) => process.stdout.write(JSON.stringify(m) + '\n');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const S = 'sess-c1';
let n = 0;
const uid = () => `f${n++}`;
const USAGE = { input_tokens: 2, cache_creation_input_tokens: 198, cache_read_input_tokens: 1000, output_tokens: 0 };

const ev = (event, parent = null) => send({ type: 'stream_event', event, session_id: S, parent_tool_use_id: parent, uuid: uid() });
const delta = (d, parent = null) => ev({ type: 'content_block_delta', index: 0, delta: d }, parent);
const msgStart = (id, parent = null) => ev({ type: 'message_start', message: { id, model: 'claude-sonnet-5', role: 'assistant', content: [], usage: USAGE } }, parent);
const life = (cu, state) => send({ type: 'command_lifecycle', command_uuid: cu, state, uuid: uid(), session_id: S });
const assistantTool = (id, name, input) => send({ type: 'assistant', message: { id: 'msg_1', model: 'claude-sonnet-5', role: 'assistant', content: [{ type: 'tool_use', id, name, input }], usage: USAGE }, session_id: S, uuid: uid(), parent_tool_use_id: null });
const resultFrame = (extra) => send({ type: 'result', session_id: S, uuid: uid(), subtype: 'success', is_error: false, stop_reason: 'end_turn', terminal_reason: 'completed', num_turns: 1, total_cost_usd: 0.01, usage: {}, modelUsage: { 'claude-sonnet-5': { contextWindow: 200000 } }, result: 'done', ...extra });

let ctrlWaiters = {}, turn = null, inited = false, reqN = 0, queue = [];

const rl = createInterface({ input: process.stdin });
rl.on('line', (line) => {
  const m = JSON.parse(line);
  if (m.type === 'control_request') return onControl(m);
  if (m.type === 'control_response') {
    const r = ctrlWaiters[m.response.request_id];
    delete ctrlWaiters[m.response.request_id];
    return r?.(m.response);
  }
  if (m.type === 'user') return onUser(m);
});
rl.on('close', () => process.exit(0));

// Sends a can_use_tool request and awaits the client's control response.
function ask(request) {
  const id = `q${reqN++}`;
  return new Promise((r) => { ctrlWaiters[id] = r; send({ type: 'control_request', request_id: id, request }); });
}

function onControl(m) {
  const reply = (response) => send({ type: 'control_response', response: { subtype: 'success', request_id: m.request_id, response } });
  switch (m.request.subtype) {
    case 'initialize': {
      // `--permission-mode` at launch decides the starting mode, like the CLI.
      const pm = process.argv.indexOf('--permission-mode');
      return reply({
        commands: [{ name: 'compact', description: 'Compact context', argumentHint: '' }],
        models: [
          { value: 'default', displayName: 'Default (recommended)', description: 'Opus 5 with 1M context', supportedEffortLevels: ['low', 'medium', 'high', 'xhigh', 'max'] },
          { value: 'sonnet', displayName: 'Sonnet', description: 'Fast for everyday tasks', supportedEffortLevels: ['low', 'high'] },
        ],
        account: { email: 'user@example.com', organization: 'Example Org', subscriptionType: 'Claude Max', apiProvider: 'firstParty' },
        current_permission_mode: pm > -1 ? process.argv[pm + 1] : 'default',
      });
    }
    case 'set_permission_mode':
      return reply({ mode: m.request.mode });
    case 'set_model':
      return reply({});
    case 'get_binary_version':
      return reply({ version: '2.1.241', buildTime: '2026-08-22T22:46:48Z' });
    case 'interrupt': {
      const cancelled = [];
      if (turn) turn.interrupted = true;
      if (m.request.cancel_queued) { for (const q of queue) { cancelled.push(q.uuid); life(q.uuid, 'cancelled'); } queue = []; }
      return reply({ still_queued: [], cancelled });
    }
    default:
      return send({ type: 'control_response', response: { subtype: 'error', request_id: m.request_id, error: `Unsupported control request subtype: ${m.request.subtype}` } });
  }
}

function onUser(m) {
  // Like the real CLI: a user message mid-turn is queued and runs as its
  // own turn after the current one completes.
  if (turn) { life(m.uuid, 'queued'); queue.push(m); return; }
  // "slow-start" models the CLI's pre-start window (probed 2026-08-24): the
  // message parks as queued, and an interrupt with cancel_queued removes it —
  // lifecycle "cancelled" plus the receipt naming its uuid, never a result.
  const text = typeof m.message.content === 'string' ? m.message.content : (m.message.content.find(b => b.type === 'text')?.text ?? '');
  if (text.includes('slow-start')) { life(m.uuid, 'queued'); queue.push(m); return; }
  runTurn(m).catch(() => process.exit(1));
}

async function runTurn(m) {
  turn = { interrupted: false };
  const u = m.uuid;
  life(u, 'queued');
  life(u, 'started');
  if (!inited) {
    inited = true;
    send({ type: 'system', subtype: 'init', cwd: process.cwd(), session_id: S, model: 'claude-sonnet-5', permissionMode: 'default', tools: ['Write', 'Task'], apiKeySource: 'none', claude_code_version: '2.1.241', uuid: uid() });
  }
  // "die-auth" loses the credentials: the synthetic API-error message and
  // its result frame, exactly as the CLI emits them with no stored login.
  const prompt = typeof m.message.content === 'string' ? m.message.content : (m.message.content.find(b => b.type === 'text')?.text ?? '');
  if (prompt.includes('die-auth')) {
    send({ type: 'assistant', message: { id: 'err_1', model: '<synthetic>', role: 'assistant', content: [{ type: 'text', text: 'Not logged in · Please run /login' }], usage: USAGE }, session_id: S, uuid: uid(), parent_tool_use_id: null, error: 'authentication_failed', is_api_error_message: true });
    send({ type: 'result', session_id: S, uuid: uid(), subtype: 'success', is_error: true, stop_reason: 'stop_sequence', terminal_reason: 'api_error', num_turns: 1, total_cost_usd: 0, usage: {}, modelUsage: {}, result: 'Not logged in · Please run /login', user_message_uuid: u });
    turn = null;
    return;
  }
  const aborted = () => {
    send({ type: 'result', session_id: S, uuid: uid(), user_message_uuid: null, subtype: 'error_during_execution', is_error: true, stop_reason: 'tool_use', terminal_reason: 'aborted_streaming', num_turns: 1, result: null, total_cost_usd: 0.005, usage: {}, modelUsage: { 'claude-sonnet-5': { contextWindow: 200000 } } });
    life(u, 'cancelled');
    turn = null;
  };

  if (flag('--question')) {
    msgStart('msg_1');
    const resp = await ask({ subtype: 'can_use_tool', tool_name: 'AskUserQuestion', display_name: 'AskUserQuestion', input: { questions: [{ question: 'Which color do you prefer?', header: 'Color', options: [{ label: 'Red', description: 'Prefer red' }, { label: 'Blue', description: 'Prefer blue' }], multiSelect: false }] }, tool_use_id: 'toolu_q', requires_user_interaction: true });
    if (turn.interrupted) return aborted();
    const answer = resp.response?.updatedInput?.answers?.['Which color do you prefer?'] ?? 'none';
    delta({ type: 'text_delta', text: `answer=${answer}` });
    ev({ type: 'message_stop' });
    resultFrame({ user_message_uuid: u });
    life(u, 'completed');
    turn = null;
    return;
  }

  if (flag('--wake')) {
    msgStart('msg_1');
    delta({ type: 'text_delta', text: 'started' });
    // A backgrounded Bash: its tool_result closes the wire call while the
    // task keeps running past the end of the turn.
    assistantTool('toolu_bg', 'Bash', { command: 'sleep 1 && echo BG', run_in_background: true, description: 'Sleep then print' });
    send({ type: 'user', message: { role: 'user', content: [{ tool_use_id: 'toolu_bg', type: 'tool_result', content: '' }] }, session_id: S, uuid: uid(), parent_tool_use_id: null, tool_use_result: { stdout: '', stderr: '', backgroundTaskId: 'bg1' } });
    ev({ type: 'message_stop' });
    resultFrame({ user_message_uuid: u });
    life(u, 'completed');
    turn = null;
    await sleep(150);
    // The background task finishes and wakes the agent with no user frame.
    send({ type: 'system', subtype: 'task_notification', task_id: 'bg1', tool_use_id: 'toolu_bg', status: 'completed', uuid: uid(), session_id: S });
    turn = { interrupted: false };
    msgStart('msg_w');
    delta({ type: 'text_delta', text: 'BG-DONE' });
    ev({ type: 'message_stop' });
    resultFrame({ user_message_uuid: null, result: 'BG-DONE' });
    turn = null;
    return;
  }

  if (flag('--subagent')) {
    msgStart('msg_1');
    delta({ type: 'text_delta', text: 'main ' });
    assistantTool('toolu_task', 'Task', { description: 'scan files', subagent_type: 'Explore' });
    msgStart('msg_s', 'toolu_task');
    send({ type: 'user', message: { role: 'user', content: 'look deeper' }, session_id: S, uuid: uid(), parent_tool_use_id: 'toolu_task' });
    delta({ type: 'text_delta', text: 'sub ' }, 'toolu_task');
    ev({ type: 'message_stop' }, 'toolu_task');
    send({ type: 'user', message: { role: 'user', content: [{ tool_use_id: 'toolu_task', type: 'tool_result', content: '4 files' }] }, session_id: S, uuid: uid(), parent_tool_use_id: null, tool_use_result: { status: 'completed' } });
    delta({ type: 'text_delta', text: 'done' });
    ev({ type: 'message_stop' });
    resultFrame({ user_message_uuid: u });
    life(u, 'completed');
    turn = null;
    return;
  }

  // Default turn: thinking, text, a Write needing permission, a todo list.
  msgStart('msg_1');
  delta({ type: 'thinking_delta', thinking: 'thinking…' });
  delta({ type: 'text_delta', text: 'Hello ' });
  // Echo --mcp-config so tests can assert the launch shape.
  const mi = process.argv.indexOf('--mcp-config');
  if (mi > -1) {
    const conf = JSON.parse(process.argv[mi + 1]).mcpServers;
    const decl = Object.entries(conf).map(([n, e]) => `${e.type ?? 'stdio'}:${n}`).join(',');
    delta({ type: 'text_delta', text: `mcp=${decl} ` });
  }
  // Echo attachments so tests can assert the wire shape.
  const c = m.message.content;
  if (Array.isArray(c)) {
    const imgs = c.filter(b => b.type === 'image' && b.source?.type === 'base64').length;
    const text = c.find(b => b.type === 'text')?.text ?? '';
    delta({ type: 'text_delta', text: `att=${imgs} ref=${text.includes('Attached files:') ? 1 : 0} ` });
  }
  assistantTool('toolu_w1', 'Write', { file_path: 'a.txt', content: 'ALPHA' });
  const resp = await ask({ subtype: 'can_use_tool', tool_name: 'Write', display_name: 'Write', input: { file_path: 'a.txt', content: 'ALPHA' }, description: 'a.txt', permission_suggestions: [{ type: 'setMode', mode: 'acceptEdits', destination: 'session' }], tool_use_id: 'toolu_w1' });
  if (turn.interrupted) return aborted();
  const behavior = resp.response?.behavior ?? 'deny';
  if (behavior === 'allow') {
    send({ type: 'user', message: { role: 'user', content: [{ tool_use_id: 'toolu_w1', type: 'tool_result', content: 'File created successfully' }] }, session_id: S, uuid: uid(), parent_tool_use_id: null, tool_use_result: { type: 'create', filePath: 'a.txt', content: 'ALPHA', originalFile: null, structuredPatch: [] } });
  }
  delta({ type: 'text_delta', text: `perm=${behavior} ` });
  if (flag('--eof')) { process.stderr.write('boom: fixture died\n'); process.exit(3); }
  assistantTool('toolu_todo', 'TodoWrite', { todos: [{ content: 'step 1', status: 'in_progress' }] });
  delta({ type: 'text_delta', text: 'done' });
  ev({ type: 'message_stop' });
  send({ type: 'rate_limit_event', rate_limit_info: { status: 'allowed', resetsAt: 1, rateLimitType: 'five_hour' }, uuid: uid(), session_id: S });
  if (turn.interrupted) return aborted();
  resultFrame({ user_message_uuid: u });
  life(u, 'completed');
  turn = null;
  if (queue.length) runTurn(queue.shift()).catch(() => process.exit(1));
}
