// ACP v1 fixture agent: speaks JSON-RPC over stdio and emits the annoying cases.
// Flags: --eof (die mid-turn), --flood=N (N chunks before the response),
//        --late-ms=N (late noise delay), --auth-required (session/new fails),
//        --commands-on-open (push availableCommands right after session/new),
//        --kiro (the kiro shape: agentInfo name, effort in _kiro.dev/metadata,
//        `/effort <level>` prompts answered with an ack chunk),
//        --cursor (the cursor shape: `about` preflight, `authenticate` before
//        session/new, per-model options in config responses, the cursor/*
//        extension requests, no usage frames), --logged-out (with --cursor:
//        `about` reports no email and `authenticate` hangs in a browser flow).
import { createInterface } from 'node:readline';

const flag = (name) => process.argv.includes(name);
const num = (name, dflt) => +(process.argv.find(a => a.startsWith(name + '='))?.split('=')[1] ?? dflt);
const send = (m) => process.stdout.write(JSON.stringify(m) + '\n');
const notify = (sessionId, update) => send({ jsonrpc: '2.0', method: 'session/update', params: { sessionId, update } });
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
let adopted = null;
let nextId = 100, pending = {}, turn = null, mcpDecl = [], effort = 'high', spurious = false;
// --grok-models: per-model reasoning efforts in `_meta`. As on grok 1.0.4,
// `reasoningEffort` there is a static default; the effort in force is only
// reported by the `model_changed` session notification.
let grokModel = 'grok-4.5', grokEffort = 'high';
// --kiro adds a model without effort levels; --qwen adds its
// `reasoning_effort` (category thought_level), as qwen 0.23.2 names it.
let qwenEffort = 'default', qwenModel = 'sonnet';
// --qwen: opus names its thought level plainly, as a later model list may.
const qwenEffortId = () => qwenModel === 'opus' ? 'effort' : 'reasoning_effort';
const configOptions = () => [
  { id: 'model', name: 'Model', category: 'model', type: 'select', currentValue: qwenModel, options: [{ value: 'sonnet', name: 'Sonnet' }, { value: 'opus', name: 'Opus' }, ...(flag('--kiro') ? [{ value: 'claude-haiku-4.5', name: 'Haiku' }] : [])] },
  ...(flag('--qwen') ? [{ id: qwenEffortId(), name: 'Reasoning effort', category: 'thought_level', type: 'select', currentValue: qwenEffort, options: [{ value: 'default', name: 'Default' }, { value: 'high', name: 'High' }] }] : []),
];
const grokModels = () => ({ currentModelId: grokModel, availableModels: [
  { modelId: 'grok-4.5', name: 'Grok 4.5', description: 'fast', _meta: { reasoningEffort: 'high', reasoningEfforts: [{ value: 'low', label: 'Low Effort' }, { value: 'high', label: 'High Effort', description: 'default' }] } },
  { modelId: 'grok-4.6', name: 'Grok 4.6', _meta: { reasoningEffort: 'high', reasoningEfforts: [{ value: 'low', label: 'Low Effort' }, { value: 'high', label: 'High Effort' }, { value: 'xhigh', label: 'Extra High' }] } },
  { modelId: 'grok-basic', name: 'Grok Basic' },
] });
const kiroMetadata = (sessionId) => send({ jsonrpc: '2.0', method: '_kiro.dev/metadata', params: { sessionId, contextUsagePercentage: 0.5, effort } });
// --antigravity: the Antigravity server's agentInfo name, which turns its
// `interaction_*` permissions into questions.
// --cursor state: the selected model and its own options (shapes recorded
// from cursor-agent 2026.09.02 with parameterizedModelPicker).
let authed = false, cursorModel = 'default', cursorOpts = { fast: 'true', thinking: 'true', context: '300k', effort: 'high' };
const sel = (id, name, category, current, values) => ({ id, name, category, type: 'select', currentValue: current, options: values.map(v => ({ value: v, name: v })) });
const cursorPerModel = () => ({
  'default': [],
  'composer-2.5': [sel('fast', 'Fast', 'model_config', cursorOpts.fast, ['false', 'true'])],
  'claude-opus-5': [sel('thinking', 'Thinking', 'thought_level', cursorOpts.thinking, ['false', 'true']), sel('context', 'Context', 'model_config', cursorOpts.context, ['300k', '1m']), sel('effort', 'Effort', 'thought_level', cursorOpts.effort, ['low', 'medium', 'high', 'xhigh'])],
})[cursorModel];
const cursorModes = () => ({ currentModeId: 'agent', availableModes: [{ id: 'agent', name: 'Agent' }, { id: 'plan', name: 'Plan' }, { id: 'ask', name: 'Ask' }] });
const cursorConfig = () => [
  { id: 'mode', name: 'Mode', category: 'mode', type: 'select', currentValue: 'agent', options: [{ value: 'agent', name: 'Agent' }, { value: 'plan', name: 'Plan' }, { value: 'ask', name: 'Ask' }] },
  { id: 'model', name: 'Model', category: 'model', type: 'select', currentValue: cursorModel, options: [{ value: 'default', name: 'Auto' }, { value: 'composer-2.5', name: 'Composer 2.5' }, { value: 'claude-opus-5', name: 'Claude Opus 5' }] },
  ...cursorPerModel(),
];
const cursorSession = () => ({ sessionId: 'sess-1', modes: cursorModes(), models: { currentModelId: cursorModel, availableModels: [{ modelId: 'default', name: 'Auto' }] }, configOptions: cursorConfig() });

// `cursor-agent about --format json`: the version/login preflight.
if (flag('about')) { console.log(JSON.stringify({ cliVersion: '0.0.1', subscriptionTier: 'Free', userEmail: flag('--logged-out') ? null : 'dev@example.com' })); process.exit(0); }

// --die-not-logged-in: the kiro shape — complain on stderr and exit before
// ever speaking ACP.
if (flag('--die-not-logged-in')) {
  process.stderr.write('error:\nYou are not logged in, please log in with fixture login\n');
  process.exit(1);
}

const rl = createInterface({ input: process.stdin });
rl.on('line', (line) => { const m = JSON.parse(line); if (m.method) onRequest(m); else onResponse(m); });
rl.on('close', () => process.exit(0));

function onResponse(m) { const r = pending[m.id]; delete pending[m.id]; r?.(m); }
function request(method, params) { const id = nextId++; return new Promise(r => { pending[id] = r; send({ jsonrpc: '2.0', id, method, params }); }); }

async function onRequest(m) {
  const reply = (result) => send({ jsonrpc: '2.0', id: m.id, result });
  switch (m.method) {
    case 'initialize': {
      // --meta-auth-methods: the qwen shape — `type`/`args` live in _meta,
      // not the typed fields.
      const authMethods = flag('--no-auth-methods')
        ? []
        : flag('--meta-auth-methods')
        ? [{ id: 'openai', name: 'Use OpenAI API key', _meta: { type: 'terminal', args: ['--auth-type=openai'] } }]
        : [{ id: 'fixture-login', name: 'Log in', type: 'terminal', args: ['auth', 'login'] }];
      // The cursor shape: no agentInfo, no steering, an agent-driven method.
      if (flag('--cursor')) return reply({ protocolVersion: 1, agentCapabilities: { loadSession: true, promptCapabilities: { image: true }, mcpCapabilities: { http: true, sse: true } }, authMethods: [{ id: 'cursor_login', name: 'Cursor Login', description: "Run 'agent login' first if not logged in." }] });
      return reply({ protocolVersion: 1, agentCapabilities: { loadSession: !flag('--no-load'), promptCapabilities: { image: true, audio: flag('--media'), embeddedContext: flag('--media') }, mcpCapabilities: { http: true, sse: false }, _meta: { steering: { supported: true } } }, authMethods, agentInfo: { name: flag('--kiro') ? 'Kiro CLI Agent' : flag('--antigravity') ? 'antigravity-acp' : 'fixture', version: '0.0.1' }, _meta: { vendor: 'spike' } });
    }
    // --auth-adopt: the antigravity ACP server shape — no auth choice of its
    // own until `authenticate` picks one, which adopts an existing login.
    // --cursor uses `authenticate` with `cursor_login` after the `about`
    // preflight instead.
    case 'authenticate': {
      if (flag('--cursor')) {
        if (m.params.methodId !== 'cursor_login') return send({ jsonrpc: '2.0', id: m.id, error: { code: -32602, message: `Unknown authentication method: ${m.params.methodId}` } });
        // Logged out, cursor starts a browser login and waits for it.
        if (flag('--logged-out')) return;
        authed = true;
        return reply({});
      }
      if (!flag('--auth-adopt')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32601, message: 'method not found' } });
      adopted = m.params.methodId;
      return reply({});
    }
    case 'session/new': {
      if (flag('--cursor') && !authed) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32000, message: "Authentication required. Please run 'agent login' first, then call authenticate() with methodId 'cursor_login'." } });
      if (flag('--cursor')) return reply(cursorSession());
      if (flag('--auth-required') || (flag('--auth-adopt') && !adopted)) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32000, message: flag('--capitalized-auth') ? 'Authentication required' : 'authentication required' } });
      // The hermes shape: a plain internal error whose data carries the words.
      if (flag('--auth-hint-error')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32603, message: 'Internal error', data: { details: 'No LLM provider configured. Run `fixture login` first.' } } });
      mcpDecl = m.params.mcpServers ?? [];
      // --grok-models: the first-class models state (no model configOption);
      // switching must ride session/set_model.
      if (flag('--grok-models')) return reply({ sessionId: 'sess-1', models: grokModels() });
      reply({ sessionId: 'sess-1', modes: { currentModeId: 'default', availableModes: [{ id: 'default', name: 'Default' }, { id: 'plan', name: 'Plan' }] }, configOptions: configOptions(), _meta: { claude: { sessionId: 'uuid-1' } } });
      // Real ACP agents push the command list as an update just after
      // session/new; --commands-on-open reproduces it so probe can wait for it.
      if (flag('--commands-on-open')) notify('sess-1', { sessionUpdate: 'available_commands_update', availableCommands: [{ name: 'compact', description: 'Compact context' }] });
      if (flag('--kiro')) kiroMetadata('sess-1');
      return;
    }
    case 'session/load': return reply(flag('--cursor') ? cursorSession() : { _meta: { loaded: m.params.sessionId } });
    case 'session/set_mode': {
      reply({});
      return notify(m.params.sessionId, { sessionUpdate: 'current_mode_update', currentModeId: m.params.modeId });
    }
    case 'session/set_config_option':
      // Cursor answers every switch with the full option list, including
      // the newly selected model's own options.
      if (flag('--cursor')) {
        const known = m.params.configId === 'model' ? ['default', 'composer-2.5', 'claude-opus-5'] : cursorPerModel().find(o => o.id === m.params.configId)?.options.map(o => o.value);
        if (!known?.includes(m.params.value)) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32602, message: `unknown ${m.params.configId} ${m.params.value}` } });
        if (m.params.configId === 'model') cursorModel = m.params.value; else cursorOpts[m.params.configId] = m.params.value;
        return reply({ configOptions: cursorConfig() });
      }
      // --qwen: the thought level is `reasoning_effort` on the wire, and a
      // switch answers with the full option list like any config response.
      if (flag('--qwen') && m.params.configId === qwenEffortId()) { qwenEffort = m.params.value; return reply({ configOptions: configOptions() }); }
      if (flag('--qwen') && m.params.configId === 'model') { qwenModel = m.params.value; return reply({ configOptions: configOptions() }); }
      // Under --grok-models there is no model configOption: only set_model works.
      if (m.params.configId !== 'model' || flag('--grok-models')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32602, message: `unknown config ${m.params.configId}` } });
      // --config-slow=N: delay the reply so a second configure overlaps it.
      if (num('--config-slow', 0)) return setTimeout(() => reply({}), num('--config-slow', 0));
      return reply({});
    case 'session/set_model':
      if (!flag('--grok-models')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32601, message: 'method not found' } });
      if (!['grok-4.5', 'grok-4.6', 'grok-basic'].includes(m.params.modelId)) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32602, message: `unknown model ${m.params.modelId}` } });
      if (num('--config-slow', 0)) await sleep(num('--config-slow', 0));
      grokModel = m.params.modelId;
      if (m.params._meta?.reasoningEffort) grokEffort = m.params._meta.reasoningEffort;
      reply({});
      send({ jsonrpc: '2.0', method: '_x.ai/session_notification', params: { sessionId: m.params.sessionId, update: { sessionUpdate: 'model_changed', model_id: grokModel, reasoning_effort: grokEffort } } });
      // The republished models state carries the stale default effort.
      return send({ jsonrpc: '2.0', method: '_x.ai/models/update', params: grokModels() });
    case 'session/prompt': return runTurn(m);
    case 'session/cancel': if (turn) { turn.cancelled = true; } spurious = flag('--spurious-cancel'); return;
    case '_session/steering': return reply({ accepted: true });
    default: return send({ jsonrpc: '2.0', id: m.id, error: { code: -32601, message: 'method not found' } });
  }
}

async function runTurn(m) {
  const sid = m.params.sessionId; turn = { cancelled: false };
  const done = (stopReason) => { send({ jsonrpc: '2.0', id: m.id, result: { stopReason, _meta: { usage: { inputTokens: 1 } } } }); turn = null; };
  const ptext = m.params.prompt.find(b => b.type === 'text')?.text ?? '';
  // --spurious-cancel: the first prompt after a cancel dies "cancelled" with
  // nothing said (the kiro race).
  if (spurious) { spurious = false; done('cancelled'); return; }
  if (flag('--grok-models') && ptext === 'model-state') {
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `${grokModel}:${grokEffort}` } });
    done('end_turn');
    return;
  }
  // Kiro's `/effort <level>`: an ack chunk, end_turn, and the level rides
  // every later metadata frame.
  if (flag('--kiro') && ptext.startsWith('/effort ')) {
    if (num('--effort-slow', 0)) await sleep(num('--effort-slow', 0));
    effort = ptext.slice('/effort '.length);
    // An unrelated update lands mid-switch; only the ack chunk is internal.
    notify(sid, { sessionUpdate: 'usage_update', used: 7, size: 100 });
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `Effort set to ${effort}` } });
    done('end_turn');
    return;
  }
  // Errored prompts: "die-auth" loses the credentials, "die-rpc" is a plain failure.
  if (ptext.includes('die-auth')) { send({ jsonrpc: '2.0', id: m.id, error: { code: -32000, message: 'credentials expired' } }); turn = null; return; }
  if (ptext.includes('die-rpc')) { send({ jsonrpc: '2.0', id: m.id, error: { code: -32603, message: 'kaput' } }); turn = null; return; }
  // Antigravity's ACP server asks as an `interaction_*` tool call plus a
  // permission whose options are the choices (recorded 2026-09-07).
  if (ptext.includes('interaction-question')) {
    const toolCall = { toolCallId: 'interaction_1', title: 'Red or blue?', status: 'pending', rawInput: {} };
    notify(sid, { sessionUpdate: 'tool_call', ...toolCall });
    const q = await request('session/request_permission', { sessionId: sid, toolCall, options: [{ optionId: '1', name: 'Red', kind: 'allow_once' }, { optionId: '2', name: 'Blue', kind: 'allow_once' }] });
    notify(sid, { sessionUpdate: 'tool_call_update', toolCallId: 'interaction_1', status: 'completed', rawOutput: 'Response received' });
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `q=${JSON.stringify(q.result?.outcome ?? 'error')} ` } });
    done('end_turn');
    return;
  }
  // Grok extensions (wire shapes cross-checked against comet + t3code).
  if (ptext.includes('grok-question')) {
    // The question's tool call rides alongside, tagged `ask_user` in _meta.
    notify(sid, { sessionUpdate: 'tool_call', toolCallId: 'call_q', title: 'ask_user_question', status: 'pending', _meta: { 'x.ai/tool': { name: 'ask_user_question', kind: 'ask_user' } } });
    const q = await request('_x.ai/ask_user_question', { sessionId: sid, toolCallId: 'call_q', mode: 'default', questions: [{ id: 'q1', question: 'Pick a fruit', options: [{ id: 'g', label: 'Grape', description: 'purple' }, { label: 'Mango' }], multiSelect: false }] });
    const answers = q.result?.answers ? JSON.stringify(q.result.answers) : (q.result?.outcome ?? 'error');
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `q=${answers} ` } });
    // The completion update for the question carries no tag (wire, 2026-09-09).
    notify(sid, { sessionUpdate: 'tool_call_update', toolCallId: 'call_q', status: 'completed' });
    done('end_turn');
    return;
  }
  // Cursor extensions (shapes from docs.cursor.com/cli/acp; every one
  // arrives as a request with an id on 2026.09.02).
  if (ptext.includes('cursor-question')) {
    // `cursor-question-free`: no options, so the answer is free text.
    const options = ptext.includes('free') ? [] : [{ id: 'r', label: 'Red' }, { id: 'b', label: 'Blue' }];
    const q = await request('cursor/ask_question', { toolCallId: 'call_q', title: 'Need input', questions: [{ id: 'color', prompt: 'Red or blue?', options, allowMultiple: false }] });
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `q=${JSON.stringify(q.result?.outcome ?? 'error')} ` } });
    done('end_turn');
    return;
  }
  if (ptext.includes('cursor-plan')) {
    notify(sid, { sessionUpdate: 'plan', entries: [{ content: 'write README', priority: 'medium', status: 'pending' }] });
    const p = await request('cursor/create_plan', { toolCallId: 'call_p', name: 'Add README', plan: '# Plan\n1. write', todos: [{ id: 't1', content: 'write README', status: 'pending' }] });
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `plan=${p.result?.outcome?.outcome ?? 'error'} ` } });
    done('end_turn');
    return;
  }
  if (ptext.includes('cursor-todos')) {
    notify(sid, { sessionUpdate: 'tool_call', toolCallId: 'call_s', title: 'Task: List files', kind: 'other', status: 'pending', rawInput: { _toolName: 'task', description: 'List files' } });
    await request('cursor/update_todos', { toolCallId: 'call_t', todos: [{ id: '1', content: 'first', status: 'completed' }, { id: '2', content: 'second', status: 'pending' }], merge: false });
    const t = await request('cursor/update_todos', { toolCallId: 'call_t', todos: [{ id: '2', content: 'second', status: 'in_progress' }], merge: true });
    const s = await request('cursor/task', { toolCallId: 'call_s', description: 'List files', prompt: 'ls', subagentType: 'explore', durationMs: 5 });
    const i = await request('cursor/generate_image', { toolCallId: 'call_i', description: 'icon' });
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `todos=${t.result?.outcome?.todos?.length} task=${s.result?.outcome?.outcome} image=${i.result?.outcome?.outcome} ` } });
    done('end_turn');
    return;
  }
  if (ptext.includes('grok-hang')) {
    // A stale prompt_complete (wrong promptId; refusal would be visible in
    // the stop reason) must be ignored; the frame echoing _meta.promptId ends
    // the turn. The session/prompt RPC then NEVER responds — the hang.
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'grok ' } });
    send({ jsonrpc: '2.0', method: '_x.ai/session/prompt_complete', params: { sessionId: sid, promptId: 'stale-0', stopReason: 'refusal' } });
    send({ jsonrpc: '2.0', method: '_x.ai/session/prompt_complete', params: { sessionId: sid, promptId: m.params._meta?.promptId, stopReason: 'end_turn' } });
    turn = null;
    return;
  }
  notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'Hello ' } });
  if (mcpDecl.length) notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `mcp=${mcpDecl.map(s => `${s.type ?? 'stdio'}:${s.name}`).join(',')} ` } });
  // Echo attachments so tests can assert the wire shape.
  const count = (type, key) => m.params.prompt.filter(b => b.type === type && b[key]).length;
  const imgs = count('image', 'data');
  if (imgs || ptext.includes('Attached files:')) {
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `att=${imgs} aud=${count('audio', 'data')} res=${count('resource', 'resource')} ref=1 ` } });
  }
  notify(sid, { sessionUpdate: 'agent_thought_chunk', content: { type: 'text', text: 'thinking…' } });
  notify(sid, { sessionUpdate: 'tool_call', toolCallId: 'call_1', title: 'Edit main.rs', kind: 'edit', status: 'pending', rawInput: { path: 'main.rs' }, locations: [{ path: 'main.rs', line: 3 }], content: [{ type: 'diff', path: 'main.rs', oldText: 'a', newText: 'b' }], extraVendorField: 42, _meta: { claude: { toolUseId: 'toolu_1' } } });
  notify(sid, { sessionUpdate: 'tool_call_update', toolCallId: 'call_1', status: 'completed', rawOutput: { ok: true }, content: [{ type: 'content', content: { type: 'text', text: 'done' } }] });
  notify(sid, { sessionUpdate: 'plan', entries: [{ content: 'step 1', priority: 'high', status: 'in_progress' }] });
  notify(sid, { sessionUpdate: 'available_commands_update', availableCommands: [{ name: 'compact', description: 'Compact context' }] });
  // Cursor sends no usage frames.
  if (!flag('--cursor')) notify(sid, { sessionUpdate: 'usage_update', used: 1200, size: 200000, cost: { amount: 0.01, currency: 'USD' }, _meta: { '_claude/rateLimit': { status: 'allowed', resetsAt: 1 } } });
  notify(sid, { sessionUpdate: 'some_future_update_kind', payload: { x: 1 } }); // unknown kind
  send({ jsonrpc: '2.0', method: '_claude/rateLimit', params: { sessionId: sid, status: 'allowed_warning' } }); // ext notification
  const perm = await request('session/request_permission', { sessionId: sid, toolCall: { toolCallId: 'call_2', title: 'Run tests' }, options: [{ optionId: 'allow', name: 'Allow', kind: 'allow_once' }, { optionId: 'reject', name: 'Reject', kind: 'reject_once' }] });
  const outcome = perm.result?.outcome?.outcome ?? 'error';
  notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `perm=${outcome} ` } });
  // "die-late": a plain RPC failure after the permission exchange.
  if (ptext.includes('die-late')) { send({ jsonrpc: '2.0', id: m.id, error: { code: -32603, message: 'kaput' } }); turn = null; return; }
  if (flag('--eof')) { process.stderr.write('boom: fixture died\n'); process.exit(3); }
  const flood = num('--flood', 0);
  for (let i = 0; i < flood; i++) notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'x'.repeat(100) } });
  if (flood) await sleep(50);
  if (turn.cancelled) { done('cancelled'); return; }
  done('end_turn');
  if (flag('--kiro')) kiroMetadata(sid);
  await sleep(num('--late-ms', 100));
  notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: '(late noise)' } });
}
