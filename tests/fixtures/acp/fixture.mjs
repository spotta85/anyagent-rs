// ACP v1 fixture agent: speaks JSON-RPC over stdio and emits the annoying cases.
// Flags: --eof (die mid-turn), --flood=N (N chunks before the response),
//        --late-ms=N (late noise delay), --auth-required (session/new fails),
//        --commands-on-open (push availableCommands right after session/new),
//        --kiro (the kiro shape: agentInfo name, effort in _kiro.dev/metadata,
//        `/effort <level>` prompts answered with an ack chunk).
import { createInterface } from 'node:readline';

const flag = (name) => process.argv.includes(name);
const num = (name, dflt) => +(process.argv.find(a => a.startsWith(name + '='))?.split('=')[1] ?? dflt);
const send = (m) => process.stdout.write(JSON.stringify(m) + '\n');
const notify = (sessionId, update) => send({ jsonrpc: '2.0', method: 'session/update', params: { sessionId, update } });
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
let nextId = 100, pending = {}, turn = null, mcpDecl = [], effort = 'high';
// --grok-models: per-model reasoning efforts in `_meta`. As on grok 1.0.4,
// `reasoningEffort` there is a static default; the effort in force is only
// reported by the `model_changed` session notification.
let grokModel = 'grok-4.5', grokEffort = 'high';
const grokModels = () => ({ currentModelId: grokModel, availableModels: [
  { modelId: 'grok-4.5', name: 'Grok 4.5', description: 'fast', _meta: { reasoningEffort: 'high', reasoningEfforts: [{ value: 'low', label: 'Low Effort' }, { value: 'high', label: 'High Effort', description: 'default' }] } },
  { modelId: 'grok-4.6', name: 'Grok 4.6', _meta: { reasoningEffort: 'high', reasoningEfforts: [{ value: 'low', label: 'Low Effort' }, { value: 'high', label: 'High Effort' }, { value: 'xhigh', label: 'Extra High' }] } },
] });
const kiroMetadata = (sessionId) => send({ jsonrpc: '2.0', method: '_kiro.dev/metadata', params: { sessionId, contextUsagePercentage: 0.5, effort } });

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
      return reply({ protocolVersion: 1, agentCapabilities: { loadSession: !flag('--no-load'), promptCapabilities: { image: true }, mcpCapabilities: { http: true, sse: false }, _meta: { steering: { supported: true } } }, authMethods, agentInfo: { name: flag('--kiro') ? 'Kiro CLI Agent' : 'fixture', version: '0.0.1' }, _meta: { vendor: 'spike' } });
    }
    case 'session/new':
      if (flag('--auth-required')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32000, message: flag('--capitalized-auth') ? 'Authentication required' : 'authentication required' } });
      // The hermes shape: a plain internal error whose data carries the words.
      if (flag('--auth-hint-error')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32603, message: 'Internal error', data: { details: 'No LLM provider configured. Run `fixture login` first.' } } });
      mcpDecl = m.params.mcpServers ?? [];
      // --grok-models: the first-class models state (no model configOption);
      // switching must ride session/set_model.
      if (flag('--grok-models')) return reply({ sessionId: 'sess-1', models: grokModels() });
      // --kiro adds a model without effort levels.
      const models = [{ value: 'sonnet', name: 'Sonnet' }, { value: 'opus', name: 'Opus' }, ...(flag('--kiro') ? [{ value: 'claude-haiku-4.5', name: 'Haiku' }] : [])];
      reply({ sessionId: 'sess-1', modes: { currentModeId: 'default', availableModes: [{ id: 'default', name: 'Default' }, { id: 'plan', name: 'Plan' }] }, configOptions: [{ id: 'model', name: 'Model', category: 'model', type: 'select', currentValue: 'sonnet', options: models }], _meta: { claude: { sessionId: 'uuid-1' } } });
      // Real ACP agents push the command list as an update just after
      // session/new; --commands-on-open reproduces it so probe can wait for it.
      if (flag('--commands-on-open')) notify('sess-1', { sessionUpdate: 'available_commands_update', availableCommands: [{ name: 'compact', description: 'Compact context' }] });
      if (flag('--kiro')) kiroMetadata('sess-1');
      return;
    case 'session/load': return reply({ _meta: { loaded: m.params.sessionId } });
    case 'session/set_mode': {
      reply({});
      return notify(m.params.sessionId, { sessionUpdate: 'current_mode_update', currentModeId: m.params.modeId });
    }
    case 'session/set_config_option':
      // Under --grok-models there is no model configOption: only set_model works.
      if (m.params.configId !== 'model' || flag('--grok-models')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32602, message: `unknown config ${m.params.configId}` } });
      // --config-slow=N: delay the reply so a second configure overlaps it.
      if (num('--config-slow', 0)) return setTimeout(() => reply({}), num('--config-slow', 0));
      return reply({});
    case 'session/set_model':
      if (!flag('--grok-models')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32601, message: 'method not found' } });
      if (!['grok-4.5', 'grok-4.6'].includes(m.params.modelId)) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32602, message: `unknown model ${m.params.modelId}` } });
      grokModel = m.params.modelId;
      if (m.params._meta?.reasoningEffort) grokEffort = m.params._meta.reasoningEffort;
      reply({});
      send({ jsonrpc: '2.0', method: '_x.ai/session_notification', params: { sessionId: m.params.sessionId, update: { sessionUpdate: 'model_changed', model_id: grokModel, reasoning_effort: grokEffort } } });
      // The republished models state carries the stale default effort.
      return send({ jsonrpc: '2.0', method: '_x.ai/models/update', params: grokModels() });
    case 'session/prompt': return runTurn(m);
    case 'session/cancel': if (turn) { turn.cancelled = true; } return;
    case '_session/steering': return reply({ accepted: true });
    default: return send({ jsonrpc: '2.0', id: m.id, error: { code: -32601, message: 'method not found' } });
  }
}

async function runTurn(m) {
  const sid = m.params.sessionId; turn = { cancelled: false };
  const done = (stopReason) => { send({ jsonrpc: '2.0', id: m.id, result: { stopReason, _meta: { usage: { inputTokens: 1 } } } }); turn = null; };
  const ptext = m.params.prompt.find(b => b.type === 'text')?.text ?? '';
  // Kiro's `/effort <level>`: an ack chunk, end_turn, and the level rides
  // every later metadata frame.
  if (flag('--kiro') && ptext.startsWith('/effort ')) {
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
  // Grok extensions (wire shapes cross-checked against comet + t3code).
  if (ptext.includes('grok-question')) {
    const q = await request('_x.ai/ask_user_question', { sessionId: sid, toolCallId: 'call_q', mode: 'default', questions: [{ id: 'q1', question: 'Pick a fruit', options: [{ id: 'g', label: 'Grape', description: 'purple' }, { label: 'Mango' }], multiSelect: false }] });
    const answers = q.result?.answers ? JSON.stringify(q.result.answers) : (q.result?.outcome ?? 'error');
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `q=${answers} ` } });
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
  const imgs = m.params.prompt.filter(b => b.type === 'image' && b.data).length;
  if (imgs || ptext.includes('Attached files:')) {
    notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `att=${imgs} ref=1 ` } });
  }
  notify(sid, { sessionUpdate: 'agent_thought_chunk', content: { type: 'text', text: 'thinking…' } });
  notify(sid, { sessionUpdate: 'tool_call', toolCallId: 'call_1', title: 'Edit main.rs', kind: 'edit', status: 'pending', rawInput: { path: 'main.rs' }, locations: [{ path: 'main.rs', line: 3 }], content: [{ type: 'diff', path: 'main.rs', oldText: 'a', newText: 'b' }], extraVendorField: 42, _meta: { claude: { toolUseId: 'toolu_1' } } });
  notify(sid, { sessionUpdate: 'tool_call_update', toolCallId: 'call_1', status: 'completed', rawOutput: { ok: true }, content: [{ type: 'content', content: { type: 'text', text: 'done' } }] });
  notify(sid, { sessionUpdate: 'plan', entries: [{ content: 'step 1', priority: 'high', status: 'in_progress' }] });
  notify(sid, { sessionUpdate: 'available_commands_update', availableCommands: [{ name: 'compact', description: 'Compact context' }] });
  notify(sid, { sessionUpdate: 'usage_update', used: 1200, size: 200000, cost: { amount: 0.01, currency: 'USD' }, _meta: { '_claude/rateLimit': { status: 'allowed', resetsAt: 1 } } });
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
