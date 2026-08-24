// ACP v1 fixture agent: speaks JSON-RPC over stdio and emits the annoying cases.
// Flags: --eof (die mid-turn), --flood=N (N chunks before the response),
//        --late-ms=N (late noise delay), --auth-required (session/new fails).
import { createInterface } from 'node:readline';

const flag = (name) => process.argv.includes(name);
const num = (name, dflt) => +(process.argv.find(a => a.startsWith(name + '='))?.split('=')[1] ?? dflt);
const send = (m) => process.stdout.write(JSON.stringify(m) + '\n');
const notify = (sessionId, update) => send({ jsonrpc: '2.0', method: 'session/update', params: { sessionId, update } });
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
let nextId = 100, pending = {}, turn = null, mcpDecl = [];

const rl = createInterface({ input: process.stdin });
rl.on('line', (line) => { const m = JSON.parse(line); if (m.method) onRequest(m); else onResponse(m); });
rl.on('close', () => process.exit(0));

function onResponse(m) { const r = pending[m.id]; delete pending[m.id]; r?.(m); }
function request(method, params) { const id = nextId++; return new Promise(r => { pending[id] = r; send({ jsonrpc: '2.0', id, method, params }); }); }

async function onRequest(m) {
  const reply = (result) => send({ jsonrpc: '2.0', id: m.id, result });
  switch (m.method) {
    case 'initialize': return reply({ protocolVersion: 1, agentCapabilities: { loadSession: !flag('--no-load'), promptCapabilities: { image: true }, mcpCapabilities: { http: true, sse: false }, _meta: { steering: { supported: true } } }, authMethods: [{ id: 'fixture-login', name: 'Log in', type: 'terminal', args: ['auth', 'login'] }], agentInfo: { name: 'fixture', version: '0.0.1' }, _meta: { vendor: 'spike' } });
    case 'session/new':
      if (flag('--auth-required')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32000, message: 'authentication required' } });
      mcpDecl = m.params.mcpServers ?? [];
      // --grok-models: the first-class models state (no model configOption);
      // switching must ride session/set_model.
      if (flag('--grok-models')) return reply({ sessionId: 'sess-1', models: { currentModelId: 'grok-4.5', availableModels: [{ modelId: 'grok-4.5', name: 'Grok 4.5', description: 'fast' }, { modelId: 'grok-4.6', name: 'Grok 4.6' }] } });
      return reply({ sessionId: 'sess-1', modes: { currentModeId: 'default', availableModes: [{ id: 'default', name: 'Default' }, { id: 'plan', name: 'Plan' }] }, configOptions: [{ id: 'model', name: 'Model', category: 'model', type: 'select', currentValue: 'sonnet', options: [{ value: 'sonnet', name: 'Sonnet' }] }], _meta: { claude: { sessionId: 'uuid-1' } } });
    case 'session/load': return reply({ _meta: { loaded: m.params.sessionId } });
    case 'session/set_mode': {
      reply({});
      return notify(m.params.sessionId, { sessionUpdate: 'current_mode_update', currentModeId: m.params.modeId });
    }
    case 'session/set_config_option':
      // Under --grok-models there is no model configOption: only set_model works.
      if (m.params.configId !== 'model' || flag('--grok-models')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32602, message: `unknown config ${m.params.configId}` } });
      return reply({});
    case 'session/set_model':
      if (!flag('--grok-models')) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32601, message: 'method not found' } });
      if (!['grok-4.5', 'grok-4.6'].includes(m.params.modelId)) return send({ jsonrpc: '2.0', id: m.id, error: { code: -32602, message: `unknown model ${m.params.modelId}` } });
      return reply({});
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
  if (flag('--eof')) { process.stderr.write('boom: fixture died\n'); process.exit(3); }
  const flood = num('--flood', 0);
  for (let i = 0; i < flood; i++) notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'x'.repeat(100) } });
  if (flood) await sleep(50);
  if (turn.cancelled) { done('cancelled'); return; }
  done('end_turn');
  await sleep(num('--late-ms', 100));
  notify(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: '(late noise)' } });
}
