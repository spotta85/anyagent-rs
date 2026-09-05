// pi RPC fixture agent, shaped like the recordings in this directory
// (pi 0.84.4): one JSON object per line both ways.
//
// It also answers the two side processes the adapter runs: `--version` and
// `auth check --provider <p> --json`.
//
// Flags: --logged-out (no provider resolves, and auth check says so),
// --api-key (auth check reports an api_key login), --reject-prompt (the
// first prompt is refused). Prompt words steer scenarios: "tool" (a bash
// call with streamed output), "sleep" (a tool only an abort ends), "ask"
// (an extension select dialog mid-turn), "confirm" (a timed confirm dialog),
// "fail" (the model errors).
import { createInterface } from 'node:readline';
import { homedir } from 'node:os';
import { join } from 'node:path';

const flag = (name) => process.argv.includes(name);
const send = (frame) => process.stdout.write(JSON.stringify(frame) + '\n');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// --- side processes the adapter shells out to -------------------------------

if (flag('--version')) {
  process.stdout.write('0.84.4\n');
  process.exit(0);
}
if (process.argv.includes('auth')) {
  const ready = !flag('--logged-out');
  process.stdout.write(JSON.stringify(
    ready
      ? { status: 'ready', provider: 'openrouter', authType: flag('--api-key') ? 'api_key' : 'oauth' }
      : { status: 'not_ready', provider: 'openrouter', reason: 'credentials_not_configured' },
  ) + '\n');
  process.exit(0);
}

// --- state ------------------------------------------------------------------

const MODELS = [
  { id: 'nemo-1', name: 'Nemo One', provider: 'openrouter', contextWindow: 100000, reasoning: true },
  { id: 'claude-x', name: 'Claude X', provider: 'anthropic', contextWindow: 200000, reasoning: true },
];
const UNKNOWN = { id: 'unknown', name: 'unknown', provider: 'unknown', contextWindow: 0 };
// Levels are per model, so a model change re-reads them.
const LEVELS = { 'nemo-1': ['off', 'low', 'medium'], 'claude-x': ['off', 'high', 'max'] };

const agentDir = process.env.PI_CODING_AGENT_DIR ?? join(homedir(), '.pi', 'agent');
const sessionArg = process.argv[process.argv.indexOf('--session') + 1];

let model = flag('--logged-out') ? UNKNOWN : MODELS[0];
let thinking = 'medium';
let sessionFile = process.argv.includes('--session') ? sessionArg : join(agentDir, 'sessions', 's1.jsonl');
let streaming = false;
let messageN = 0, dialogN = 0, rejected = false;
const dialogs = {}; // extension dialog id -> resolver

const state = () => ({
  model, thinkingLevel: thinking, isStreaming: streaming, isCompacting: false,
  steeringMode: 'one-at-a-time', followUpMode: 'one-at-a-time',
  sessionFile, sessionId: 's1', autoCompactionEnabled: true,
  messageCount: messageN, pendingMessageCount: 0,
});

const rl = createInterface({ input: process.stdin });
rl.on('line', (line) => onCommand(JSON.parse(line)).catch(() => process.exit(1)));
rl.on('close', () => process.exit(0));

// --- commands ---------------------------------------------------------------

async function onCommand(cmd) {
  const ok = (data) => send({ id: cmd.id, type: 'response', command: cmd.type, success: true, ...(data !== undefined && { data }) });
  const no = (error) => send({ id: cmd.id, type: 'response', command: cmd.type, success: false, error });
  switch (cmd.type) {
    case 'get_state':
      return ok(state());
    case 'get_available_models':
      return ok({ models: flag('--logged-out') ? [] : MODELS });
    case 'get_available_thinking_levels':
      return ok({ levels: LEVELS[model.id] ?? ['off'] });
    case 'get_commands':
      return ok({ commands: [
        { name: 'review', description: 'Review the diff', source: 'prompt' },
        { name: 'skill:release', description: 'Cut a release', source: 'skill' },
      ] });
    case 'prompt': {
      if (flag('--reject-prompt') && !rejected) {
        rejected = true;
        return no('Agent is already processing. Specify streamingBehavior.');
      }
      if (streaming) return no("Agent is already processing. Specify streamingBehavior ('steer' or 'followUp') to queue the message.");
      ok();
      return run(cmd.message);
    }
    // Like pi 0.84.4: a session with nothing worth summarizing is refused
    // (probed live). The success path is modelled, not recorded: pi cannot
    // reach a provider on the machine this was written on.
    case 'compact':
      if (flag('--compact-refuses')) return no('Nothing to compact (session too small)');
      send({ type: 'compaction_start' });
      send({ type: 'compaction_end' });
      return ok();
    case 'steer':
      if (!streaming) return no('nothing is streaming');
      steered.push(cmd.message);
      ok();
      return send({ type: 'queue_update', steering: [...steered], followUp: [] });
    case 'abort':
      aborting = sawAbort = true;
      // The receipt lands after the run settles, as the real CLI does.
      return;
    case 'clear_queue': {
      const cleared = [...steered];
      steered = [];
      send({ type: 'queue_update', steering: [], followUp: [] });
      return ok({ steering: cleared, followUp: [] });
    }
    case 'set_model': {
      const next = MODELS.find((m) => m.provider === cmd.provider && m.id === cmd.modelId);
      if (!next) return no(`Model not found: ${cmd.provider}/${cmd.modelId}`);
      model = next;
      return ok(model);
    }
    case 'set_thinking_level':
      if (!(LEVELS[model.id] ?? []).includes(cmd.level)) return no(`unsupported level: ${cmd.level}`);
      thinking = cmd.level;
      return ok();
    case 'extension_ui_response': {
      const resolve = dialogs[cmd.id];
      delete dialogs[cmd.id];
      return resolve?.(cmd);
    }
    default:
      return no(`unknown command: ${cmd.type}`);
  }
}

// --- one settled run --------------------------------------------------------

let steered = [], aborting = false, sawAbort = false;

/// A whole prompt: one LLM turn per steering message, then a single settle.
async function run(message) {
  streaming = true;
  aborting = sawAbort = false;
  steered = [];
  send({ type: 'agent_start' });
  await turn(message);
  // An abort ends the current run, but pi resumes anything still queued as
  // a fresh one: only `clear_queue` stops that.
  while (steered.length) {
    aborting = false;
    await turn(steered.shift());
    send({ type: 'queue_update', steering: [...steered], followUp: [] });
  }
  streaming = false;
  send({ type: 'agent_end', messages: [], willRetry: false });
  send({ type: 'agent_settled' });
  if (sawAbort) send({ type: 'response', command: 'abort', success: true });
}

/// One LLM call: the prompt replayed as a user message, then the assistant's.
async function turn(message) {
  send({ type: 'turn_start' });
  message_('user', [{ type: 'text', text: message }]);
  const scenario = String(message);
  send({ type: 'message_start', message: { role: 'assistant', content: [], stopReason: 'pending' } });
  const id = `m${++messageN}`;
  update({ type: 'thinking_start', contentIndex: 0 });
  update({ type: 'thinking_delta', contentIndex: 0, delta: 'planning' });

  if (scenario.includes('ask')) await dialog('select');
  if (scenario.includes('confirm')) await dialog('confirm');
  if (scenario.includes('tool') || scenario.includes('sleep')) {
    await toolCall(scenario.includes('sleep'));
    if (aborting) return end('error', 'This operation was aborted');
  }
  if (scenario.includes('fail')) return end('error', 'the provider refused');

  update({ type: 'text_start', contentIndex: 1 });
  for (const chunk of ['ready ', `[${id}]`]) {
    update({ type: 'text_delta', contentIndex: 1, delta: chunk });
    await sleep(5);
  }
  end('stop');
}

/// A bash call: start, accumulating output, then the result.
async function toolCall(long) {
  const callId = 'call-1';
  update({ type: 'toolcall_start', contentIndex: 2, id: callId, toolName: 'bash' });
  const args = { command: long ? 'sleep 30' : 'echo hi' };
  send({ type: 'tool_execution_start', toolCallId: callId, toolName: 'bash', args });
  let out = '';
  for (const piece of ['one\n', 'two\n']) {
    if (aborting) break;
    out += piece;
    send({ type: 'tool_execution_update', toolCallId: callId, toolName: 'bash', args, partialResult: { content: [{ type: 'text', text: out }] } });
    await sleep(long ? 60 : 5);
  }
  while (long && !aborting) await sleep(20);
  const text = aborting ? `${out}\nCommand aborted` : out;
  send({ type: 'tool_execution_end', toolCallId: callId, toolName: 'bash', result: { content: [{ type: 'text', text }] }, isError: aborting });
  message_('toolResult', [{ type: 'text', text }]);
}

/// A blocking extension dialog; the run stops until the client answers.
/// The reply is echoed back as a notify so tests can see what pi received.
function dialog(method) {
  const id = `d${++dialogN}`;
  return new Promise((resolve) => {
    dialogs[id] = (reply) => {
      const echo = method === 'confirm' ? `confirmed ${reply.confirmed ?? 'nothing'}` : `picked ${reply.value ?? 'nothing'}`;
      send({ type: 'extension_ui_request', id: `n${id}`, method: 'notify', message: echo, notifyType: 'info' });
      resolve();
    };
    if (method === 'confirm') {
      send({ type: 'extension_ui_request', id, method: 'confirm', title: 'Delete it?', message: 'This cannot be undone.', timeout: 5000 });
    } else {
      send({ type: 'extension_ui_request', id, method: 'select', title: 'Pick a colour', options: ['Red', 'Green'] });
    }
  });
}

const update = (event) => send({ type: 'message_update', usage: usage(), assistantMessageEvent: event });

/// Closes the assistant message and its turn, carrying the stop verdict.
function end(stopReason, errorMessage) {
  const message = { role: 'assistant', content: [], model: model.id, provider: model.provider, usage: usage(), stopReason, ...(errorMessage && { errorMessage }) };
  send({ type: 'message_end', message });
  send({ type: 'turn_end', message, toolResults: [] });
}

const message_ = (role, content) => {
  send({ type: 'message_start', message: { role, content } });
  send({ type: 'message_end', message: { role, content } });
};

const usage = () => ({ input: 1200, output: 34, cacheRead: 0, cacheWrite: 0, totalTokens: 1234, cost: { input: 0.01, output: 0.02, cacheRead: 0, cacheWrite: 0, total: 0.03 } });
