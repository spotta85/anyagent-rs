// agy stream-json fixture agent, shaped like the recordings in this
// directory (agy 1.1.24): one JSON object per line both ways.
//
// It also answers the two side processes the adapter runs: `--version` and
// `--output-format=json models`.
//
// Flags: --logged-out (no init, an ERROR result plus the stderr line agy
// prints). Prompt words steer scenarios: "tool" (a run_command call, denied
// unless --dangerously-skip-permissions), "ask" (a skipped question),
// "sleep" (a spoken step, then a tool only a kill ends), "subagent", "fail" (the model errors),
// "die" (the process exits mid-turn), "recall" (echoes the conversation id
// and the launch flags), "chunks" (text in two deltas).
import { createInterface } from 'node:readline';

const argv = process.argv.slice(2);
const flag = (name) => argv.includes(name);
const after = (name) => argv[argv.indexOf(name) + 1];
const send = (frame) => process.stdout.write(JSON.stringify(frame) + '\n');
const usage = (n) => ({ input_tokens: n, output_tokens: 1, thinking_tokens: 0, cache_read_tokens: 0, total_tokens: n + 1 });

// --- side processes the adapter shells out to -------------------------------

if (flag('--version')) {
  process.stdout.write('1.1.24\n');
  process.exit(0);
}
if (flag('models')) {
  const models = [
    { id: 'gemini-3.8-flash-high', label: 'Gemini 3.8 Flash (High)' },
    { id: 'gemini-3.8-flash-low', label: 'Gemini 3.8 Flash (Low)' },
    { id: 'claude-sonnet-4-6', label: 'Claude Sonnet 4.6 (Thinking)' },
  ];
  send({ conversation_id: '', status: 'SUCCESS', response: '', duration_seconds: 0, num_turns: 0, usage: usage(0), command: { name: 'models', data: { models } } });
  process.exit(0);
}

// --- startup: the three ways a launch ends before `init` --------------------

const model = argv.includes('--model') ? after('--model') : 'gemini-3.8-flash-high';
const startupError = (error) => {
  send({ event: 'result', result: { conversation_id: '', status: 'ERROR', response: '', error, duration_seconds: 0, num_turns: 0, usage: usage(0) } });
  process.exit(1);
};
if (flag('--logged-out')) {
  process.stderr.write("Error: authentication required. Run 'agy' to log in, then retry.\n");
  startupError('authentication failed or timed out');
}
if (!model.startsWith('gemini-') && !model.startsWith('claude-')) {
  startupError(`invalid model selection (--model "${model}" --effort ""): model ${model} is not recognized as a known model or custom model in settings`);
}

// --- state ------------------------------------------------------------------

const conversation = argv.includes('--conversation') ? after('--conversation') : 'c1';
const skip = flag('--dangerously-skip-permissions');
let step = 0, turns = 0;

send({ event: 'init', conversation_id: conversation, init: { cwd: process.cwd(), tools: ['ask_question', 'run_command', 'write_to_file'], permission_mode: skip ? 'always-proceed' : 'request-review' } });

const rl = createInterface({ input: process.stdin });
rl.on('line', (line) => onUser(JSON.parse(line)).catch(() => process.exit(1)));
rl.on('close', () => process.exit(0));

// --- turns ------------------------------------------------------------------

const update = (fields) => send({ event: 'step_update', step_update: { conversation_id: conversation, step_index: step, ...fields } });
const text = (delta, state = 'DONE') => update({ state, step_type: 'agent_response', text_delta: delta, ...(state === 'DONE' && { duration_seconds: 1, usage: usage(13762) }) });
// `result.usage` sums every step snapshot (recorded): twice a single step.
const result = (response, status = 'SUCCESS', error) => {
  turns += 1;
  send({ event: 'result', result: { conversation_id: conversation, status, response, ...(error && { error }), duration_seconds: 1, num_turns: turns, usage: usage(2 * 13762 + 1) } });
};

async function onUser(frame) {
  const prompt = frame.message.content;
  update({ state: 'DONE', step_type: 'user_input' });
  step += 1;
  if (prompt.includes('die')) process.exit(1);
  if (prompt.includes('fail')) return result('', 'ERROR', 'model exploded');
  if (prompt.includes('sleep')) {
    text('On it.\n');
    step += 1;
    update({ state: 'ACTIVE', step_type: 'tool', tool_name: 'run_command', tool_info: { name: 'run_command', parameters: { CommandLine: 'sleep 25' } } });
    await new Promise((r) => setTimeout(r, 30_000));
    return result('slept');
  }
  if (prompt.includes('tool')) {
    const info = { name: 'run_command', parameters: { CommandLine: 'echo hello > probe.txt' } };
    update({ state: 'ACTIVE', step_type: 'tool', tool_name: 'run_command', tool_info: info });
    if (skip) {
      update({ state: 'DONE', step_type: 'tool', tool_name: 'run_command', duration_seconds: 0.02, tool_info: { ...info, output: 'hello\n' } });
      step += 1;
      text('Done: I wrote the file.\n');
    } else {
      update({ state: 'ERROR', step_type: 'tool', tool_name: 'run_command', duration_seconds: 0.03, tool_info: { ...info, error: { type: 'TOOL_ERROR', message: 'permission check failed for command "echo hello > probe.txt": user denied permission to run command' } } });
      process.stderr.write('jetski: no output produced — a tool required the "command" permission that headless mode cannot prompt for, so it was auto-denied.\n');
    }
    return result(skip ? 'Done: I wrote the file.\n' : '');
  }
  if (prompt.includes('ask')) {
    update({ state: 'DONE', step_type: 'unknown', duration_seconds: 0.02 });
    step += 1;
    text('You skipped the question!\n');
    return result('You skipped the question!\n');
  }
  if (prompt.includes('subagent')) {
    const info = { subagents: [{ type_name: 'pong_agent', role: 'Pong Responder', initial_prompt: 'ping', conversation_id: 'child-1', log_uri: 'file://~/.gemini/antigravity-cli/brain/child-1/.system_generated/logs/transcript.jsonl' }] };
    update({ state: 'ACTIVE', step_type: 'subagent', tool_name: 'invoke_subagent', subagent_info: info });
    update({ state: 'DONE', step_type: 'subagent', tool_name: 'invoke_subagent', duration_seconds: 0.03, subagent_info: info });
    step += 1;
    text('The subagent said pong.\n');
    return result('The subagent said pong.\n');
  }
  if (prompt.includes('chunks')) {
    text('I have created the', 'ACTIVE');
    text(' file.\n');
    return result('I have created the file.\n');
  }
  if (prompt.includes('recall')) {
    const reply = `recalled ${conversation} flags=${argv.filter((a) => a.startsWith('--')).join(',')}\n`;
    text(reply);
    return result(reply);
  }
  text('pong\n');
  result('pong\n');
}
