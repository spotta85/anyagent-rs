// opencode v1 fixture server, shaped like `opencode serve` 1.18: HTTP routes
// plus the `/event` SSE bus, gated by the per-session basic-auth secret.
// Launch args after the scenario flags are the real ones (`serve --hostname
// 127.0.0.1 --port N`). Flags: --logged-out (no connected provider),
// --rename (the server titles the session after the first turn). Prompt
// words: "write-file" (a write asks permission), "question" (a question
// tool), "sleep" (only an abort ends the turn), "child" (a task-tool child
// session runs and asks permission), "die" (exit mid-turn).
import { createServer } from 'node:http';

const flag = (name) => process.argv.includes(name);
const argAfter = (name) => { const i = process.argv.indexOf(name); return i > -1 ? process.argv[i + 1] : undefined; };
const port = Number(argAfter('--port'));
// --port-taken: die at once the way a squatted port kills the real server.
if (flag('--port-taken')) { process.stderr.write(`Error: listen EADDRINUSE: address already in use 127.0.0.1:${port}\n`); process.exit(1); }
const secret = process.env.OPENCODE_SERVER_PASSWORD ?? '';
const config = JSON.parse(process.env.OPENCODE_CONFIG_CONTENT ?? '{}');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const sessions = {};
let sesN = 0, msgN = 0, partN = 0, askN = 0;
const bus = new Set();
const busy = {};
const waiters = {};
const emit = (type, properties) => { const line = `data: ${JSON.stringify({ type, properties })}\n\n`; for (const res of bus) res.write(line); };

function newSession(parentID) {
  const id = `ses_${++sesN}`;
  sessions[id] = { id, title: `New session - 2026-09-05T00:00:00.00${sesN}Z`, model: { providerID: 'opencode', modelID: 'big-pickle' }, messages: [], parentID, reverted: null, fork: undefined };
  return sessions[id];
}

// A session id this process never made stands for one on disk from an
// earlier server (resume, fork): it comes back with a one-turn history.
function known(id) {
  if (!sessions[id] && /^ses_\d+$/.test(id)) {
    sessions[id] = { id, title: 'Earlier talk', model: { providerID: 'opencode', modelID: 'big-pickle' }, messages: [], reverted: null, fork: undefined };
    for (const role of ['user', 'assistant']) message(id, role);
  }
  return sessions[id];
}
const message = (sid, role) => { const m = { id: `msg_${String(++msgN).padStart(3, '0')}`, sessionID: sid, role, time: { created: msgN } }; sessions[sid].messages.push(m); return m; };
const part = (sid, messageID, fields) => ({ id: `prt_${++partN}`, sessionID: sid, messageID, ...fields });
const partUpdated = (sid, p) => emit('message.part.updated', { sessionID: sid, part: p });
const ask = (kind, sid, props) => { const id = `${kind}_${++askN}`; return new Promise((r) => { waiters[id] = r; emit(kind === 'per' ? 'permission.asked' : 'question.asked', { id, sessionID: sid, ...props }); }); };

// One prompted turn on the bus; `text` is the assistant text to stream first.
async function runTurn(ses, body, text) {
  const sid = ses.id;
  busy[sid] = true;
  ses.aborting = false;
  emit('session.status', { sessionID: sid, status: { type: 'busy' } });
  const prompt = body.parts?.find((p) => p.type === 'text')?.text ?? '';
  const images = (body.parts ?? []).filter((p) => p.type === 'file').length;
  const user = message(sid, 'user');
  emit('message.updated', { sessionID: sid, info: { ...user, time: { ...user.time, completed: 1 } } });
  const asst = message(sid, 'assistant');
  emit('message.updated', { sessionID: sid, info: asst });
  if (prompt.includes('die')) { process.stderr.write('boom: fixture died\n'); process.exit(3); }
  partUpdated(sid, part(sid, asst.id, { type: 'reasoning', text: 'thinking…' }));
  const tp = part(sid, asst.id, { type: 'text', text: 'Hello ' });
  partUpdated(sid, tp);
  const say = (s) => { tp.text += s; emit('message.part.delta', { sessionID: sid, messageID: asst.id, partID: tp.id, field: 'text', delta: s }); };
  say(text);
  say(`model=${body.model ? `${body.model.providerID}/${body.model.modelID}` : 'unset'} variant=${body.variant ?? 'unset'} images=${images} `);
  if (prompt.includes('Attached files:')) say('ref=1 ');
  if (ses.reverted) say(`reverted=${ses.reverted} `);
  if (ses.fork !== undefined) say(`fork=${ses.fork ?? 'tip'} `);
  if (prompt.includes('sleep')) {
    const bash = part(sid, asst.id, { type: 'tool', tool: 'bash', callID: `call_${partN}`, state: { status: 'running', input: { command: 'sleep 45' } } });
    partUpdated(sid, bash);
    while (!ses.aborting) await sleep(10);
    return abortTurn(ses, asst);
  }
  const bash = part(sid, asst.id, { type: 'tool', tool: 'bash', callID: `call_${partN}`, state: { status: 'pending', input: {} } });
  partUpdated(sid, bash);
  partUpdated(sid, { ...bash, state: { status: 'completed', input: { command: 'echo PEAR' }, output: 'PEAR\n' } });
  if (prompt.includes('write-file')) {
    const write = part(sid, asst.id, { type: 'tool', tool: 'write', callID: `call_${partN + 1}`, state: { status: 'running', input: { filePath: 'fruit.txt', content: 'PEAR' } } });
    partUpdated(sid, write);
    const resp = await ask('per', sid, { permission: 'write', patterns: ['fruit.txt'], metadata: { filepath: 'fruit.txt' }, tool: { messageID: asst.id, callID: write.callID } });
    if (ses.aborting) return abortTurn(ses, asst);
    partUpdated(sid, { ...write, state: resp === 'reject' ? { status: 'error', input: write.state.input, error: 'denied' } : { status: 'completed', input: write.state.input, output: 'wrote fruit.txt' } });
    say(`perm=${resp} `);
  }
  if (prompt.includes('question')) {
    const answers = await ask('que', sid, { questions: [{ question: 'Which color?', header: 'Color', options: [{ label: 'Red', description: 'Prefer red' }, { label: 'Blue' }], multiple: false, custom: false }] });
    if (ses.aborting) return abortTurn(ses, asst);
    say(`q=${answers.map((a) => a.join('+')).join(',')} `);
  }
  if (prompt.includes('child')) await runChild(ses, asst, say);
  partUpdated(sid, part(sid, asst.id, { type: 'tool', tool: 'todowrite', callID: `call_${partN + 1}`, state: { status: 'completed', input: { todos: [{ content: 'step 1', status: 'in_progress' }] }, output: '' } }));
  say('done');
  partUpdated(sid, part(sid, asst.id, { type: 'step-finish', tokens: { total: 1200, input: 1000, output: 200 }, cost: 0.01 }));
  emit('message.updated', { sessionID: sid, info: { ...asst, time: { ...asst.time, completed: 2 } } });
  busy[sid] = false;
  emit('session.idle', { sessionID: sid });
  if (flag('--rename') && !ses.renamed) { ses.renamed = true; ses.title = 'Pear talk'; emit('session.updated', { info: { id: sid, title: ses.title } }); }
}

// An abort: the message ends with the abort error, idle comes twice (the
// second one late), like 1.18.
function abortTurn(ses, asst) {
  const sid = ses.id;
  const error = { name: 'MessageAbortedError', data: {} };
  emit('message.updated', { sessionID: sid, info: { ...asst, error, time: { ...asst.time, completed: 3 } } });
  emit('session.error', { sessionID: sid, error });
  busy[sid] = false;
  emit('session.idle', { sessionID: sid });
  setTimeout(() => emit('session.idle', { sessionID: sid }), 30);
}

// A task-tool child session: created, streams under the parent's task tool,
// asks a permission of its own, then finishes.
async function runChild(ses, asst, say) {
  const sid = ses.id;
  const task = part(sid, asst.id, { type: 'tool', tool: 'task', callID: `call_task_${partN}`, state: { status: 'running', input: { description: 'review' } } });
  partUpdated(sid, task);
  const child = newSession(sid);
  emit('session.created', { sessionID: child.id, info: { id: child.id, parentID: sid, title: 'Child session - 2026' } });
  const cm = message(child.id, 'assistant');
  emit('message.updated', { sessionID: child.id, info: cm });
  partUpdated(child.id, part(child.id, cm.id, { type: 'text', text: 'child text' }));
  const resp = await ask('per', child.id, { permission: 'bash', patterns: ['ls'], metadata: { command: 'ls' }, tool: { messageID: cm.id, callID: 'call_child' } });
  emit('message.updated', { sessionID: child.id, info: { ...cm, time: { ...cm.time, completed: 4 } } });
  partUpdated(sid, { ...task, state: { status: 'completed', input: task.state.input, output: `child ${resp}` } });
  say(`child=${resp} `);
}

const json = (res, status, body) => { const s = JSON.stringify(body ?? {}); res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(s) }); res.end(s); };
const readBody = (req) => new Promise((r) => { let b = ''; req.on('data', (c) => (b += c)); req.on('end', () => r(b ? JSON.parse(b) : {})); });

createServer(async (req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1');
  if (req.headers.authorization !== `Basic ${Buffer.from(`opencode:${secret}`).toString('base64')}`) return json(res, 401, { error: 'unauthorized' });
  const p = url.pathname.split('/').filter(Boolean);
  const body = req.method === 'POST' ? await readBody(req) : {};
  const ses = p[0] === 'session' && p[1] !== 'status' && known(p[1]);
  if (req.method === 'GET' && url.pathname === '/global/health') return json(res, 200, { version: '1.18.24' });
  if (req.method === 'GET' && url.pathname === '/config/providers') return json(res, 200, { providers: [{ id: 'opencode', models: { 'big-pickle': { name: 'Big Pickle', limit: { context: 128000 }, variants: { high: {}, low: {} } }, small: { name: 'Small', limit: { context: 8000 } } } }, { id: 'offline', models: { x: { name: 'X' } } }] });
  if (req.method === 'GET' && url.pathname === '/provider') return json(res, 200, { connected: flag('--logged-out') ? [] : ['opencode'] });
  if (req.method === 'GET' && url.pathname === '/command') return json(res, 200, [{ name: 'init', description: 'Initialize the project' }]);
  if (req.method === 'GET' && url.pathname === '/session/status') return json(res, 200, Object.fromEntries(Object.keys(sessions).map((id) => [id, { type: busy[id] ? 'busy' : 'idle' }])));
  if (req.method === 'GET' && url.pathname === '/event') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
    res.write('data: {"type":"server.connected","properties":{}}\n\n');
    bus.add(res);
    req.on('close', () => bus.delete(res));
    return;
  }
  if (req.method === 'POST' && url.pathname === '/session') { const s = newSession(); return json(res, 200, { id: s.id, title: s.title, model: s.model }); }
  if (req.method === 'POST' && p[0] === 'question' && p[2] === 'reply') { waiters[p[1]]?.(body.answers); delete waiters[p[1]]; return json(res, 200, true); }
  if (!ses) return json(res, 404, { error: `no session ${p[1]}` });
  if (req.method === 'GET' && p.length === 2) return json(res, 200, { id: ses.id, title: ses.title, model: ses.model });
  if (req.method === 'GET' && p[2] === 'message') return json(res, 200, ses.messages.map((m) => ({ info: m, parts: [] })));
  if (req.method === 'POST' && p[2] === 'prompt_async') { if (busy[ses.id]) return json(res, 400, { error: 'busy' }); runTurn(ses, body, '').catch(() => process.exit(1)); return json(res, 202, {}); }
  if (req.method === 'POST' && p[2] === 'command') { await runTurn(ses, { parts: [{ type: 'text', text: '' }], model: body.model }, `cmd=${body.command} args=${body.arguments} `); return json(res, 200, { info: {}, parts: [] }); }
  if (req.method === 'POST' && p[2] === 'abort') { ses.aborting = true; return json(res, 200, true); }
  if (req.method === 'POST' && p[2] === 'permissions') { waiters[p[3]]?.(body.response); delete waiters[p[3]]; return json(res, 200, true); }
  if (req.method === 'POST' && p[2] === 'summarize') {
    busy[ses.id] = true;
    emit('session.status', { sessionID: ses.id, status: { type: 'busy' } });
    const m = message(ses.id, 'assistant');
    emit('message.updated', { sessionID: ses.id, info: m });
    partUpdated(ses.id, part(ses.id, m.id, { type: 'text', text: 'summary' }));
    emit('message.updated', { sessionID: ses.id, info: { ...m, time: { ...m.time, completed: 5 } } });
    emit('session.compacted', { sessionID: ses.id });
    busy[ses.id] = false;
    emit('session.idle', { sessionID: ses.id });
    return json(res, 200, true);
  }
  if (req.method === 'POST' && p[2] === 'revert') { ses.reverted = body.messageID; return json(res, 200, { id: ses.id }); }
  if (req.method === 'POST' && p[2] === 'fork') { const f = newSession(); f.fork = body.messageID ?? null; return json(res, 200, { id: f.id, title: f.title, model: f.model }); }
  json(res, 404, { error: `no route ${req.method} ${url.pathname}` });
}).listen(port, '127.0.0.1');
