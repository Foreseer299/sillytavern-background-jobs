import test from 'node:test';
import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { Writable } from 'node:stream';
import { JobService } from '../server/service.mjs';

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const frame = content => 'data: ' + JSON.stringify({ choices: [{ index: 0, delta: { content }, finish_reason: null }] }) + '\n\n';
const ending = 'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n';
const result = text => frame(text) + ending;
const base = [{ name: 'Test User', is_user: true, mes: 'Synthetic request', send_date: '2026-01-01' }];
class Response extends Writable {
    constructor() { super(); this.chunks = []; this.headers = {}; this.statusCode = 200; }
    _write(chunk, encoding, cb) { this.chunks.push(Buffer.from(chunk)); cb(); }
    setHeader(k, v) { this.headers[k] = v; }
    get text() { return Buffer.concat(this.chunks).toString(); }
}
async function setup(t, invoke, options = {}) {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'independent-bg-test-'));
    const user = { directories: { root, chats: path.join(root, 'chats') }, profile: { handle: 'synthetic' } };
    const target = { avatar: 'Character.png', file: 'Synthetic chat', chatId: 'Synthetic chat' };
    const chatfile = path.join(user.directories.chats, 'Character', target.file + '.jsonl');
    await fs.mkdir(path.dirname(chatfile), { recursive: true });
    const writeChat = async rows => fs.writeFile(chatfile, [{ chat_metadata: { integrity: 'synthetic' } }, ...rows].map(JSON.stringify).join('\n'));
    await writeChat(base);
    const service = new JobService({ invoke, ...options });
    t.after(async () => { await service.shutdown(); await fs.rm(root, { recursive: true, force: true }); });
    const input = () => ({ requestId: `${Date.now()}-${randomUUID()}`, target, base: { messages: structuredClone(base) }, generate: { type: 'normal', chat_completion_source: 'custom', messages: [{ role: 'user', content: 'synthetic' }], stream: true, proxy_password: 'SYNTHETIC_SECRET_SENTINEL' } });
    return { root, user, service, input, target, chatfile, writeChat };
}
async function finished(service, user, id) {
    for (let i = 0; i < 300; i++) { const status = await service.status(user, id); if (!['pending', 'running'].includes(status.state)) return status; await delay(5); }
    throw new Error('test timed out');
}
test('acceptance is durable, requestId is idempotent and never journals prompts/credentials', async t => {
    let calls = 0;
    const c = await setup(t, async (body, user, res) => { calls++; res.end(result('response')); });
    const input = c.input(), first = await c.service.create(c.user, input), again = await c.service.create(c.user, input);
    assert.equal(first.created, true); assert.equal(again.job.id, first.job.id);
    const status = await finished(c.service, c.user, first.job.id); assert.equal(status.state, 'completed'); assert.equal(calls, 1);
    const journal = await fs.readFile(path.join(c.root, 'background-jobs', status.id + '.json'), 'utf8');
    assert.equal(journal.includes('SYNTHETIC_SECRET_SENTINEL'), false); assert.equal(journal.includes('Synthetic request'), false); assert.equal(journal.includes('"generate"'), false);
    const mode = (await fs.stat(path.join(c.root, 'background-jobs', status.id + '.json'))).mode & 0o777; assert.equal(mode, 0o600);
});
test('closing delivery stream does not abort generation; new stream replays exact bytes', async t => {
    let aborted = false, resume;
    const gate = new Promise(resolve => { resume = resolve; });
    const c = await setup(t, async (body, user, res, socket) => { socket.on('close', () => { aborted = true; }); res.write(frame('first')); await gate; res.end(frame('second') + ending); });
    const { job } = await c.service.create(c.user, c.input());
    const response = new Response(); await c.service.stream(c.user, job.id, response); response.destroy();
    resume(); const status = await finished(c.service, c.user, job.id);
    assert.equal(aborted, false); assert.equal(status.replyText, 'firstsecond');
    const replay = new Response(); await c.service.stream(c.user, job.id, replay);
    assert.equal(replay.text, frame('first') + frame('second') + ending);
});
test('explicit cancel aborts isolated socket, leaves terminal record and never resubmits', async t => {
    let calls = 0, aborted = false;
    const c = await setup(t, async (body, user, res, socket) => { calls++; socket.on('close', () => { aborted = true; res.end(); }); res.write(frame('partial')); });
    const input = c.input(), { job } = await c.service.create(c.user, input);
    await delay(20); await c.service.cancel(c.user, job.id);
    assert.equal(aborted, true); assert.equal((await c.service.status(c.user, job.id)).state, 'cancelled');
    await c.service.create(c.user, input); assert.equal(calls, 1);
});
test('cancel immediately after acceptance prevents upstream even before run starts', async t => {
    let calls = 0;
    const c = await setup(t, async (body, user, res) => { calls++; res.end(result('unexpected')); });
    const { job } = await c.service.create(c.user, c.input()); await c.service.cancel(c.user, job.id); await delay(30);
    assert.equal(calls, 0);
});
test('ACK refuses unsaved or conflicting messages, accepts actual saved formatted text', async t => {
    const c = await setup(t, async (body, user, res) => res.end(result('raw response')));
    const { job } = await c.service.create(c.user, c.input()); await finished(c.service, c.user, job.id);
    await assert.rejects(c.service.ack(c.user, job.id, { replyText: 'formatted' }), { code: 'chat_marker_conflict' });
    const reply = { name: 'Character', is_user: false, mes: 'formatted', extra: { background_job_id: job.id } };
    await c.writeChat([{ ...base[0], mes: 'user edited' }, reply]);
    await assert.rejects(c.service.ack(c.user, job.id, { replyText: 'formatted' }), { code: 'chat_save_conflict' });
    await c.writeChat([...base, reply]); const ack = await c.service.ack(c.user, job.id, { replyText: 'formatted' });
    assert.equal(ack.acknowledged, true); assert.ok(ack.acknowledgedTextHash);
});
test('ACK cleanup retains idempotency tombstone and never sends request twice', async t => {
    let calls = 0;
    const c = await setup(t, async (body, user, res) => { calls++; res.end(result('ok')); });
    const input = c.input(), { job } = await c.service.create(c.user, input); await finished(c.service, c.user, job.id);
    await c.writeChat([...base, { is_user: false, mes: 'ok', extra: { background_job_id: job.id } }]);
    await c.service.ack(c.user, job.id, { replyText: 'ok' });
    const store = await c.service.store(c.user); await c.service.cleanup(store, true);
    const duplicate = await c.service.create(c.user, input);
    assert.equal(duplicate.job.state, 'acknowledged'); assert.equal(duplicate.job.acknowledged, true); assert.equal(calls, 1);
});
test('terminal conflict can be explicitly discarded without modifying native chat', async t => {
    const c = await setup(t, async (body, user, res) => res.end(result('kept until explicit discard')));
    const { job } = await c.service.create(c.user, c.input()); await finished(c.service, c.user, job.id);
    await assert.rejects(c.service.discard(c.user, job.id, {}), { code: 'discard_confirmation_required' });
    const before = await fs.readFile(c.chatfile, 'utf8');
    const discarded = await c.service.discard(c.user, job.id, { confirm: true });
    assert.equal(discarded.discarded, true); assert.equal(await fs.readFile(c.chatfile, 'utf8'), before);
});
test('completed results survive process restart; unfinished jobs become interrupted with zero retry', async t => {
    const c = await setup(t, async (body, user, res) => res.end(result('durable')));
    const { job } = await c.service.create(c.user, c.input()); await finished(c.service, c.user, job.id);
    let calls = 0; const fresh = new JobService({ invoke: () => { calls++; } });
    t.after(() => fresh.shutdown());
    assert.equal((await fresh.status(c.user, job.id)).replyText, 'durable');
    const file = path.join(c.root, 'background-jobs', job.id + '.json'), record = JSON.parse(await fs.readFile(file, 'utf8'));
    record.state = 'running'; delete record.wireBase64; record.replyText = ''; await fs.writeFile(file, JSON.stringify(record));
    const crashed = new JobService({ invoke: () => { calls++; } }); t.after(() => crashed.shutdown());
    assert.equal((await crashed.status(c.user, job.id)).state, 'interrupted'); assert.equal(calls, 0);
});
test('terminal status is not published until its result journal is durably committed', async t => {
    const c = await setup(t, async (body, user, res) => res.end(result('durable-after-save')));
    const originalSave = c.service.save.bind(c.service);
    let release, entered;
    const blocked = new Promise(resolve => { release = resolve; });
    const saving = new Promise(resolve => { entered = resolve; });
    c.service.save = async (store, job) => {
        if (job.state === 'completed') { entered(); await blocked; }
        return originalSave(store, job);
    };
    const { job } = await c.service.create(c.user, c.input()); await saving;
    const visible = await c.service.status(c.user, job.id);
    assert.equal(visible.state, 'running'); assert.equal(visible.replyText, '');
    release(); assert.equal((await finished(c.service, c.user, job.id)).state, 'completed');
    const fresh = new JobService({ invoke: () => assert.fail('no retry') }); t.after(() => fresh.shutdown());
    assert.equal((await fresh.status(c.user, job.id)).replyText, 'durable-after-save');
});
test('cross-user job access and unauthenticated access are refused', async t => {
    const c = await setup(t, async (body, user, res) => res.end(result('private result')));
    const { job } = await c.service.create(c.user, c.input());
    await assert.rejects(c.service.status({}, job.id), { code: 'unauthorized' });
    const otherRoot = path.join(c.root, 'other-user');
    await assert.rejects(c.service.status({ directories: { root: otherRoot, chats: path.join(otherRoot, 'chats') } }, job.id), { code: 'job_not_found' });
});
test('full journal rejects new work without deleting an unconsumed result', async t => {
    const c = await setup(t, async (body, user, res) => res.end(result('keep me')), { limits: { maxJobs: 1 } });
    const { job } = await c.service.create(c.user, c.input()); await finished(c.service, c.user, job.id);
    const input = c.input(); input.target = { ...input.target, file: 'second', chatId: 'second' };
    await fs.copyFile(c.chatfile, path.join(path.dirname(c.chatfile), 'second.jsonl'));
    await assert.rejects(c.service.create(c.user, input), { code: 'journal_full' });
    assert.equal((await c.service.status(c.user, job.id)).replyText, 'keep me');
});
test('same chat cannot have a second unconsumed job', async t => {
    const c = await setup(t, async (body, user, res) => res.end(result('ok')));
    await c.service.create(c.user, c.input());
    await assert.rejects(c.service.create(c.user, c.input()), { code: 'chat_busy' });
});
test('native HTTP error keeps live status/body but journals only non-sensitive category', async t => {
    const c = await setup(t, async (body, user, res) => res.status(429).send('SYNTHETIC_SECRET_ERROR_BODY'));
    const { job } = await c.service.create(c.user, c.input()); assert.equal((await finished(c.service, c.user, job.id)).state, 'failed');
    const response = new Response(); await c.service.stream(c.user, job.id, response);
    assert.equal(response.statusCode, 429); assert.equal(response.text, 'SYNTHETIC_SECRET_ERROR_BODY');
    const record = await fs.readFile(path.join(c.root, 'background-jobs', job.id + '.json'), 'utf8');
    assert.equal(record.includes('SYNTHETIC_SECRET_ERROR_BODY'), false);
});
test('expired request IDs cannot be reused after tombstone retention', async t => {
    const c = await setup(t, () => assert.fail('must not invoke'));
    const input = c.input(); input.requestId = `${Date.now() - 8 * 86400_000}-${randomUUID()}`;
    await assert.rejects(c.service.create(c.user, input), { code: 'request_expired' });
});
test('future-clock tolerance cannot expire a tombstone before its request ID expires', async t => {
    let now = Date.now(), calls = 0;
    const c = await setup(t, async (body, user, res) => { calls++; res.end(result('ok')); }, { clock: () => now });
    const input = c.input(); input.requestId = `${now + 240_000}-${randomUUID()}`;
    const { job } = await c.service.create(c.user, input); await finished(c.service, c.user, job.id);
    await c.service.discard(c.user, job.id, { confirm: true });
    now += 7 * 86400_000 + 60_000;
    await c.service.cleanup(await c.service.store(c.user), true);
    const again = await c.service.create(c.user, input);
    assert.equal(again.job.acknowledged, true); assert.equal(calls, 1);
});
test('wire cap reports failure instead of accepting silently truncated output', async t => {
    const c = await setup(t, async (body, user, res) => res.end(result('long'.repeat(200)))), oldLimit = c.service.limits.maxWireBytes;
    c.service.limits.maxWireBytes = 100;
    const { job } = await c.service.create(c.user, c.input());
    assert.equal((await finished(c.service, c.user, job.id)).error.code, 'response_limit');
    c.service.limits.maxWireBytes = oldLimit;
});
test('abandon before a delayed POST fences the request ID with zero upstream calls', async t => {
    let calls = 0;
    const c = await setup(t, async (body, user, res) => { calls++; res.end(result('must not run')); });
    const input = c.input();
    const abandoned = await c.service.abandon(c.user, input.requestId, { target: input.target });
    assert.equal(abandoned.acknowledged, true); assert.equal(abandoned.discarded, true);
    const late = await c.service.create(c.user, input);
    assert.equal(late.created, false); assert.equal(late.job.id, abandoned.id);
    assert.equal(late.job.state, 'acknowledged'); await delay(20); assert.equal(calls, 0);
    const fresh = new JobService({ invoke: () => assert.fail('no retry after restart') }); t.after(() => fresh.shutdown());
    assert.equal((await fresh.create(c.user, input)).job.id, abandoned.id);
});
test('acceptance before abandon returns the original job without cancel or resubmission', async t => {
    let calls = 0, cancelled = false, release;
    const gate = new Promise(resolve => { release = resolve; });
    const c = await setup(t, async (body, user, res, socket) => {
        calls++; socket.on('close', () => { cancelled = true; }); res.write(frame('accepted')); await gate; res.end(ending);
    });
    const input = c.input(), { job } = await c.service.create(c.user, input);
    await delay(20);
    const recovered = await c.service.abandon(c.user, input.requestId, { target: input.target });
    assert.equal(recovered.id, job.id); assert.equal(recovered.acknowledged, false);
    assert.equal(recovered.state, 'running'); assert.equal(cancelled, false);
    assert.equal((await c.service.create(c.user, input)).job.id, job.id);
    release(); assert.equal((await finished(c.service, c.user, job.id)).replyText, 'accepted');
    assert.equal(calls, 1); assert.equal(cancelled, false);
});
