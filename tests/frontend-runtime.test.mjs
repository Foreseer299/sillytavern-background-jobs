import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import * as core from '../extension/core.mjs';

const source = (await readFile(new URL('../extension/index.js', import.meta.url), 'utf8'))
    .replace(/^import .*;$/gm, '')
    .replace(/^\/\/ This extension/m, 'const { API, MARKER, GenerationGate, JobError, canonical, eligibility, fingerprint, reconciliation, sameTarget, submitOnce, targetOf } = __core;\n// This extension');
const tick = () => new Promise(resolve => setTimeout(resolve, 10));

async function fixture({ override, stream = true, user = 'synthetic-account', storage = new Map() } = {}) {
    const listeners = new Map(), calls = [], notifications = [];
    const events = Object.fromEntries(['GENERATION_AFTER_COMMANDS', 'GENERATE_AFTER_DATA', 'CHAT_COMPLETION_SETTINGS_READY', 'MESSAGE_RECEIVED', 'GENERATION_ENDED', 'GENERATION_STOPPED', 'CHAT_CHANGED', 'APP_READY'].map(key => [key, key]));
    const ctx = {
        mainApi: 'openai', characterId: 0, characters: [{ avatar: 'synthetic.png' }], chatId: 'synthetic-chat', name2: 'Synthetic',
        chat: [{ name: 'User', is_user: true, mes: 'synthetic question', send_date: '2026-01-01' }], chatMetadata: {},
        getRequestHeaders: () => ({ 'Content-Type': 'application/json' }), saveSettingsDebounced() {},
        addOneMessage(message) { ctx.rendered = (ctx.rendered ?? []).concat([message]); },
        streamingProcessor: { isFinished: true, isStopped: false, abortController: new AbortController() },
        eventTypes: events, eventSource: { on(event, handler) { listeners.set(event, [...(listeners.get(event) ?? []), handler]); } },
    };
    const wire = 'data: {"choices":[{"index":0,"delta":{"content":"synthetic answer"},"finish_reason":null}]}\n\ndata: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n';
    const job = { id: 'job-1', requestId: 'unknown', target: { avatar: 'synthetic.png', file: 'synthetic-chat', chatId: 'synthetic-chat' }, state: 'completed', baseCount: ctx.chat.length, baseHash: await core.fingerprint(ctx.chat), replyText: 'synthetic answer' };
    let saved = structuredClone(ctx.chat), acked = false;
    const defaultFetch = async (url, init = {}) => {
        if (url === '/api/chats/save') { saved = structuredClone(ctx.chat); return Response.json({}); }
        if (url === '/api/chats/get') return Response.json([{ chat_metadata: {} }, ...saved]);
        if (url === `${core.API}/jobs` && init.method === 'POST') { job.requestId = JSON.parse(init.body).requestId; return Response.json(job, { status: 202 }); }
        if (url === `${core.API}/jobs/job-1/stream`) return new Response(wire, { headers: { 'Content-Type': 'text/event-stream' } });
        if (url === `${core.API}/jobs/job-1/status`) return Response.json(job);
        if (url.startsWith(`${core.API}/jobs?`)) return Response.json({ jobs: acked ? [] : [job] });
        if (url.startsWith(`${core.API}/jobs/by-request/`)) return Response.json(job);
        if (url === `${core.API}/jobs/job-1/ack`) {
            const message = saved.find(item => item.extra?.background_job_id === job.id);
            assert.ok(message, 'ACK must follow a real save containing marker');
            assert.equal(message.mes, JSON.parse(init.body).replyText, 'ACK verifies actual persisted text');
            acked = true; return Response.json({ acknowledged: true });
        }
        if (url === `${core.API}/jobs/job-1/cancel`) return Response.json({ ok: true });
        if (url === '/api/backends/chat-completions/generate') return Response.json({ native: true });
        throw new Error(`Unexpected synthetic URL ${url}`);
    };
    const nativeFetch = async (url, init = {}) => {
        calls.push({ url: String(url), init });
        return override ? override(String(url), init, defaultFetch, { ctx, job, listeners }) : defaultFetch(String(url), init);
    };
    const window = { fetch: nativeFetch, addEventListener() {}, confirm: () => false };
    const sandbox = vm.createContext({
        window, getContext: () => ctx, extension_settings: { background_jobs: { enabled: true } }, currentUser: { handle: user },
        __core: core, location: { href: 'https://synthetic.test/', origin: 'https://synthetic.test' },
        document: { querySelector: () => null, addEventListener() {}, hidden: false },
        localStorage: { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value) },
        navigator: {}, URL, Response, Request, TextEncoder, crypto, structuredClone, console,
        setInterval: () => 1, setTimeout: () => 1,
        toastr: Object.fromEntries(['success', 'warning', 'error', 'info'].map(level => [level, message => notifications.push({ level, message })])),
    });
    vm.runInContext(source, sandbox, { filename: 'background-jobs-index.js' });
    ctx.saveChat = () => window.fetch('/api/chats/save', { method: 'POST', body: new Uint8Array([1, 2, 3]) });
    const emit = async (event, ...args) => { for (const listener of listeners.get(event) ?? []) await listener(...args); };
    const request = { type: 'normal', chat_completion_source: 'custom', messages: [{ role: 'user', content: 'synthetic question' }], stream };
    async function arm() {
        await emit(events.GENERATION_AFTER_COMMANDS, 'normal', {}, false);
        await emit(events.GENERATE_AFTER_DATA, { prompt: request.messages }, false);
        await emit(events.CHAT_COMPLETION_SETTINGS_READY, { ...request, messages: [...request.messages] });
    }
    const send = () => window.fetch('/api/backends/chat-completions/generate', { method: 'POST', body: JSON.stringify(request) });
    return { ctx, job, calls, notifications, window, events, emit, arm, send, request, wire, storage, isAcknowledged: () => acked };
}

test('online response remains exact raw SSE, one model dispatch, persisted marker before ACK', async () => {
    const f = await fixture(); await f.arm();
    const response = await f.send();
    assert.equal(await response.text(), f.wire);
    assert.equal(f.calls.filter(call => call.url === `${core.API}/jobs`).length, 1);
    assert.equal(f.calls.filter(call => call.url === '/api/backends/chat-completions/generate').length, 0);
    assert.ok(f.notifications.some(item => item.message.includes('已交给服务器，可以离开')));
    f.ctx.chat.push({ name: 'Synthetic', is_user: false, mes: 'native transformed answer', extra: {} });
    await f.emit(f.events.MESSAGE_RECEIVED, 1, 'normal');
    assert.equal(f.ctx.chat[1].extra.background_job_id, undefined, 'marker waits for normal native save');
    await f.ctx.saveChat(); await tick(); await tick();
    assert.equal(f.ctx.chat[1].extra.background_job_id, f.job.id);
    assert.equal(f.isAcknowledged(), true);
    await f.emit(f.events.CHAT_CHANGED); await tick();
    assert.equal(f.ctx.chat.length, 2, 'normal online completion is not appended twice');
});

test('failed-stream MESSAGE_RECEIVED never marks or acknowledges partial text', async () => {
    const f = await fixture(); await f.arm(); await f.send();
    f.ctx.streamingProcessor.isFinished = false; f.ctx.streamingProcessor.isStopped = true;
    f.ctx.chat.push({ name: 'Synthetic', is_user: false, mes: 'partial', extra: {} });
    await f.emit(f.events.MESSAGE_RECEIVED, 1, 'normal');
    await f.ctx.saveChat(); await tick();
    await f.emit(f.events.CHAT_CHANGED); await tick();
    assert.equal(f.ctx.chat[1].extra.background_job_id, undefined);
    assert.equal(f.isAcknowledged(), false);
    assert.equal(f.ctx.chat.length, 2, 'conflict preserves partial and complete server result separately');
});

test('reopen restores one completed result through native chat array/render/save and persistent dedupe', async () => {
    const f = await fixture();
    await f.emit(f.events.APP_READY); await tick(); await tick();
    assert.equal(f.ctx.chat.length, 2);
    assert.equal(f.ctx.chat[1].extra.background_job_id, f.job.id);
    assert.equal(f.ctx.rendered.length, 1);
    assert.equal(f.isAcknowledged(), true);
    await f.emit(f.events.CHAT_CHANGED); await tick();
    assert.equal(f.ctx.chat.length, 2);
});

test('tool-call bytes and upstream HTTP status are passed unchanged', async () => {
    const raw = 'data: {"choices":[{"delta":{"tool_calls":[{"function":{"name":"game_content","arguments":"{\\"context\\":\\"原文\\"}"}}]}}]}\n\ndata: [DONE]\n\n';
    const f = await fixture({ override: (url, init, next) => url.endsWith('/stream') ? new Response(raw, { status: 429, headers: { 'Content-Type': 'text/event-stream', 'x-synthetic': 'kept' } }) : next(url, init) });
    await f.arm(); const response = await f.send();
    assert.equal(response.status, 429);
    assert.equal(response.headers.get('x-synthetic'), 'kept');
    assert.equal(await response.text(), raw);
});

test('auxiliary quiet request stays native without consuming the main generation gate', async () => {
    const f = await fixture(); await f.arm();
    const auxiliary = { ...f.request, type: 'quiet' };
    const result = await f.window.fetch('/api/backends/chat-completions/generate', { method: 'POST', body: JSON.stringify(auxiliary) });
    assert.deepEqual(await result.json(), { native: true });
    await f.send();
    assert.equal(f.calls.filter(call => call.url === `${core.API}/jobs`).length, 1);
    assert.equal(f.calls.filter(call => call.url === '/api/backends/chat-completions/generate').length, 1);
});

test('unknown acceptance never retries native generation and is isolated by native account handle', async () => {
    const storage = new Map();
    const f = await fixture({ storage, override: (url, init, next) => {
        if (url === `${core.API}/jobs`) throw new Error('lost accept response');
        if (url.includes('/by-request/')) return Response.json({ code: 'not_found' }, { status: 404 });
        return next(url, init);
    } });
    await f.arm(); await assert.rejects(f.send(), { code: 'acceptance_unknown' });
    assert.equal(f.calls.filter(call => call.url === '/api/backends/chat-completions/generate').length, 0);
    assert.ok([...storage.keys()].some(key => key.includes('synthetic-account')));
    const other = await fixture({ storage, user: 'other-account' });
    await other.arm(); await other.send();
    assert.equal(other.calls.filter(call => call.url === `${core.API}/jobs`).length, 1, 'another account is not blocked by the previous account pending record');
});

test('only explicit native stop cancels; losing stream attachment does not cancel', async () => {
    const f = await fixture(); await f.arm(); await f.send();
    assert.equal(f.calls.some(call => call.url.endsWith('/cancel')), false);
    await f.emit(f.events.GENERATION_STOPPED); await tick();
    assert.equal(f.calls.filter(call => call.url.endsWith('/cancel')).length, 1);
    const lost = await fixture({ override: (url, init, next) => {
        if (url.endsWith('/stream')) throw new Error('synthetic disconnect');
        return next(url, init);
    } });
    await lost.arm(); await assert.rejects(lost.send());
    assert.equal(lost.calls.some(call => call.url.endsWith('/cancel')), false);
});

test('a changed disk chat or metadata prevents recovery from overwriting another tab', async () => {
    for (const metadataOnly of [false, true]) {
        const f = await fixture({ override: (url, init, next, { ctx }) => {
            if (url === '/api/chats/get') return Response.json([
                { chat_metadata: metadataOnly ? { synthetic_variable: 'changed in another tab' } : {} },
                ...ctx.chat.map(message => metadataOnly ? message : { ...message, mes: 'edited in another tab' }),
            ]);
            return next(url, init);
        } });
        await f.emit(f.events.APP_READY); await tick(); await tick();
        assert.equal(f.ctx.chat.length, 1);
        assert.equal(f.calls.some(call => call.url === '/api/chats/save'), false);
        assert.equal(f.isAcknowledged(), false);
    }
});

test('a normal native completion does not receive a marker until the backend confirms complete', async () => {
    const f = await fixture({ override: (url, init, next, { job }) => {
        if (url.endsWith('/status')) return Response.json({ ...job, state: 'running' });
        return next(url, init);
    } });
    await f.arm(); await f.send();
    f.ctx.chat.push({ name: 'Synthetic', is_user: false, mes: 'answer', extra: {} });
    await f.emit(f.events.MESSAGE_RECEIVED, 1, 'normal');
    await f.ctx.saveChat(); await tick(); await tick();
    assert.equal(f.ctx.chat[1].extra.background_job_id, undefined);
    assert.equal(f.isAcknowledged(), false);
});

test('explicit stop while acceptance is pending cancels the eventual accepted job', async () => {
    let entered;
    const waiting = new Promise(resolve => { entered = resolve; });
    let release;
    const held = new Promise(resolve => { release = resolve; });
    const f = await fixture({ override: async (url, init, next) => {
        if (url === `${core.API}/jobs`) { entered(); await held; }
        return next(url, init);
    } });
    await f.arm(); const submission = f.send();
    await waiting; await f.emit(f.events.GENERATION_STOPPED); release();
    await submission; await tick();
    assert.equal(f.calls.filter(call => call.url.endsWith('/cancel')).length, 1);
});
