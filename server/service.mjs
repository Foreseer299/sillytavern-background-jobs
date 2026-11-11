import { EventEmitter } from 'node:events';
import { Writable } from 'node:stream';
import { randomUUID } from 'node:crypto';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { JobError, eligibility, chatFingerprint, fingerprint, parseResult } from './protocol.mjs';

export const LIMITS = Object.freeze({ retentionMs: 7 * 86400_000, maxJobs: 32, maxActive: 4, maxInputBytes: 16 * 1024 * 1024, maxBytes: 128 * 1024 * 1024, maxJobBytes: 6 * 1024 * 1024, maxWireBytes: 2 * 1024 * 1024, timeoutMs: 15 * 60_000 });
const ACTIVE = new Set(['pending', 'running']);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const REQUEST_ID = /^(\d{13})-([0-9a-f-]{36})$/i;

function validateRequestId(requestId, now, retentionMs) {
    const match = REQUEST_ID.exec(requestId ?? '');
    if (!match || !UUID.test(match[2])) throw new JobError('invalid_request_id');
    const age = now - Number(match[1]);
    if (age > retentionMs) throw new JobError('request_expired', 410);
    if (age < -300_000) throw new JobError('request_from_future');
}

/** A native response sink with its own socket, unrelated to any browser connection. */
export class NativeCapture extends Writable {
    constructor({ stream, onChunk, onFailure, socket }) {
        super(); this.statusCode = 200; this.statusMessage = ''; this.headersSent = false;
        this.headers = { 'content-type': stream ? 'text/event-stream; charset=utf-8' : 'application/json; charset=utf-8' };
        this.socket = socket; this.onChunk = onChunk; this.onFailure = onFailure;
        this.on('pipe', source => source.on('error', () => { onFailure('upstream_stream_error'); this.end(); }));
        this.on('error', () => onFailure('response_sink_error'));
    }
    _write(chunk, encoding, callback) {
        this.headersSent = true;
        try { this.onChunk(Buffer.from(chunk), this); callback(); }
        catch { this.onFailure('response_limit'); callback(); this.socket.emit('close'); this.end(); }
    }
    status(code) { this.statusCode = code; return this; }
    setHeader(name, value) { this.headers[String(name).toLowerCase()] = String(value); return this; }
    getHeader(name) { return this.headers[String(name).toLowerCase()]; }
    removeHeader(name) { delete this.headers[String(name).toLowerCase()]; }
    set(name, value) { return this.setHeader(name, value); }
    type(value) { return this.setHeader('content-type', value); }
    flushHeaders() { this.headersSent = true; this.emit('ready'); }
    send(body) { if (body && typeof body === 'object' && !Buffer.isBuffer(body)) body = JSON.stringify(body); this.end(body ?? ''); return this; }
    json(body) { return this.send(body); }
    sendStatus(code) { return this.status(code).send(String(code)); }
}

/** Invoke only the native generation handler; no secret handling or upstream HTTP code is copied. */
export function nativeInvoker(router) {
    const route = router?.stack?.find(layer => layer.route?.path === '/generate' && layer.route.methods?.post)?.route;
    if (!route || route.stack.length !== 1 || typeof route.stack[0].handle !== 'function') throw new Error('Unsupported native /generate route');
    const handler = route.stack[0].handle;
    return (body, user, capture, socket) => {
        const request = new EventEmitter();
        Object.assign(request, { body, user, socket, connection: socket, method: 'POST', url: '/generate', headers: {} });
        return handler(request, capture);
    };
}

function validTarget(target) {
    if (!target || !['avatar', 'file', 'chatId'].every(k => typeof target[k] === 'string' && target[k].length > 0 && target[k].length <= 255)
        || !target.avatar.endsWith('.png') || [target.avatar, target.file].some(s => /[\/\\\0]/.test(s) || s === '.' || s === '..')) throw new JobError('invalid_target');
    return { avatar: target.avatar, file: target.file, chatId: target.chatId };
}
const targetKey = target => JSON.stringify([target.avatar, target.file]);
const publicJob = job => ({
    id: job.id, requestId: job.requestId, state: job.state, target: job.target,
    createdAt: job.createdAt, updatedAt: job.updatedAt, baseHash: job.baseHash, baseCount: job.baseCount,
    replyText: job.replyText ?? '', httpStatus: job.httpStatus ?? null, error: job.error ?? null,
    acknowledged: Boolean(job.acknowledged), discarded: Boolean(job.discarded), acknowledgedTextHash: job.acknowledgedTextHash ?? null,
    expired: Date.now() - job.createdAt > LIMITS.retentionMs,
});

export class JobService {
    constructor({ invoke, limits = {}, clock = Date.now } = {}) {
        this.invoke = invoke; this.limits = { ...LIMITS, ...limits }; this.clock = clock;
        this.stores = new Map(); this.closed = false;
    }
    async store(user) {
        if (!user?.directories?.root || !user?.directories?.chats) throw new JobError('unauthorized', 401);
        const key = path.resolve(user.directories.root);
        if (!this.stores.has(key)) this.stores.set(key, this.loadStore(key, user.directories.chats));
        return this.stores.get(key);
    }
    async loadStore(root, chats) {
        const directory = path.join(root, 'background-jobs');
        await fs.mkdir(directory, { recursive: true, mode: 0o700 });
        const stat = await fs.lstat(directory);
        if (!stat.isDirectory() || stat.isSymbolicLink()) throw new JobError('unsafe_journal_directory', 503);
        await fs.chmod(directory, 0o700);
        const store = { directory, chats: path.resolve(chats), jobs: new Map(), runtimes: new Map(), lock: Promise.resolve() };
        for (const file of await fs.readdir(directory)) {
            if (!file.endsWith('.json') || !UUID.test(file.slice(0, -5))) continue;
            const filePath = path.join(directory, file), stat = await fs.lstat(filePath);
            if (!stat.isFile() || stat.isSymbolicLink() || stat.size > this.limits.maxJobBytes) throw new JobError('invalid_journal', 503);
            let job;
            try { job = JSON.parse(await fs.readFile(filePath, 'utf8')); } catch { throw new JobError('invalid_journal', 503); }
            if (job.version !== 1 || job.id !== file.slice(0, -5) || !REQUEST_ID.test(job.requestId)) throw new JobError('invalid_journal', 503);
            store.jobs.set(job.id, job);
            if (ACTIVE.has(job.state)) {
                job.state = 'interrupted'; job.error = { code: 'server_restarted' }; job.updatedAt = this.clock();
                delete job.wireBase64; await this.save(store, job);
            }
        }
        await this.cleanup(store);
        return store;
    }
    async locked(store, fn) {
        const task = store.lock.then(fn); store.lock = task.catch(() => {}); return task;
    }
    async save(store, job) {
        const data = JSON.stringify(job);
        if (Buffer.byteLength(data) > this.limits.maxJobBytes) throw new JobError('result_too_large', 507);
        const temporary = path.join(store.directory, `.${job.id}.${randomUUID()}.tmp`);
        let handle;
        try {
            handle = await fs.open(temporary, 'wx', 0o600); await handle.writeFile(data); await handle.sync(); await handle.close(); handle = null;
            await fs.rename(temporary, path.join(store.directory, job.id + '.json'));
            const directory = await fs.open(store.directory, 'r'); try { await directory.sync(); } finally { await directory.close(); }
        } finally { if (handle) await handle.close(); await fs.unlink(temporary).catch(() => {}); }
    }
    async cleanup(store, reclaim = false) {
        const now = this.clock();
        for (const job of store.jobs.values()) {
            if (!job.acknowledged) continue; // Unconsumed results are never silently deleted.
            // An allowed future client timestamp must not outlive its tombstone.
            const retentionStart = Math.max(job.createdAt, Number(job.requestId.split('-')[0]));
            if (now - retentionStart > this.limits.retentionMs) {
                await fs.unlink(path.join(store.directory, job.id + '.json')); store.jobs.delete(job.id);
            } else if (reclaim && job.state !== 'acknowledged') {
                const next = { ...job, state: 'acknowledged' }; delete next.replyText; delete next.wireBase64;
                await this.save(store, next); store.jobs.set(job.id, next);
            }
        }
    }
    usage(store) {
        let bytes = 0, jobs = 0;
        for (const job of store.jobs.values()) {
            bytes += ACTIVE.has(job.state) ? this.limits.maxJobBytes : Buffer.byteLength(JSON.stringify(job));
            if (!job.acknowledged) jobs++;
        }
        return { bytes, jobs };
    }
    async readChat(store, target) {
        const filename = path.join(store.chats, target.avatar.replace('.png', ''), target.file + '.jsonl');
        const relative = path.relative(store.chats, filename);
        if (relative.startsWith('..') || path.isAbsolute(relative)) throw new JobError('invalid_target');
        let content;
        try {
            const real = await fs.realpath(filename), parent = await fs.realpath(store.chats);
            if (path.relative(parent, real).startsWith('..')) throw new JobError('invalid_target');
            const stat = await fs.stat(real);
            if (stat.size > 32 * 1024 * 1024) throw new JobError('chat_too_large', 409);
            content = await fs.readFile(real, 'utf8');
        } catch (error) { if (error.code === 'ENOENT') throw new JobError('chat_not_saved', 409); throw error; }
        let rows;
        try { rows = content.split('\n').filter(line => line.trim()).map(line => JSON.parse(line)); }
        catch { throw new JobError('invalid_chat_file', 409); }
        // Native /chats/save writes its metadata header before context.chat.
        if (rows.length && (Object.hasOwn(rows[0], 'chat_metadata') || Object.hasOwn(rows[0], 'user_name'))) rows.shift();
        return rows;
    }
    async create(user, input) {
        if (this.closed) throw new JobError('server_stopping', 503);
        validateRequestId(input?.requestId, this.clock(), this.limits.retentionMs);
        const target = validTarget(input.target), store = await this.store(user);
        return this.locked(store, async () => {
            const previous = [...store.jobs.values()].find(job => job.requestId === input.requestId);
            if (previous) {
                if (targetKey(previous.target) !== targetKey(target)) throw new JobError('request_id_conflict', 409);
                return { created: false, job: publicJob(previous) };
            }
            if (Buffer.byteLength(JSON.stringify({ generate: input.generate, base: input.base })) > this.limits.maxInputBytes) throw new JobError('ineligible', 422);
            const envelope = eligibility(input.generate);
            if (!Array.isArray(input.base?.messages) || !input.base.messages.length) throw new JobError('invalid_base');
            const baseHash = chatFingerprint(input.base.messages);
            if (input.base.hash && input.base.hash !== baseHash) throw new JobError('base_hash_mismatch', 409);
            const existing = [...store.jobs.values()].find(job => !job.acknowledged && targetKey(job.target) === targetKey(target));
            if (existing) { const error = new JobError('chat_busy', 409); error.job = publicJob(existing); throw error; }
            const saved = await this.readChat(store, target);
            if (chatFingerprint(saved) !== baseHash) throw new JobError('chat_changed', 409);
            await this.cleanup(store, true);
            const usage = this.usage(store);
            if ([...store.jobs.values()].filter(job => ACTIVE.has(job.state)).length >= this.limits.maxActive) throw new JobError('active_limit', 429);
            if (usage.jobs >= this.limits.maxJobs || usage.bytes + this.limits.maxJobBytes > this.limits.maxBytes) throw new JobError('journal_full', 429);
            const now = this.clock();
            const job = { version: 1, id: randomUUID(), requestId: input.requestId, target, baseHash, baseCount: saved.length,
                createdAt: now, updatedAt: now, state: 'pending', acknowledged: false, httpStatus: null, replyText: '', error: null };
            await this.save(store, job); // No upstream work starts until durable acceptance succeeds.
            store.jobs.set(job.id, job);
            const body = structuredClone(input.generate);
            setImmediate(() => this.run(store, job, body, user, envelope).catch(() => {}));
            return { created: true, job: publicJob(job) };
        });
    }
    async run(store, job, body, user, envelope) {
        if (!ACTIVE.has(job.state)) return;
        const socket = new EventEmitter(), events = new EventEmitter();
        const runtime = { socket, events, chunks: [], bytes: 0, ready: false, finished: false, failure: null };
        store.runtimes.set(job.id, runtime);
        const capture = new NativeCapture({ stream: Boolean(body.stream), socket,
            onChunk: (chunk, sink) => {
                if (runtime.bytes + chunk.length > this.limits.maxWireBytes) throw new JobError('response_limit');
                runtime.chunks.push(chunk); runtime.bytes += chunk.length;
                job.httpStatus = sink.statusCode; runtime.ready = true; events.emit('ready'); events.emit('chunk', chunk);
            }, onFailure: code => { runtime.failure ??= code; } });
        runtime.capture = capture;
        const finish = async () => {
            if (runtime.finished) return;
            runtime.finished = true; clearTimeout(runtime.timer);
            await this.locked(store, async () => {
                if (ACTIVE.has(job.state)) {
                    const next = { ...job, httpStatus: capture.statusCode };
                    try {
                        if (runtime.failure) throw new JobError(runtime.failure);
                        next.replyText = parseResult(Buffer.concat(runtime.chunks), { stream: Boolean(body.stream), envelope, status: capture.statusCode });
                        next.wireBase64 = Buffer.concat(runtime.chunks).toString('base64');
                        next.contentType = capture.getHeader('content-type'); next.state = 'completed';
                    } catch (error) { next.state = 'failed'; next.error = { code: error.code ?? 'invalid_response' }; next.replyText = ''; }
                    next.updatedAt = this.clock();
                    try { await this.save(store, next); Object.assign(job, next); }
                    catch { job.state = 'failed'; job.error = { code: 'journal_write_failed' }; delete job.wireBase64; job.replyText = ''; }
                }
                runtime.ready = true; events.emit('ready'); events.emit('done');
                // Failed raw responses may contain echoed secrets; never journal them.
                if (job.state === 'completed') store.runtimes.delete(job.id);
            });
            body = null; user = null;
        };
        capture.once('finish', () => { void finish().catch(() => {
            // EventEmitter does not observe async listener rejections.
            job.state = 'failed'; job.error = { code: 'completion_handler_error' }; job.replyText = '';
            delete job.wireBase64; runtime.finished = true; runtime.ready = true;
            clearTimeout(runtime.timer); body = null; user = null;
            try { events.emit('ready'); events.emit('done'); } catch { /* A delivery error cannot crash generation. */ }
        }); });
        runtime.timer = setTimeout(() => { runtime.failure = 'generation_timeout'; socket.emit('close'); capture.end(); }, this.limits.timeoutMs);
        runtime.timer.unref?.();
        try { await this.locked(store, async () => {
            if (!ACTIVE.has(job.state)) return;
            job.state = 'running'; job.updatedAt = this.clock(); await this.save(store, job);
        }); }
        catch { runtime.failure = 'journal_write_failed'; capture.end(); return; }
        if (!ACTIVE.has(job.state) || this.closed || runtime.finished || runtime.failure || capture.writableEnded) { capture.end(); return; }
        try { await this.invoke(body, user, capture, socket); }
        catch { runtime.failure = 'native_handler_error'; capture.end(); }
    }
    async lookup(user, id) {
        const store = await this.store(user), job = store.jobs.get(id);
        if (!job) throw new JobError('job_not_found', 404);
        return { store, job };
    }
    async status(user, id) { return publicJob((await this.lookup(user, id)).job); }
    async byRequest(user, requestId) {
        const store = await this.store(user), job = [...store.jobs.values()].find(job => job.requestId === requestId);
        if (!job) throw new JobError('job_not_found', 404);
        return publicJob(job);
    }
    async abandon(user, requestId, input) {
        if (this.closed) throw new JobError('server_stopping', 503);
        validateRequestId(requestId, this.clock(), this.limits.retentionMs);
        const target = validTarget(input?.target), store = await this.store(user);
        return this.locked(store, async () => {
            const existing = [...store.jobs.values()].find(job => job.requestId === requestId);
            if (existing) {
                if (targetKey(existing.target) !== targetKey(target)) throw new JobError('request_id_conflict', 409);
                return publicJob(existing); // Already accepted: recover it, never cancel or resubmit it.
            }
            await this.cleanup(store, true);
            const now = this.clock();
            const tombstone = { version: 1, id: randomUUID(), requestId, target, baseHash: null, baseCount: 0,
                createdAt: now, updatedAt: now, state: 'acknowledged', acknowledged: true, discarded: true,
                httpStatus: null, error: { code: 'request_abandoned_before_acceptance' } };
            if (this.usage(store).bytes + Buffer.byteLength(JSON.stringify(tombstone)) > this.limits.maxBytes) throw new JobError('journal_full', 429);
            await this.save(store, tombstone);
            store.jobs.set(tombstone.id, tombstone);
            return publicJob(tombstone);
        });
    }
    async list(user, chatId) {
        const store = await this.store(user);
        return { jobs: [...store.jobs.values()].filter(job => !job.acknowledged && (!chatId || job.target.chatId === chatId)).map(publicJob) };
    }
    async cancel(user, id) {
        const { store, job } = await this.lookup(user, id);
        return this.locked(store, async () => {
            if (ACTIVE.has(job.state)) {
                job.state = 'cancelled'; job.error = { code: 'cancelled_by_user' }; job.updatedAt = this.clock();
                const runtime = store.runtimes.get(id);
                try { await this.save(store, job); }
                finally { if (runtime) { runtime.socket.emit('close'); runtime.capture.end(); } }
            }
            return publicJob(job);
        });
    }
    async ack(user, id, input) {
        const { store, job } = await this.lookup(user, id);
        return this.locked(store, async () => {
            if (job.acknowledged) return publicJob(job);
            if (ACTIVE.has(job.state)) throw new JobError('job_not_finished', 409);
            if (job.state === 'completed') {
                if (typeof input?.replyText !== 'string' || !input.replyText.trim()) throw new JobError('invalid_ack');
                const rows = await this.readChat(store, job.target);
                const matches = rows.map((row, i) => row?.extra?.background_job_id === job.id ? i : -1).filter(i => i >= 0);
                if (matches.length !== 1 || matches[0] !== job.baseCount) throw new JobError('chat_marker_conflict', 409);
                const saved = rows[matches[0]];
                if (saved.is_user !== false || saved.mes !== input.replyText || chatFingerprint(rows.slice(0, job.baseCount)) !== job.baseHash) throw new JobError('chat_save_conflict', 409);
                input = { ...input, textHash: fingerprint(input.replyText) };
            } else if (input?.discard !== true) throw new JobError('discard_required', 409);
            const next = { ...job, acknowledged: true, updatedAt: this.clock(), ...(input.textHash ? { acknowledgedTextHash: input.textHash } : {}) };
            await this.save(store, next); store.jobs.set(id, next); store.runtimes.delete(job.id);
            return publicJob(next);
        });
    }
    async discard(user, id, input) {
        if (input?.confirm !== true) throw new JobError('discard_confirmation_required');
        const { store, job } = await this.lookup(user, id);
        return this.locked(store, async () => {
            if (ACTIVE.has(job.state)) throw new JobError('job_not_finished', 409);
            const next = { ...job, state: 'acknowledged', acknowledged: true, discarded: true, updatedAt: this.clock() };
            delete next.wireBase64; delete next.replyText;
            await this.save(store, next); store.jobs.set(id, next); store.runtimes.delete(id);
            return publicJob(next);
        });
    }
    async stream(user, id, response) {
        const { store, job } = await this.lookup(user, id);
        let runtime = store.runtimes.get(id);
        if (ACTIVE.has(job.state) && !runtime) {
            await new Promise(resolve => setImmediate(resolve)); runtime = store.runtimes.get(id);
        }
        if (runtime && !runtime.ready) await new Promise(resolve => {
            const ready = () => { runtime.events.off('ready', ready); response.off('close', ready); resolve(); };
            runtime.events.once('ready', ready); response.once('close', ready);
        });
        if (response.destroyed) return;
        response.statusCode = job.httpStatus ?? (job.state === 'cancelled' ? 409 : 200);
        response.setHeader('Cache-Control', 'no-store'); response.setHeader('X-Accel-Buffering', 'no');
        response.setHeader('Content-Type', job.contentType ?? runtime?.capture?.getHeader('content-type') ?? 'application/json; charset=utf-8');
        if (job.wireBase64) { response.end(Buffer.from(job.wireBase64, 'base64')); return; }
        if (!runtime) {
            response.statusCode = job.state === 'acknowledged' ? 410 : 409;
            response.end(JSON.stringify({ error: { code: job.error?.code ?? 'wire_unavailable' } })); return;
        }
        for (const chunk of runtime.chunks) response.write(chunk);
        if (runtime.finished || !ACTIVE.has(job.state)) { response.end(); return; }
        const onChunk = chunk => { if (!response.destroyed && !response.writableEnded) response.write(chunk); };
        const onDone = () => { detach(); if (!response.destroyed && !response.writableEnded) response.end(); };
        const detach = () => { runtime.events.off('chunk', onChunk); runtime.events.off('done', onDone); };
        runtime.events.on('chunk', onChunk); runtime.events.once('done', onDone);
        response.once('close', detach); response.once('error', detach); // Delivery disconnect never touches generation's socket.
    }
    async shutdown() {
        this.closed = true;
        for (const promise of this.stores.values()) {
            const store = await promise;
            await this.locked(store, async () => {
                for (const job of store.jobs.values()) if (ACTIVE.has(job.state)) {
                    job.state = 'interrupted'; job.error = { code: 'server_stopped' }; job.updatedAt = this.clock();
                    const runtime = store.runtimes.get(job.id);
                    try { await this.save(store, job); }
                    catch { job.error = { code: 'shutdown_journal_write_failed' }; }
                    finally { if (runtime) { runtime.socket.emit('close'); runtime.capture.end(); } }
                }
            });
        }
    }
}

export function registerRoutes(router, service) {
    const wrap = handler => async (request, response) => {
        try { await handler(request, response); }
        catch (error) {
            if (!response.headersSent) response.status(error.status ?? 500).json({ code: error instanceof JobError ? error.code : 'internal_error', ...(error.job ? { job: error.job } : {}) });
            else response.end();
        }
    };
    router.post('/jobs', wrap(async (req, res) => { const result = await service.create(req.user, req.body); res.status(result.created ? 202 : 200).json(result.job); }));
    router.get('/jobs', wrap(async (req, res) => res.json(await service.list(req.user, req.query.chatId))));
    router.get('/jobs/by-request/:requestId', wrap(async (req, res) => res.json(await service.byRequest(req.user, req.params.requestId))));
    router.get('/jobs/:id/status', wrap(async (req, res) => res.json(await service.status(req.user, req.params.id))));
    router.get('/jobs/:id/stream', wrap(async (req, res) => service.stream(req.user, req.params.id, res)));
    router.post('/jobs/:id/cancel', wrap(async (req, res) => res.json(await service.cancel(req.user, req.params.id))));
    router.post('/jobs/:id/ack', wrap(async (req, res) => res.json(await service.ack(req.user, req.params.id, req.body))));
    router.post('/jobs/:id/discard', wrap(async (req, res) => res.json(await service.discard(req.user, req.params.id, req.body))));
    router.post('/requests/:requestId/abandon', wrap(async (req, res) => res.json(await service.abandon(req.user, req.params.requestId, req.body))));
}
