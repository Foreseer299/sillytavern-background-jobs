import { getContext } from '/scripts/st-context.js';
import { extension_settings } from '/scripts/extensions.js';
import { currentUser } from '/scripts/user.js';
import { API, MARKER, GenerationGate, JobError, canonical, eligibility, fingerprint, reconciliation, sameTarget, submitOnce, targetOf } from './core.mjs';

// This extension owns no provider credentials and never invokes another model.
// ST builds its complete request first; the server owns only the accepted job.
const NAME = 'background_jobs';
const STORAGE = 'garden.background-jobs.requests.v1';
const previousFetch = window.fetch.bind(window);
const gate = new GenerationGate();
const live = new Map();
let jobs = [];
let refreshing = false;
let panel;
let statusText = '尚未接管生成。';
let pollTimer;
let submitting = null;

function settings() {
    extension_settings[NAME] ??= { enabled: false };
    return extension_settings[NAME];
}

function status(message, level = null) {
    statusText = message;
    const element = panel?.querySelector('.bj-status');
    if (element) element.textContent = message;
    if (level && globalThis.toastr?.[level]) globalThis.toastr[level](message, '独立后台生成', { timeOut: 7000, preventDuplicates: true });
}

function pendingRequests() {
    try { return JSON.parse(localStorage.getItem(storageKey()) || '[]').filter(item => item?.requestId && item?.target); }
    catch { return []; }
}

function storageKey() {
    // Immediate local persistence is needed if the tab closes before debounced
    // settings save. The native account handle isolates users on this origin.
    return `${STORAGE}:${encodeURIComponent(currentUser?.handle ?? 'default-user')}`;
}

function remember(requestId, target) {
    const records = pendingRequests().filter(item => item.requestId !== requestId);
    records.push({ requestId, target });
    // Only identifiers, never prompts, results, settings or headers.
    localStorage.setItem(storageKey(), JSON.stringify(records));
}

function forget(requestId) {
    if (!requestId) return;
    localStorage.setItem(storageKey(), JSON.stringify(pendingRequests().filter(item => item.requestId !== requestId)));
}

function apiFetch(url, init = {}) {
    return previousFetch(url, {
        ...init,
        headers: { ...getContext().getRequestHeaders(), ...init.headers },
        credentials: 'same-origin',
        cache: 'no-store',
    });
}

async function readJson(url, init) {
    const response = await apiFetch(url, init);
    const data = await response.json();
    if (!response.ok) throw new JobError(data.code || data.error?.code || `http_${response.status}`, data.job);
    return data;
}

async function persistedChat(target) {
    const response = await apiFetch('/api/chats/get', {
        method: 'POST', body: JSON.stringify({ avatar_url: target.avatar, file_name: target.file }),
    });
    if (!response.ok) throw new JobError('chat_read_failed');
    const messages = await response.json();
    if (!Array.isArray(messages)) throw new JobError('chat_read_failed');
    const header = messages[0];
    const hasHeader = header && (Object.hasOwn(header, 'chat_metadata') || Object.hasOwn(header, 'user_name'));
    return { messages: hasHeader ? messages.slice(1) : messages, metadata: hasHeader ? header.chat_metadata ?? {} : {} };
}

function accepted(job) {
    forget(job.requestId);
    jobs = [...jobs.filter(item => item.id !== job.id), job];
    status('已交给服务器，可以离开。关闭页面、锁屏或断网不会取消；“停止生成”会取消此任务。', 'success');
    renderJobs();
}

function currentLive() {
    const target = targetOf(getContext());
    return [...live.values()].find(item => sameTarget(item.job.target, target));
}

async function interceptGeneration(input, init, body, candidate) {
    const context = getContext();
    const target = targetOf(context);
    if (!sameTarget(target, candidate.target)) throw new JobError('chat_changed');
    if (pendingRequests().some(item => sameTarget(item.target, target))) {
        status('上次提交是否受理尚未确认。请点击“检查并恢复”，确认原任务后再发送。', 'warning');
        throw new JobError('acceptance_unknown');
    }
    // saveChatConditional may swallow save errors; server independently verifies
    // this exact baseline against the persisted chat before accepting the job.
    const submission = { target, stopped: false };
    submitting = submission;
    await context.saveChat();
    if (!sameTarget(target, targetOf(getContext()))) throw new JobError('chat_changed');
    const base = JSON.parse(JSON.stringify(context.chat));
    const requestId = `${Date.now()}-${crypto.randomUUID()}`;
    const payload = { requestId, target, base: { messages: base }, generate: body };
    let job;
    try {
        job = await submitOnce(apiFetch, payload, {
            remember,
            accepted(result) { forget(requestId); accepted(result); },
        });
    } catch (error) {
        if (submitting === submission) submitting = null;
        if (error.code !== 'acceptance_unknown') forget(requestId);
        if (error.job) jobs = [...jobs.filter(item => item.id !== error.job.id), error.job];
        status(error.code === 'acceptance_unknown'
            ? '提交结果尚未确认。已保留请求编号，重新连接后会查询同一任务；请勿重复发送。'
            : error.code === 'chat_busy'
                ? '此聊天已有未处理的后台任务，请先检查并恢复。'
                : '后台任务没有成功启动；此次没有自动重发。请检查任务面板。', 'error');
        renderJobs();
        throw error;
    }
    if (submitting === submission) submitting = null;
    if (job.acknowledged) throw new JobError('already_acknowledged', job);
    const entry = { job, stream: Boolean(body.stream), ended: false, messageSeen: false, stopped: submission.stopped, nativeSaved: false, finalizing: false };
    live.set(job.id, entry);
    if (submission.stopped) {
        try { await readJson(`${API}/jobs/${encodeURIComponent(job.id)}/cancel`, { method: 'POST', body: '{}' }); }
        catch { status('停止请求尚未确认，请在任务面板重试。', 'warning'); }
    }
    try {
        // The stream is passed through byte-for-byte, including tool frames and
        // non-2xx HTTP status. An abort disconnects this reader, not the job.
        return await apiFetch(`${API}/jobs/${encodeURIComponent(job.id)}/stream`, { signal: init?.signal ?? input?.signal });
    } catch (error) {
        live.delete(job.id);
        if (!entry.stopped) status('与页面的连接已断开，服务器任务会继续。重新打开此聊天可恢复。', 'warning');
        throw error;
    }
}

async function acknowledge(job, message) {
    const result = await readJson(`${API}/jobs/${encodeURIComponent(job.id)}/ack`, {
        method: 'POST', body: JSON.stringify({ replyText: message.mes }),
    });
    if (result.acknowledged || result.ok) {
        jobs = jobs.filter(item => item.id !== job.id);
        live.delete(job.id);
        forget(job.requestId);
        status('结果已保存到原聊天。');
        renderJobs();
    }
}

async function acknowledgeSavedRequest() {
    const context = getContext();
    for (const entry of live.values()) {
        if (!entry.messageSeen || !sameTarget(targetOf(context), entry.job.target)) continue;
        entry.nativeSaved = true;
        try { await finalizeNativeMessage(entry); } catch { /* Keep the unacknowledged result. */ }
    }
    for (const message of context.chat) {
        const id = message?.extra?.[MARKER];
        if (!id) continue;
        const entry = live.get(id);
        const job = entry?.job ?? jobs.find(item => item.id === id);
        if (!job || entry?.finalizing || !sameTarget(targetOf(context), job.target)) continue;
        try { await acknowledge(job, message); }
        catch { /* Keep result durable. Server verifies the real saved file. */ }
    }
}

window.fetch = async function backgroundJobsFetch(input, init) {
    let url;
    try { url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url, location.href); }
    catch { return previousFetch(input, init); }
    const method = String(init?.method ?? input?.method ?? 'GET').toUpperCase();
    if (url.origin !== location.origin || method !== 'POST') return previousFetch(input, init);
    const isGeneration = url.pathname === '/api/backends/chat-completions/generate';
    const isSave = url.pathname === '/api/chats/save';
    if (isSave) {
        // Native saveChat may gzip its request. We do not inspect/replace it.
        const response = await previousFetch(input, init);
        if (response.ok) void acknowledgeSavedRequest();
        return response;
    }
    // ST's native callers serialize JSON in init.body. Unknown request forms
    // remain native; cloning a Request can consume an unrelated script's body.
    if (!isGeneration || typeof init?.body !== 'string') return previousFetch(input, init);
    let body;
    try { body = JSON.parse(init.body); } catch { return previousFetch(input, init); }
    if (!settings().enabled) return previousFetch(input, init);
    const candidate = gate.take(getContext(), body);
    if (!candidate) return previousFetch(input, init);
    const reason = eligibility(body);
    if (reason) {
        if (reason === 'tools') status('此请求使用需要浏览器执行的工具，仍由酒馆前台处理；请保持页面打开。', 'info');
        else status('此请求不在独立后台生成的支持范围，仍由酒馆前台处理。');
        return previousFetch(input, init);
    }
    if (new TextEncoder().encode(JSON.stringify({ generate: body, base: { messages: getContext().chat } })).length > 16 * 1024 * 1024) {
        status('此请求包含较大的提示或附件，仍由酒馆前台处理。');
        return previousFetch(input, init);
    }
    return interceptGeneration(input, init, body, candidate);
};

async function markNativeMessage(messageId, type) {
    const entry = currentLive();
    if (!entry || entry.stopped || type !== 'normal') return;
    const context = getContext();
    const processor = context.streamingProcessor;
    if (entry.stream && (!processor?.isFinished || processor.isStopped || processor.abortController?.signal.aborted)) {
        // ST also emits MESSAGE_RECEIVED on a failed stream. That partial reply
        // is never a completed result and must never receive our durable marker.
        live.delete(entry.job.id);
        status('页面接收未完成，完整结果仍由服务器保留。重新连接后会核对聊天内容。', 'warning');
        return;
    }
    const { job } = entry;
    const message = context.chat[messageId];
    if (messageId !== job.baseCount || !message || message.is_user || message.is_system || !message.mes) return;
    if (await fingerprint(context.chat.slice(0, job.baseCount)) !== job.baseHash) {
        status('聊天内容发生变化，结果仍保留在服务器，未自动覆盖。', 'warning');
        return;
    }
    if (!sameTarget(job.target, targetOf(getContext())) || context.chat[messageId] !== message) return;
    entry.messageSeen = true;
    entry.message = message;
    // The successful native save happens after other MESSAGE_RECEIVED handlers
    // finish applying their transformations. Only then may we add our marker.
}

async function finalizeNativeMessage(entry) {
    if (entry.finalizing || !entry.nativeSaved || !entry.messageSeen || entry.stopped) return;
    entry.finalizing = true;
    try {
        const job = await readJson(`${API}/jobs/${encodeURIComponent(entry.job.id)}/status`);
        entry.job = job;
        if (job.state !== 'completed') return;
        const context = getContext();
        if (!sameTarget(job.target, targetOf(context)) || context.chat[job.baseCount] !== entry.message
            || await fingerprint(context.chat.slice(0, job.baseCount)) !== job.baseHash) return;
        const persisted = await persistedChat(job.target);
        if (await fingerprint(persisted.messages) !== await fingerprint(context.chat)
            || canonical(persisted.metadata) !== canonical(context.chatMetadata ?? {})
            || !sameTarget(job.target, targetOf(getContext()))) {
            status('服务器聊天已发生变化，后台结果保留，未覆盖现有内容。', 'warning');
            live.delete(job.id);
            return;
        }
        const message = entry.message;
        message.extra ??= {};
        message.extra[MARKER] = job.id;
        if (message.swipe_info?.[message.swipe_id]?.extra) message.swipe_info[message.swipe_id].extra[MARKER] = job.id;
        await context.saveChat();
        await acknowledge(job, message);
    } finally { entry.finalizing = false; }
}

async function recover(job) {
    const context = getContext();
    if (!sameTarget(job.target, targetOf(context)) || live.has(job.id)) return;
    const action = await reconciliation(job, context.chat);
    if (!sameTarget(job.target, targetOf(getContext()))) return;
    if (action.kind === 'ack') {
        // First let the backend check the marker already on disk. Re-saving an
        // old tab here could overwrite changes made in another browser tab.
        await acknowledge(job, action.message);
        return;
    }
    if (action.kind === 'conflict') {
        job.localConflict = action.reason;
        status('原聊天已有变化，结果保留在面板中。可以查看或复制，未自动插入。');
        return;
    }
    if (action.kind !== 'append') return;
    const persisted = await persistedChat(job.target);
    if (persisted.messages.length !== job.baseCount || await fingerprint(persisted.messages) !== job.baseHash
        || canonical(persisted.metadata) !== canonical(context.chatMetadata ?? {})) {
        job.localConflict = 'disk_chat_changed';
        status('服务器聊天已发生变化，结果保留在面板中，未覆盖其他页面的内容。');
        return;
    }
    // Re-check after async hashing: editing or switching during the check must
    // not append to a different chat or overwrite the new content.
    if (await fingerprint(context.chat) !== job.baseHash || context.chat.length !== job.baseCount
        || !sameTarget(job.target, targetOf(getContext()))) return;
    const message = {
        name: context.name2, is_user: false, is_system: false,
        send_date: new Date().toISOString(), mes: job.replyText,
        gen_started: job.createdAt, gen_finished: job.finishedAt ?? new Date().toISOString(),
        extra: { [MARKER]: job.id, background_recovered: true },
    };
    message.swipe_id = 0;
    message.swipes = [message.mes];
    message.swipe_info = [{ send_date: message.send_date, gen_started: message.gen_started,
        gen_finished: message.gen_finished, extra: { ...message.extra } }];
    context.chat.push(message);
    context.addOneMessage(message);
    // No synthetic MESSAGE_RECEIVED: recovery must not trigger another script's
    // automatic model calls, tool execution, or duplicate native finalization.
    await context.saveChat();
    await acknowledge(job, message);
}

async function refresh() {
    const target = targetOf(getContext());
    if (refreshing || !target) return;
    refreshing = true;
    try {
        for (const pending of pendingRequests().filter(item => sameTarget(item.target, target))) {
            try {
                const found = await readJson(`${API}/jobs/by-request/${encodeURIComponent(pending.requestId)}`);
                forget(pending.requestId);
                if (!found.acknowledged) jobs = [...jobs.filter(item => item.id !== found.id), found];
            } catch { /* A missing response is not proof a prior POST was rejected. */ }
        }
        const result = await readJson(`${API}/jobs?chatId=${encodeURIComponent(target.chatId)}`);
        if (!sameTarget(target, targetOf(getContext()))) return;
        jobs = (result.jobs ?? []).filter(job => sameTarget(job.target, target) && !job.acknowledged);
        for (const job of jobs) {
            const entry = live.get(job.id);
            if (entry) {
                entry.job = job;
                if (entry.messageSeen && entry.nativeSaved) await finalizeNativeMessage(entry);
                if (entry.ended && !entry.messageSeen && !getContext().streamingProcessor
                    && ['completed', 'failed', 'cancelled', 'interrupted'].includes(job.state)) live.delete(job.id);
                if (['failed', 'cancelled', 'interrupted'].includes(job.state) && entry.ended) live.delete(job.id);
            }
            try { await recover(job); }
            catch { status('结果仍保留在服务器，聊天保存尚未确认。可稍后检查并恢复。', 'warning'); }
        }
        if (pendingRequests().some(item => sameTarget(item.target, target))) status('上次提交状态尚未确认，正在保留原请求编号；请勿重复发送。');
        renderJobs();
    } catch { status('暂时无法连接后台任务服务。已受理的任务不会因页面断开而取消。'); }
    finally { refreshing = false; }
}

function button(label, action) {
    const element = document.createElement('button');
    element.type = 'button'; element.className = 'menu_button'; element.textContent = label;
    element.addEventListener('click', () => { void Promise.resolve().then(action).catch(() => status('操作尚未确认，结果仍保留。请重新检查。', 'warning')); });
    return element;
}

function renderJobs() {
    const list = panel?.querySelector('.bj-jobs');
    if (!list) return;
    list.replaceChildren();
    for (const pending of pendingRequests().filter(item => sameTarget(item.target, targetOf(getContext())))) {
        const row = document.createElement('div'); row.className = 'bj-job';
        const text = document.createElement('p');
        text.textContent = '上次提交的受理状态尚未确认。可以让服务器原子核对：已受理就恢复原任务，未受理就阻止迟到的提交并解除等待。';
        row.append(text, button('确认未受理并解除等待', async () => {
            const found = await readJson(`${API}/requests/${encodeURIComponent(pending.requestId)}/abandon`, {
                method: 'POST', body: JSON.stringify({ target: pending.target }),
            });
            if (!found.acknowledged && !found.id) throw new JobError('acceptance_unknown');
            // The server atomically installs a tombstone if absent. A late POST
            // with the same ID cannot start a second generation after this.
            forget(pending.requestId);
            if (!found.acknowledged) {
                jobs = [...jobs.filter(item => item.id !== found.id), found];
                status('已找到先前受理的任务，正在恢复原任务。');
            } else status('服务器已确认并解除等待，可以重新发送。');
            renderJobs(); await refresh();
        }));
        list.append(row);
    }
    const labels = { pending: '等待开始', running: '服务器生成中', completed: '生成完成', failed: '生成失败', cancelled: '已停止', interrupted: '服务器重启后中断' };
    for (const job of jobs.filter(item => sameTarget(item.target, targetOf(getContext())))) {
        const row = document.createElement('div'); row.className = 'bj-job';
        const title = document.createElement('p');
        title.textContent = `${labels[job.state] ?? job.state}${job.localConflict ? ' · 聊天发生变化，未自动插入' : ''}${job.error?.code ? ` · ${job.error.code}` : ''}`;
        row.append(title);
        if (job.replyText) {
            const details = document.createElement('details');
            const summary = document.createElement('summary'); summary.textContent = '查看服务器保存的结果';
            const text = document.createElement('textarea'); text.readOnly = true; text.value = job.replyText;
            details.append(summary, text, button('复制结果', async () => {
                await navigator.clipboard.writeText(job.replyText); status('结果已复制。');
            }));
            row.append(details);
        }
        if (['pending', 'running'].includes(job.state)) row.append(button('停止此任务', async () => {
            await readJson(`${API}/jobs/${encodeURIComponent(job.id)}/cancel`, { method: 'POST', body: '{}' }); await refresh();
        }));
        if (['failed', 'cancelled', 'interrupted'].includes(job.state)) row.append(button('清除此失败记录', async () => {
            await readJson(`${API}/jobs/${encodeURIComponent(job.id)}/ack`, { method: 'POST', body: JSON.stringify({ discard: true }) }); await refresh();
        }));
        if (job.state === 'completed' && job.localConflict) row.append(button('放弃此结果', async () => {
            if (!window.confirm('此结果尚未写回聊天。确认已经复制或不再需要，并从服务器移除此结果？')) return;
            await readJson(`${API}/jobs/${encodeURIComponent(job.id)}/discard`, { method: 'POST', body: JSON.stringify({ confirm: true }) });
            await refresh();
        }));
        list.append(row);
    }
}

function mount() {
    if (panel?.isConnected) return;
    const container = document.querySelector('#extensions_settings2') ?? document.querySelector('#extensions_settings');
    if (!container) return;
    panel = document.createElement('details'); panel.id = 'background-jobs-panel'; panel.open = true;
    const summary = document.createElement('summary'); summary.textContent = '独立后台生成';
    const label = document.createElement('label'); label.className = 'checkbox_label';
    const toggle = document.createElement('input'); toggle.type = 'checkbox'; toggle.checked = Boolean(settings().enabled);
    toggle.addEventListener('change', () => {
        settings().enabled = toggle.checked; gate.clear(); getContext().saveSettingsDebounced();
        status(toggle.checked ? '已启用。只接管普通单角色主回复；请确认柏宝箱后台接管已关闭。' : '已关闭新任务接管，已受理任务和结果继续保留。');
    });
    const text = document.createElement('span'); text.textContent = '启用独立后台生成'; label.append(toggle, text);
    const note = document.createElement('p'); note.textContent = '请勿与柏宝箱后台生成同时启用。只支持普通单角色单条回复；需要执行工具的请求仍使用酒馆前台。';
    const statusElement = document.createElement('p'); statusElement.className = 'bj-status'; statusElement.setAttribute('role', 'status'); statusElement.textContent = statusText;
    const list = document.createElement('div'); list.className = 'bj-jobs';
    panel.append(summary, label, note, statusElement, button('检查并恢复', refresh), list);
    container.append(panel); renderJobs();
}

const context = getContext();
const events = context.eventTypes;
context.eventSource.on(events.GENERATION_AFTER_COMMANDS, (type, options, dryRun) => gate.begin(type, options, dryRun, getContext(), settings().enabled));
context.eventSource.on(events.GENERATE_AFTER_DATA, (data, dryRun) => gate.promptReady(data, dryRun));
context.eventSource.on(events.CHAT_COMPLETION_SETTINGS_READY, body => gate.settingsReady(body));
context.eventSource.on(events.MESSAGE_RECEIVED, markNativeMessage);
context.eventSource.on(events.GENERATION_ENDED, () => {
    gate.clear();
    const entry = currentLive(); if (entry) entry.ended = true;
    setTimeout(() => void refresh(), 500);
});
context.eventSource.on(events.GENERATION_STOPPED, () => {
    gate.clear();
    if (submitting && sameTarget(submitting.target, targetOf(getContext()))) submitting.stopped = true;
    const entry = currentLive();
    if (!entry) return;
    entry.stopped = true;
    void readJson(`${API}/jobs/${encodeURIComponent(entry.job.id)}/cancel`, { method: 'POST', body: '{}' })
        .then(() => { status('已要求服务器停止此任务。'); return refresh(); })
        .catch(() => status('停止请求尚未确认，请在任务面板重试。', 'warning'));
});
context.eventSource.on(events.CHAT_CHANGED, () => {
    gate.clear(); jobs = [];
    for (const [id, entry] of live) if (!sameTarget(entry.job.target, targetOf(getContext()))) live.delete(id);
    mount(); void refresh();
});
context.eventSource.on(events.APP_READY, () => { mount(); void refresh(); });
document.addEventListener('visibilitychange', () => { if (!document.hidden) void refresh(); });
window.addEventListener('online', () => void refresh());
pollTimer = setInterval(() => { if (!document.hidden && (jobs.length || pendingRequests().length)) void refresh(); }, 4000);
// No unload/pagehide/signal listener cancels a server job.
void pollTimer;
mount();
