/** State helpers for the native background generation plugin. */
export const API = '/api/plugins/background-jobs';
export const MARKER = 'background_job_id';
const BODY_WRAPPERS = new Set(['output_everything_here', 'game_content', 'emit_complete_response']);

export function canonical(value) {
    // Match the JSON actually sent to the server, including Date/toJSON and
    // omitted undefined values. Build the ordered string directly: stringify
    // would otherwise reorder integer-looking keys after the lexical sort.
    const json = JSON.stringify(value);
    if (json === undefined) return undefined;
    const encode = item => {
        if (item === null || typeof item !== 'object') return JSON.stringify(item);
        if (Array.isArray(item)) return '[' + item.map(encode).join(',') + ']';
        return '{' + Object.keys(item).sort().map(key => JSON.stringify(key) + ':' + encode(item[key])).join(',') + '}';
    };
    return encode(JSON.parse(json));
}

export async function fingerprint(messages) {
    const normalized = messages.map(message => {
        const result = { ...message };
        if (result.extra && typeof result.extra === 'object' && !Array.isArray(result.extra)) {
            result.extra = { ...result.extra };
            if (result.extra.reasoning === '') delete result.extra.reasoning;
            if (!Object.keys(result.extra).length) delete result.extra;
        }
        return result;
    });
    const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(canonical(normalized)));
    return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

export function targetOf(context) {
    const avatar = context.characters?.[context.characterId]?.avatar;
    const file = context.chatId ?? context.getCurrentChatId?.();
    if (context.groupId || !avatar || !file) return null;
    return { avatar, file, chatId: file };
}

export function sameTarget(left, right) {
    return Boolean(left && right && left.avatar === right.avatar && left.file === right.file);
}

export function eligibility(body) {
    if (!body || !Array.isArray(body.messages)) return 'invalid';
    if (body.type !== 'normal') return 'type';
    if (!['openai', 'custom'].includes(body.chat_completion_source)) return 'source';
    if (body.n !== undefined && body.n !== 1) return 'multichoice';
    if (body.custom_include_body?.trim?.() || body.custom_exclude_body?.trim?.()) return 'body_overrides';
    if (body.functions?.length || body.function_call) return 'tools';
    if (body.tools != null && !Array.isArray(body.tools)) return 'tools';
    if (!body.tools?.length && body.tool_choice && !['none', 'auto'].includes(body.tool_choice)) return 'tools';
    if (body.tools?.length) {
        const [tool] = body.tools;
        if (body.tools.length !== 1 || tool?.type !== 'function' || !BODY_WRAPPERS.has(tool.function?.name)) return 'tools';
        const field = tool.function.name === 'game_content' ? 'context' : 'content';
        const schema = tool.function.parameters;
        if (schema?.type !== 'object' || Object.keys(schema.properties ?? {}).length !== 1
            || schema.properties[field]?.type !== 'string' || !Array.isArray(schema.required)
            || schema.required.length !== 1 || schema.required[0] !== field
            || (schema.additionalProperties !== undefined && schema.additionalProperties !== false)) return 'tools';
        if (body.tool_choice?.function?.name && body.tool_choice.function.name !== tool.function.name) return 'tools';
    }
    return null;
}

export class GenerationGate {
    armed = null;
    begin(type, options, dryRun, context, enabled) {
        if (type !== 'normal' || dryRun || !enabled || context.mainApi !== 'openai'
            || options?.automatic_trigger || options?.quiet_prompt || options?.quietToLoud
            || options?.force_chid !== undefined || !targetOf(context)) {
            return;
        }
        this.armed = { target: targetOf(context), created: Date.now(), prompt: null, parameters: null };
    }
    promptReady(data, dryRun) {
        if (this.armed && !dryRun && Array.isArray(data?.prompt)) this.armed.prompt = data.prompt;
    }
    settingsReady(body) {
        const prompt = this.armed?.prompt?.filter(message => message && typeof message === 'object');
        // ST filters the array but preserves each native main-prompt message.
        // Script-created raw/quiet requests have different message identities.
        if (prompt && body.type === 'normal' && body.messages?.length === prompt.length
            && body.messages.every((message, index) => message === prompt[index])) this.armed.parameters = body;
    }
    take(context, body) {
        const candidate = this.armed;
        if (!candidate?.parameters || body.type !== 'normal' || !sameTarget(candidate.target, targetOf(context))
            || Date.now() - candidate.created > 120000 || canonical(candidate.parameters) !== canonical(body)) return null;
        this.armed = null;
        return candidate;
    }
    clear() { this.armed = null; }
}

export async function reconciliation(job, messages) {
    const markers = messages.filter(message => message?.extra?.[MARKER] === job.id);
    if (markers.length === 1) return { kind: 'ack', message: markers[0] };
    if (markers.length > 1) return { kind: 'conflict', reason: 'duplicate_marker' };
    if (job.state !== 'completed') return { kind: 'wait' };
    if (typeof job.replyText !== 'string' || !job.replyText.length) return { kind: 'conflict', reason: 'no_text' };
    if (messages.length !== job.baseCount || await fingerprint(messages) !== job.baseHash) {
        return { kind: 'conflict', reason: 'chat_changed' };
    }
    return { kind: 'append' };
}

export class JobError extends Error {
    constructor(code, job = null) { super(code); this.name = 'BackgroundJobError'; this.code = code; this.job = job; }
}

/** Submit once. An uncertain response is resolved by idempotency lookup only. */
export async function submitOnce(fetchApi, payload, { remember, accepted } = {}) {
    remember?.(payload.requestId, payload.target);
    let response;
    try {
        response = await fetchApi(`${API}/jobs`, { method: 'POST', body: JSON.stringify(payload) });
    } catch {
        return lookupAccepted();
    }
    let result;
    try { result = await response.json(); } catch { return lookupAccepted(); }
    if ((response.status === 202 || response.status === 200) && result?.id) {
        accepted?.(result);
        return result;
    }
    if (response.status >= 500) return lookupAccepted();
    throw new JobError(result?.code || result?.error?.code || `http_${response.status}`, result?.job);

    async function lookupAccepted() {
        try {
            const lookup = await fetchApi(`${API}/jobs/by-request/${encodeURIComponent(payload.requestId)}`);
            if (lookup.ok) {
                const found = await lookup.json();
                if (found?.id) { accepted?.(found); return found; }
            }
        } catch { /* The durable request ID remains available after reconnect. */ }
        throw new JobError('acceptance_unknown');
    }
}
