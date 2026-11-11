import { createHash } from 'node:crypto';

export const ENVELOPES = Object.freeze({ output_everything_here: 'content', game_content: 'context', emit_complete_response: 'content' });

export class JobError extends Error {
    constructor(code, status = 400) { super(code); this.code = code; this.status = status; }
}

export function canonical(value) {
    if (value === null || typeof value !== 'object') return JSON.stringify(value);
    if (Array.isArray(value)) return '[' + value.map(v => canonical(v) ?? 'null').join(',') + ']';
    return '{' + Object.keys(value).sort().filter(k => value[k] !== undefined)
        .map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
}
export const fingerprint = value => createHash('sha256').update(canonical(value)).digest('hex');
export function normalizeChatMessages(messages) {
    return messages.map(message => {
        const result = { ...message };
        if (result.extra && typeof result.extra === 'object' && !Array.isArray(result.extra)) {
            result.extra = { ...result.extra };
            if (result.extra.reasoning === '') delete result.extra.reasoning;
            if (!Object.keys(result.extra).length) delete result.extra;
        }
        return result;
    });
}
export const chatFingerprint = messages => fingerprint(normalizeChatMessages(messages));

/** Reject ambiguity instead of silently selecting the final duplicate JSON key. */
export function strictJson(text) {
    const value = JSON.parse(text);
    let i = 0;
    const ws = () => { while (/\s/.test(text[i] ?? '') && i < text.length) i++; };
    function string() {
        const begin = i++;
        while (i < text.length) {
            if (text[i] === '\\') { i += 2; continue; }
            if (text[i++] === '"') return JSON.parse(text.slice(begin, i));
        }
        throw new JobError('invalid_json');
    }
    function scan(depth = 0) {
        if (depth > 100) throw new JobError('json_too_deep');
        ws();
        if (text[i] === '"') { string(); return; }
        if (text[i] === '{') {
            i++; ws(); const keys = new Set();
            if (text[i] === '}') { i++; return; }
            for (;;) {
                ws(); const key = string();
                if (keys.has(key)) throw new JobError('duplicate_json_key');
                keys.add(key); ws(); i++; scan(depth + 1); ws();
                if (text[i++] === '}') return;
            }
        }
        if (text[i] === '[') {
            i++; ws(); if (text[i] === ']') { i++; return; }
            for (;;) { scan(depth + 1); ws(); if (text[i++] === ']') return; }
        }
        while (i < text.length && !/[\s,\]}]/.test(text[i])) i++;
    }
    scan();
    return value;
}

export function eligibility(body) {
    if (!body || body.type !== 'normal' || !['openai', 'custom'].includes(body.chat_completion_source)
        || !Array.isArray(body.messages) || (body.n !== undefined && body.n !== 1)
        || (body.functions != null && (!Array.isArray(body.functions) || body.functions.length)) || body.function_call) throw new JobError('ineligible', 422);
    for (const key of ['custom_include_body', 'custom_exclude_body']) {
        if (body[key] != null && (typeof body[key] !== 'string' || body[key].trim())) throw new JobError('ineligible', 422);
    }
    if (body.tools != null && !Array.isArray(body.tools)) throw new JobError('ineligible', 422);
    if (!body.tools?.length) {
        if (body.tool_choice && body.tool_choice !== 'none' && body.tool_choice !== 'auto') throw new JobError('ineligible', 422);
        return null;
    }
    if (!Array.isArray(body.tools) || body.tools.length !== 1) throw new JobError('ineligible', 422);
    const tool = body.tools[0];
    const name = tool.function?.name, field = ENVELOPES[name];
    const schema = tool.function?.parameters;
    if (tool.type !== 'function' || !field || schema?.type !== 'object'
        || !schema.properties || Object.keys(schema.properties).length !== 1
        || schema.properties[field]?.type !== 'string'
        || !Array.isArray(schema.required) || schema.required.length !== 1 || schema.required[0] !== field
        || (schema.additionalProperties !== undefined && schema.additionalProperties !== false)) throw new JobError('ineligible', 422);
    if (body.tool_choice?.function?.name && body.tool_choice.function.name !== name) throw new JobError('ineligible', 422);
    return { name, field };
}

/** Parse a bounded complete native wire response; no invented DONE or partial success. */
export function parseResult(wire, { stream, envelope, status = 200 }) {
    if (status < 200 || status >= 300) throw new JobError('upstream_http_error');
    let text;
    try { text = new TextDecoder('utf-8', { fatal: true }).decode(wire); }
    catch { throw new JobError('invalid_utf8'); }
    let content = '', tool = null, finish = null, done = !stream;
    function consume(data, delta) {
        if (data?.error) throw new JobError('upstream_error');
        if (!Array.isArray(data?.choices)) throw new JobError('invalid_response');
        if (data.choices.length === 0) return; // Usage-only stream frame.
        if (data.choices.length !== 1 || (data.choices[0].index ?? 0) !== 0) throw new JobError('multiple_choices');
        const choice = data.choices[0], part = delta ? choice.delta : choice.message;
        if (!part || typeof part !== 'object') throw new JobError('invalid_response');
        if (part.content != null && typeof part.content !== 'string') throw new JobError('unsupported_content');
        content += part.content ?? '';
        if (part.function_call) throw new JobError('unsupported_tool');
        if (part.tool_calls != null && !Array.isArray(part.tool_calls)) throw new JobError('unsupported_tool');
        if (part.tool_calls?.length) {
            if (!envelope || !Array.isArray(part.tool_calls) || part.tool_calls.length !== 1) throw new JobError('unsupported_tool');
            const call = part.tool_calls[0];
            if ((call.index ?? 0) !== 0 || (call.type && call.type !== 'function')) throw new JobError('multiple_tools');
            if (!tool) tool = { name: '', arguments: '', id: null };
            if (call.id && tool.id && call.id !== tool.id) throw new JobError('multiple_tools');
            if (call.id) tool.id = call.id;
            if (call.function?.name) {
                // A function name may arrive as deltas, or be repeated as a whole.
                const name = tool.name === call.function.name ? tool.name : tool.name + call.function.name;
                if (!envelope.name.startsWith(name)) throw new JobError('multiple_tools');
                tool.name = name;
            }
            if (call.function?.arguments != null && typeof call.function.arguments !== 'string') throw new JobError('invalid_tool_arguments');
            tool.arguments += call.function?.arguments ?? '';
        }
        if (choice.finish_reason != null) {
            if (finish && finish !== choice.finish_reason) throw new JobError('invalid_finish');
            finish = choice.finish_reason;
        }
    }
    try {
        if (stream) {
            for (const event of text.replace(/\r\n/g, '\n').split('\n\n')) {
                const data = event.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5).trimStart()).join('\n');
                if (!data) continue;
                if (done) throw new JobError('data_after_done');
                if (data.trim() === '[DONE]') { done = true; continue; }
                consume(strictJson(data), true);
            }
        } else consume(strictJson(text), false);
    } catch (error) { if (error instanceof JobError) throw error; throw new JobError('invalid_json'); }
    if (!done || !finish) throw new JobError('incomplete_response');
    if (!['stop', 'tool_calls'].includes(finish)) throw new JobError('incomplete_response');
    if (tool) {
        if (content || !envelope || tool.name !== envelope.name || finish !== 'tool_calls') throw new JobError('unsupported_tool');
        let args;
        try { args = strictJson(tool.arguments); } catch { throw new JobError('invalid_tool_arguments'); }
        if (!args || Array.isArray(args) || Object.keys(args).length !== 1 || typeof args[envelope.field] !== 'string') throw new JobError('invalid_tool_arguments');
        content = args[envelope.field];
    } else if (envelope || finish === 'tool_calls') throw new JobError('missing_tool');
    if (!content.trim()) throw new JobError('empty_response');
    return content;
}
