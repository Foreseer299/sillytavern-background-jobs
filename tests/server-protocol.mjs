import test from 'node:test';
import assert from 'node:assert/strict';
import { chatFingerprint, strictJson, eligibility, parseResult } from '../server/protocol.mjs';

const normal = { type: 'normal', chat_completion_source: 'custom', messages: [{ role: 'user', content: 'synthetic' }], stream: true, n: 1 };
const tool = (name, field) => ({ ...normal, tools: [{ type: 'function', function: { name, parameters: { type: 'object', properties: { [field]: { type: 'string' } }, required: [field], additionalProperties: false } } }] });
const frame = data => 'data: ' + JSON.stringify(data) + '\n\n';
const delta = (part, finish_reason = null) => ({ choices: [{ index: 0, delta: part, finish_reason }] });
const wire = text => Buffer.from(text);

test('fingerprint ignores only empty default extra.reasoning and empty extra', () => {
    const base = [{ name: 'User', is_user: true, mes: 'hello', send_date: 'today' }];
    assert.equal(chatFingerprint(base), chatFingerprint([{ ...base[0], extra: { reasoning: '' } }]));
    assert.equal(chatFingerprint(base), chatFingerprint([{ ...base[0], extra: {} }]));
    for (const change of [{ mes: 'changed' }, { send_date: 'tomorrow' }, { name: 'Other' }, { extra: { reasoning: 'nonempty' } }, { extra: { custom: 1 } }, { swipe_id: 1 }]) {
        assert.notEqual(chatFingerprint(base), chatFingerprint([{ ...base[0], ...change }]));
    }
});
test('strict JSON rejects duplicates at any depth and escaped duplicate names', () => {
    for (const text of ['{"a":1,"a":2}', '{"x":{"a":1,"\\u0061":2}}', '[{"a":1,"a":2}]']) assert.throws(() => strictJson(text));
    assert.deepEqual(strictJson('{"a":"\\\"", "x":[1,true,null]}'), { a: '"', x: [1, true, null] });
});
test('eligibility accepts normal and three narrowly specified envelope schemas', () => {
    assert.equal(eligibility(normal), null);
    for (const [name, field] of [['output_everything_here', 'content'], ['game_content', 'context'], ['emit_complete_response', 'content']]) {
        assert.deepEqual(eligibility(tool(name, field)), { name, field });
    }
});
test('eligibility rejects other sources/types, custom YAML body, unknown/multiple/malformed tools', () => {
    for (const change of [{ type: 'continue' }, { chat_completion_source: 'claude' }, { n: 2 }, { custom_include_body: 'n: 2' }, { custom_exclude_body: 'tools' }, { tools: {} }, { tools: tool('unknown', 'content').tools }, { tools: [tool('game_content', 'context').tools[0], tool('game_content', 'context').tools[0]] }]) {
        assert.throws(() => eligibility({ ...normal, ...change }), { code: 'ineligible' });
    }
});
test('normal SSE parses text, unicode, comments, usage and terminal DONE', () => {
    const response = ': keepalive\n\n' + frame(delta({ content: '中文😀' })) + frame(delta({}, 'stop')) + frame({ choices: [], usage: { total_tokens: 2 } }) + 'data: [DONE]\n\n';
    assert.equal(parseResult(wire(response), { stream: true }), '中文😀');
});
test('nonstream normal is parsed without rewriting raw native response', () => {
    assert.equal(parseResult(wire(JSON.stringify({ choices: [{ message: { content: 'ok' }, finish_reason: 'stop' }] })), {}), 'ok');
});
test('tool envelope arguments spanning chunks decode only after complete finish', () => {
    const envelope = eligibility(tool('game_content', 'context'));
    const response = frame(delta({ tool_calls: [{ index: 0, id: 'abc', type: 'function', function: { name: 'game_content', arguments: '{"context":"中' } }] }))
        + frame(delta({ tool_calls: [{ index: 0, function: { arguments: '文\\n😀"}' } }] }, 'tool_calls')) + 'data: [DONE]\n\n';
    assert.equal(parseResult(wire(response), { stream: true, envelope }), '中文\n😀');
});
test('truncated, filtered, multiple choice, errors and malformed tools cannot become completed', () => {
    for (const response of [
        frame(delta({ content: 'partial' })),
        frame(delta({ content: 'partial' }, 'length')) + 'data: [DONE]\n\n',
        frame({ error: { message: 'secret-synthetic' } }) + 'data: [DONE]\n\n',
        frame({ choices: [{ index: 0, delta: { content: 'a' } }, { index: 1, delta: { content: 'b' } }] }) + 'data: [DONE]\n\n',
        frame(delta({ content: 'a', tool_calls: {} }, 'stop')) + 'data: [DONE]\n\n',
    ]) assert.throws(() => parseResult(wire(response), { stream: true }));
});
test('tool envelopes reject duplicate keys, unknown field, extra fields and wrong name', () => {
    const envelope = eligibility(tool('output_everything_here', 'content'));
    for (const args of ['{"content":"a","content":"b"}', '{"other":"a"}', '{"content":"a","extra":1}', '{"content":']) {
        const response = { choices: [{ message: { tool_calls: [{ type: 'function', function: { name: envelope.name, arguments: args } }] }, finish_reason: 'tool_calls' }] };
        assert.throws(() => parseResult(wire(JSON.stringify(response)), { envelope }));
    }
});
