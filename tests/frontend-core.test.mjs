import test from 'node:test';
import assert from 'node:assert/strict';
import { canonical as serverCanonical, chatFingerprint } from '../server/protocol.mjs';
import { API, MARKER, GenerationGate, canonical, eligibility, fingerprint, reconciliation, submitOnce } from '../extension/core.mjs';

const context = () => ({ mainApi: 'openai', characterId: 0, characters: [{ avatar: 'test.png' }], chatId: 'test-chat', chat: [] });
const body = (extra = {}) => ({ type: 'normal', chat_completion_source: 'custom', messages: [{ role: 'user', content: 'synthetic test' }], ...extra });
const wrapper = name => ({ type: 'function', function: { name, parameters: { type: 'object', properties: { [name === 'game_content' ? 'context' : 'content']: { type: 'string' } }, required: [name === 'game_content' ? 'context' : 'content'], additionalProperties: false } } });

test('numeric keys and JSON wire normalization have identical browser/server fingerprints', async () => {
    const messages = [{ mes: 'synthetic', is_user: true, extra: {
        '10': { '100': 'a', '2': 'b' }, '2': 'second',
        time: new Date('2026-01-02T03:04:05Z'), omitted: undefined,
        list: [undefined, { '12': false, '3': true }],
    } }];
    const wire = JSON.parse(JSON.stringify(messages));
    assert.equal(canonical(messages), serverCanonical(wire));
    assert.equal(await fingerprint(messages), chatFingerprint(wire));
    assert.ok(messages[0].extra.time instanceof Date);
    const modified = structuredClone(wire);
    modified[0].extra['10']['2'] = 'changed';
    assert.notEqual(await fingerprint(messages), chatFingerprint(modified));
});

test('fingerprint agrees with backend and only ignores empty reasoning metadata', async () => {
    const original = [{ name: 'Synthetic', mes: 'message', is_user: true, send_date: '2026-01-01', extra: { z: 2, a: 1 } }];
    const native = structuredClone(original); native[0].extra.reasoning = '';
    assert.equal(await fingerprint(original), chatFingerprint(original));
    assert.equal(await fingerprint(original), await fingerprint(native));
    const edited = structuredClone(original); edited[0].extra.a = 9;
    assert.notEqual(await fingerprint(original), await fingerprint(edited));
    assert.equal(await fingerprint([{ mes: 'x' }]), await fingerprint([{ mes: 'x', extra: { reasoning: '' } }]));
    assert.deepEqual(original[0].extra, { z: 2, a: 1 }, 'fingerprinting never edits user chat');
});

test('nested own __proto__, constructor and prototype keys preserve backend canonical/hash parity', async () => {
    const messages = JSON.parse('[{"mes":"synthetic","extra":{"__proto__":{"nested":{"__proto__":"retained","constructor":{"prototype":"nested value"}}},"constructor":{"prototype":{"__proto__":"also retained"}},"prototype":[{"__proto__":{"constructor":"array value"}}]}}]');
    const before = JSON.stringify(messages);
    assert.equal(canonical(messages), serverCanonical(messages));
    assert.equal(await fingerprint(messages), chatFingerprint(messages));
    const roundTrip = JSON.parse(canonical(messages));
    assert.equal(Object.hasOwn(roundTrip[0].extra, '__proto__'), true);
    assert.equal(roundTrip[0].extra.__proto__.nested.__proto__, 'retained');
    assert.equal(roundTrip[0].extra.constructor.prototype.__proto__, 'also retained');
    assert.equal(roundTrip[0].extra.prototype[0].__proto__.constructor, 'array value');
    assert.equal(JSON.stringify(messages), before, 'canonicalization leaves input unchanged');
});

test('only a native normal main prompt with the same message identities arms interception', () => {
    const gate = new GenerationGate(), ctx = context(), request = body();
    gate.begin('normal', {}, false, ctx, true);
    gate.promptReady({ prompt: request.messages }, false);
    const auxiliary = structuredClone(request);
    gate.settingsReady(auxiliary);
    assert.equal(gate.take(ctx, auxiliary), null, 'script clone cannot steal main gate');
    gate.settingsReady({ ...request, messages: [...request.messages] });
    assert.equal(gate.take(ctx, { ...request, type: 'quiet' }), null, 'quiet cannot consume main gate');
    assert.ok(gate.take(ctx, structuredClone(request)), 'native filtered-array request matches');
    assert.equal(gate.take(ctx, request), null, 'at most one submission');
});

test('excluded generation modes, groups, dry runs and another target do not intercept', () => {
    for (const type of ['quiet', 'continue', 'regenerate', 'swipe', 'impersonate']) {
        const gate = new GenerationGate(), ctx = context(), request = body();
        gate.begin(type, {}, false, ctx, true); gate.promptReady({ prompt: request.messages }, false); gate.settingsReady(request);
        assert.equal(gate.take(ctx, request), null);
    }
    for (const [options, dryRun, ctx] of [[{}, true, context()], [{ automatic_trigger: true }, false, context()], [{}, false, { ...context(), groupId: 'group' }]]) {
        const gate = new GenerationGate(), request = body();
        gate.begin('normal', options, dryRun, ctx, true); gate.promptReady({ prompt: request.messages }, false); gate.settingsReady(request);
        assert.equal(gate.take(ctx, request), null);
    }
    const gate = new GenerationGate(), ctx = context(), request = body();
    gate.begin('normal', {}, false, ctx, true); gate.promptReady({ prompt: request.messages }, false); gate.settingsReady(request);
    assert.equal(gate.take({ ...ctx, chatId: 'different' }, request), null);
});

test('only strict single known text wrappers qualify; real tools and body overrides stay native', () => {
    assert.equal(eligibility(body()), null);
    for (const name of ['output_everything_here', 'game_content', 'emit_complete_response']) assert.equal(eligibility(body({ tools: [wrapper(name)] })), null);
    assert.equal(eligibility(body({ tools: [wrapper('browser_search')] })), 'tools');
    assert.equal(eligibility(body({ tools: [wrapper('game_content'), wrapper('emit_complete_response')] })), 'tools');
    assert.equal(eligibility(body({ n: 2 })), 'multichoice');
    assert.equal(eligibility(body({ custom_include_body: 'tools: [unsafe]' })), 'body_overrides');
    assert.equal(eligibility(body({ type: 'quiet' })), 'type');
});

test('completed result appends only to exact baseline and persistent marker deduplicates', async () => {
    const messages = [{ name: 'Test', is_user: true, mes: 'question' }];
    const job = { id: 'j', state: 'completed', baseCount: messages.length, baseHash: await fingerprint(messages), replyText: 'answer' };
    assert.deepEqual(await reconciliation(job, messages), { kind: 'append' });
    assert.equal((await reconciliation(job, [{ ...messages[0], mes: 'edited' }])).kind, 'conflict');
    assert.equal((await reconciliation(job, [...messages, { mes: 'other response' }])).kind, 'conflict');
    const saved = { mes: 'answer', extra: { [MARKER]: 'j' } };
    assert.equal((await reconciliation(job, [...messages, saved])).kind, 'ack');
    assert.equal((await reconciliation(job, [...messages, saved, saved])).reason, 'duplicate_marker');
});

test('a lost submit response recovers by request ID without resending generation', async () => {
    const calls = [], stored = [];
    const result = await submitOnce(async (url, init) => {
        calls.push([url, init?.method]);
        if (init?.method === 'POST') throw new Error('synthetic disconnect after accept');
        return Response.json({ id: 'existing', state: 'running' });
    }, { requestId: 'request-1', target: { avatar: 'x', file: 'y' } }, { remember: id => stored.push(id) });
    assert.equal(result.id, 'existing');
    assert.deepEqual(calls, [[`${API}/jobs`, 'POST'], [`${API}/jobs/by-request/request-1`, undefined]]);
    assert.deepEqual(stored, ['request-1']);
});

test('unknown acceptance and definitive rejection never retry or fall back to a model', async () => {
    let calls = 0;
    await assert.rejects(submitOnce(async (_url, init) => {
        calls++; if (init?.method === 'POST') throw new Error('offline');
        return Response.json({ code: 'missing' }, { status: 404 });
    }, { requestId: 'unknown', target: {} }), { code: 'acceptance_unknown' });
    assert.equal(calls, 2);
    calls = 0;
    await assert.rejects(submitOnce(async () => { calls++; return Response.json({ code: 'ineligible' }, { status: 422 }); }, { requestId: 'bad', target: {} }), { code: 'ineligible' });
    assert.equal(calls, 1);
});
