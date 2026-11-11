import { JobService, nativeInvoker, registerRoutes } from './service.mjs';
import { promises as fs } from 'node:fs';
import path from 'node:path';

export const info = {
    id: 'background-jobs', name: 'Background Jobs',
    description: 'Independent durable background generation for native SillyTavern chat completions.',
};
let service;
export async function init(router) {
    const { router: nativeRouter } = await import('../../src/endpoints/backends/chat-completions.js');
    service = new JobService({ invoke: nativeInvoker(nativeRouter) });
    const { getUserDirectoriesList } = await import('../../src/users.js');
    for (const directories of await getUserDirectoriesList()) {
        try { await fs.lstat(path.join(directories.root, 'background-jobs')); }
        catch (error) { if (error.code === 'ENOENT') continue; throw error; }
        try { await service.store({ directories }); }
        catch { console.error('[background-jobs] A user journal could not be loaded; requests for that journal remain blocked.'); }
    }
    registerRoutes(router, service);
    console.info('[background-jobs] Ready; no generation starts without an authenticated request.');
}
export async function exit() { await service?.shutdown(); }
export default { info, init, exit };
