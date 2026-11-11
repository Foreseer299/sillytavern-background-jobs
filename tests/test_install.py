import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.package = self.base / 'package'
        for d in ['scripts', 'server', 'extension']:
            (self.package / d).mkdir(parents=True)
        shutil.copyfile(Path(__file__).resolve().parents[1] / 'scripts/install.py', self.package / 'scripts/install.py')
        (self.package / 'server/index.mjs').write_text('export const info = {};\n')
        (self.package / 'extension/manifest.json').write_text('{"js":"index.js"}\n')
        (self.package / 'extension/index.js').write_text('// Independent fixture\n')
        self.app = self.base / 'app'
        self.app.mkdir()
        (self.app / 'server.js').write_text('native core\n')
        (self.app / 'data').mkdir()
        (self.app / 'data/chat.jsonl').write_text('important user data\n')

    def tearDown(self):
        self.tmp.cleanup()

    def run_install(self, action='install', *args, success=True):
        result = subprocess.run([sys.executable, str(self.package / 'scripts/install.py'), action,
                                 '--root', str(self.app), *args], capture_output=True, text=True)
        self.assertEqual(result.returncode == 0, success, result.stderr)
        self.assertEqual((self.app / 'server.js').read_text(), 'native core\n')
        self.assertEqual((self.app / 'data/chat.jsonl').read_text(), 'important user data\n')
        return result

    def test_preview_never_writes(self):
        before = sorted(str(x.relative_to(self.app)) for x in self.app.rglob('*'))
        self.run_install()
        self.assertEqual(sorted(str(x.relative_to(self.app)) for x in self.app.rglob('*')), before)

    def test_install_repeat_remove_preserves_user_jobs_and_unrelated_plugin(self):
        self.run_install('install', '--apply')
        self.run_install('install', '--apply')
        receipt = self.app / '.background-jobs-install.json'
        self.assertEqual(receipt.stat().st_mode & 0o777, 0o600)
        server = self.app / 'plugins/background-jobs'
        (server / 'personal-note.txt').write_text('keep me')
        (self.app / 'data/background-jobs').mkdir()
        (self.app / 'data/background-jobs/result.json').write_text('durable result')
        self.run_install('uninstall', '--apply')
        self.assertFalse((server / 'index.mjs').exists())
        self.assertEqual((server / 'personal-note.txt').read_text(), 'keep me')
        self.assertEqual((self.app / 'data/background-jobs/result.json').read_text(), 'durable result')
        self.run_install('uninstall', '--apply')

    def test_existing_unknown_target_is_never_adopted(self):
        p = self.app / 'plugins/background-jobs/index.mjs'
        p.parent.mkdir(parents=True)
        p.write_text('different implementation')
        self.run_install('install', '--apply', success=False)
        self.assertEqual(p.read_text(), 'different implementation')

    def test_uninstall_refuses_later_edits(self):
        self.run_install('install', '--apply')
        p = self.app / 'plugins/background-jobs/index.mjs'
        p.write_text('my later improvement')
        self.run_install('uninstall', '--apply', success=False)
        self.assertEqual(p.read_text(), 'my later improvement')
        self.assertTrue((self.app / '.background-jobs-install.json').exists())

    def test_symlink_escape_rejected(self):
        outside = self.base / 'outside'
        outside.mkdir()
        (self.app / 'plugins').symlink_to(outside, target_is_directory=True)
        self.run_install('install', '--apply', success=False)
        self.assertEqual(list(outside.iterdir()), [])

    def test_receipt_path_traversal_rejected(self):
        self.run_install('install', '--apply')
        p = self.app / '.background-jobs-install.json'
        data = json.loads(p.read_text())
        data['files'][0]['path'] = '../../server.js'
        p.write_text(json.dumps(data))
        self.run_install('uninstall', '--apply', success=False)


if __name__ == '__main__':
    unittest.main()
