#!/usr/bin/env python3
"""Verify benchmark runners execute directly with benchmark tools mocked."""

import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRATCH_ROOT = ROOT / ".orbit" / "tmp"
RUNNERS = (
    "benchmark/scripts/run_ghz_gateway.sh",
    "benchmark/scripts/run_k6_admin.sh",
    "benchmark/scripts/run_k6_gateway.sh",
)


class RunnerLaunchTest(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary_directory = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(self.temporary_directory.cleanup)
        self.scratch = Path(self.temporary_directory.name)
        self.mock_bin = self.scratch / "bin"
        self.mock_bin.mkdir()
        self.call_log = self.scratch / "mock-calls.jsonl"

        mock_source = f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys

with open(os.environ["RUNNER_LAUNCH_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps({{"tool": Path(sys.argv[0]).name, "args": sys.argv[1:]}}))
    log.write("\\n")
"""
        for tool in ("ghz", "k6"):
            mock = self.mock_bin / tool
            mock.write_text(mock_source, encoding="utf-8")
            mock.chmod(mock.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def environment(self):
        env = os.environ.copy()
        env["PATH"] = f"{self.mock_bin}{os.pathsep}{env.get('PATH', '')}"
        env["RUNNER_LAUNCH_LOG"] = str(self.call_log)
        env["RESULTS_DIR"] = str(self.scratch / "results")
        env["TOTAL"] = "1"
        env["CONCURRENCY"] = "1"
        env["TARGET"] = "127.0.0.1:1"
        env["ADMIN_BASE_URL"] = "http://127.0.0.1:1"
        return env

    def launch(self, relative_runner):
        runner = ROOT / relative_runner
        try:
            completed = subprocess.run(
                [str(runner)],
                cwd=ROOT,
                env=self.environment(),
                text=True,
                capture_output=True,
                check=False,
            )
        except OSError as error:
            self.fail(f"direct execution of {relative_runner} failed: {error}")

        self.assertEqual(
            completed.returncode,
            0,
            msg=f"{relative_runner} failed\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}",
        )
        self.assertNotIn("syntax error", completed.stderr.lower())

    def mock_calls(self):
        if not self.call_log.exists():
            return []
        return [json.loads(line) for line in self.call_log.read_text(encoding="utf-8").splitlines()]

    def test_all_runners_start_with_bash_shebang_at_byte_zero(self):
        for relative_runner in RUNNERS:
            with self.subTest(runner=relative_runner):
                content = (ROOT / relative_runner).read_bytes()
                self.assertTrue(
                    content.startswith(b"#!/usr/bin/env bash\n"),
                    f"{relative_runner} must start with its Bash shebang",
                )

    def test_ghz_gateway_executes_twice_with_mocked_ghz(self):
        self.launch(RUNNERS[0])

        calls = self.mock_calls()
        self.assertEqual([call["tool"] for call in calls], ["ghz", "ghz"])
        for call in calls:
            self.assertIn("--total", call["args"])
            self.assertEqual(call["args"][call["args"].index("--total") + 1], "1")
            self.assertEqual(call["args"][-1], "127.0.0.1:1")

    def test_k6_admin_executes_with_mocked_k6(self):
        self.launch(RUNNERS[1])

        calls = self.mock_calls()
        self.assertEqual([call["tool"] for call in calls], ["k6"])
        self.assertTrue(any("admin_bench.js" in arg for arg in calls[0]["args"]))

    def test_k6_gateway_executes_with_mocked_k6(self):
        self.launch(RUNNERS[2])

        calls = self.mock_calls()
        self.assertEqual([call["tool"] for call in calls], ["k6"])
        self.assertTrue(any("gateway_bench.js" in arg for arg in calls[0]["args"]))


if __name__ == "__main__":
    unittest.main()
