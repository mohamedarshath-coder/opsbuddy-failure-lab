"""DataSakshi agent monitor: runs one Claude Code stage inside the agent workflow.

Started by .github/workflows/agent.yml, always from main (never from the feature
branch, so a run cannot change its own guard). It:
- decodes the stage request DataSakshi sent (prompt, stage instruction, result
  schema, Claude Code arguments) and checks every argument against a fixed
  allowlist: read-only stages never get edit or command tools;
- runs Claude Code with the prompt on stdin, writes every stream line to the
  transcript, keeps the highest usage per message, and stops Claude past the
  turn cap or the time limit;
- always writes report.b64 (gzip + base64 JSON) for DataSakshi, and the stage's
  status for the push step.

Environment: STAGE, MAX_TURNS, MAX_MINUTES, PAYLOAD_FILE, OUT_DIR (and
ANTHROPIC_API_KEY for Claude Code).
"""

import base64
import gzip
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

READ = {"Read", "Grep", "Glob"}
WRITE = READ | {"Edit", "Write", "Bash"}
STAGE_TOOLS = {
    "plan": READ,
    "code-review": READ,
    "code": WRITE,
    "rework-review": WRITE,
    "rework-uat": WRITE,
}
# Flags DataSakshi sends; any other flag is refused. Values checked below.
FLAGS_NO_VALUE = {"--bare", "-p", "--verbose", "--no-session-persistence", "--strict-mcp-config"}
FLAGS_WITH_VALUE = {"--output-format", "--model", "--tools", "--permission-mode", "--max-budget-usd"}
MAX_BUDGET_USD = 10.0
STDERR_TAIL = 2000
# Highest value kept per message: a streamed message repeats its usage.
USAGE_KEYS = (
    "input_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
    "output_tokens",
)


def _int(value) -> int:
    return value if isinstance(value, int) and value >= 0 else 0


def check_args(args: list[str], stage: str) -> None:
    seen = {}
    i = 0
    while i < len(args):
        flag = args[i]
        if flag in FLAGS_NO_VALUE:
            seen[flag] = True
            i += 1
        elif flag in FLAGS_WITH_VALUE and i + 1 < len(args):
            seen[flag] = args[i + 1]
            i += 2
        else:
            raise ValueError(f"argument {flag!r} is not allowed")
    if seen.get("--output-format") != "stream-json" or seen.get("--permission-mode") != (
        "bypassPermissions"
    ):
        raise ValueError("output format or permission mode is not the expected one")
    if "--bare" not in seen or "-p" not in seen:
        raise ValueError("Claude Code must run with --bare -p")
    tools = set(str(seen.get("--tools", "")).split(","))
    if not tools or not tools <= STAGE_TOOLS[stage]:
        raise ValueError(f"tools {sorted(tools)} are not allowed for stage {stage}")
    budget = float(seen.get("--max-budget-usd", "inf"))
    if not 0 < budget <= MAX_BUDGET_USD:
        raise ValueError(f"budget {budget} is outside 0-{MAX_BUDGET_USD}")
    if not str(seen.get("--model", "")).startswith("claude-"):
        raise ValueError("model must be a Claude model")


def write_report(out: Path, report: dict) -> None:
    raw = json.dumps(report, separators=(",", ":")).encode("utf-8")
    (out / "report.b64").write_text(base64.b64encode(gzip.compress(raw, mtime=0)).decode())
    result = report.get("result") or {}
    status = (result.get("structured_output") or {}).get("status", "")
    (out / "status").write_text(str(status) if not report.get("stop") else "")


def main() -> int:
    out = Path(os.environ["OUT_DIR"])
    out.mkdir(parents=True, exist_ok=True)
    report = {"result": None, "usage_by_message": {}, "stop": None, "error": None}
    try:
        stage = os.environ["STAGE"]
        if stage not in STAGE_TOOLS:
            raise ValueError(f"unknown stage {stage!r}")
        max_turns = int(os.environ["MAX_TURNS"])
        max_minutes = int(os.environ["MAX_MINUTES"])
        payload = json.loads(
            gzip.decompress(base64.b64decode(Path(os.environ["PAYLOAD_FILE"]).read_text()))
        )
        args = [str(a) for a in payload["args"]]
        check_args(args, stage)
        command = [
            "claude",
            *args,
            "--append-system-prompt",
            str(payload["instruction"]),
            "--json-schema",
            str(payload["schema"]),
        ]
        proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        stderr_lines: list[str] = []
        threading.Thread(
            target=lambda: stderr_lines.extend(proc.stderr), daemon=True
        ).start()
        proc.stdin.write(str(payload["prompt"]))
        proc.stdin.close()

        timed_out = threading.Event()

        def on_timeout() -> None:
            timed_out.set()
            proc.kill()

        timer = threading.Timer(max_minutes * 60, on_timeout)
        timer.start()
        usage: dict[str, dict] = {}
        with (out / "transcript.jsonl").open("w", encoding="utf-8") as log:
            for line in proc.stdout:
                log.write(line)
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(event, dict):
                    continue
                if event.get("type") == "assistant":
                    message = event.get("message") or {}
                    mid = message.get("id")
                    if isinstance(mid, str):
                        prev = usage.get(mid, {})
                        now = message.get("usage") or {}
                        usage[mid] = {
                            k: max(_int(prev.get(k)), _int(now.get(k))) for k in USAGE_KEYS
                        }
                        if len(usage) > max_turns:
                            report["stop"] = "turns"
                            proc.kill()
                            break
                elif event.get("type") == "result":
                    report["result"] = event
        timer.cancel()
        proc.wait(timeout=60)
        if timed_out.is_set():
            report["stop"] = "time"
        report["usage_by_message"] = usage
        if proc.returncode and report["result"] is None and not report["stop"]:
            report["error"] = "".join(stderr_lines)[-STDERR_TAIL:] or f"exit {proc.returncode}"
    except Exception as exc:  # noqa: BLE001 - reported to DataSakshi, never hidden
        report["error"] = f"{type(exc).__name__}: {exc}"
    write_report(out, report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
