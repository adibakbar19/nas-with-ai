from __future__ import annotations

import re
from pathlib import Path


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def extract_error_summary(log_path: Path, *, max_lines: int = 400, max_chars: int = 500) -> str | None:
    if not log_path.exists():
        return None
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    tail = lines[-max_lines:]

    exc_re = re.compile(r"([A-Za-z_][A-Za-z0-9_.]*(?:Exception|Error)):\s*(.+)")
    for raw in reversed(tail):
        line = raw.strip()
        if not line or " end job=" in line:
            continue
        match = exc_re.search(line)
        if match:
            return f"{match.group(1)}: {match.group(2)}"[:max_chars]

    for raw in reversed(tail):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("Traceback"):
            continue
        if line.startswith("File "):
            continue
        if line.startswith("at "):
            continue
        if line.startswith("[") and (" start job=" in line or " end job=" in line):
            continue
        return line[:max_chars]
    return None


def sanitize_log_line(raw_line: str, *, max_chars: int = 400) -> str:
    line = raw_line.replace("\r", "").rstrip("\n")
    line = _ANSI_ESCAPE_RE.sub("", line)
    if len(line) > max_chars:
        line = f"{line[:max_chars-3]}..."
    return line


def progress_from_log_line(line: str, current_pct: int, current_stage: str) -> tuple[int, str]:
    text = line.strip()
    if not text:
        return current_pct, current_stage

    if text.startswith("PIPELINE_STAGE:"):
        stage = text.split(":", 1)[1].strip().lower()
        stage_aliases = {
            "clean": "transform",
            "validated": "validate",
        }
        stage = stage_aliases.get(stage, stage)
        stage_progress = {
            "extract": 20,
            "transform": 45,
            "validate": 70,
            "final": 88,
        }
        if stage in stage_progress:
            return max(current_pct, stage_progress[stage]), stage

    lowered = text.lower()
    if "traceback (most recent call last)" in lowered:
        return max(current_pct, 95), "failing"
    if "validation_complete" in lowered:
        return max(current_pct, 90), "validate"
    if "write_complete" in lowered:
        return max(current_pct, 96), "final"

    match = re.search(r"\((\d+)\s*\+\s*(\d+)\)\s*/\s*(\d+)\]", text)
    if match:
        done = int(match.group(1))
        active = int(match.group(2))
        total = max(1, int(match.group(3)))
        ratio = min(1.0, (done + active) / total)
        pct = max(current_pct, min(94, int(20 + ratio * 70)))
        return pct, current_stage

    return current_pct, current_stage


def parse_counts_from_log(log_path: Path) -> dict:
    """Extract success/warning/failed counts from the CHECKPOINT stage=load line."""
    if not log_path.exists():
        return {}
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {}
    for line in reversed(text.splitlines()):
        if "CHECKPOINT" in line and "stage=load" in line and "event=done" in line:
            counts = {}
            for key in ("success_count", "warning_count", "failed_count"):
                m = re.search(rf"{key}=(\d+)", line)
                if m:
                    counts[key] = int(m.group(1))
            return counts
    return {}
