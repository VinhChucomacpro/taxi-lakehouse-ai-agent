from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_DOCS = [
    "README.md",
    "AGENTS.md",
    "docs/architecture.md",
    "docs/data-contracts.md",
    "docs/modeling-decisions.md",
    "docs/gold-star-schema.md",
    "docs/runbook.md",
    "docs/data-quality-report.md",
    "docs/agent-evaluation.md",
    "docs/demo-scenarios.md",
    "docs/performance-report.md",
    "docs/release-checklist.md",
    "docs/security-notes.md",
]

REQUIRED_ENV_KEYS = [
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "MINIO_ENDPOINT",
    "MINIO_BUCKET",
    "DUCKDB_PATH",
    "QUERY_AUDIT_LOG_PATH",
    "OPENAI_API_KEY",
    "OPENAI_ANSWER_SYNTHESIS",
    "API_PORT",
]

REQUIRED_PORT_NOTES = [
    "8080",
    "9001",
    "8000",
    "8501",
]

SECRET_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9_-]{20,}"),
    re.compile(r"OPENAI_API_KEY\s*=\s*(?!replace-me|<redacted>|$).+", re.IGNORECASE),
]


def main() -> int:
    checks = [
        check_required_docs,
        check_env_example,
        check_runbook_ports,
        check_release_checklist,
        check_no_tracked_env,
        check_no_obvious_doc_secrets,
    ]
    failures: list[str] = []
    for check in checks:
        failures.extend(check())

    if failures:
        print("Release check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Release check passed.")
    return 0


def check_required_docs() -> list[str]:
    return [
        f"Missing required document: {path}"
        for path in REQUIRED_DOCS
        if not (ROOT / path).is_file()
    ]


def check_env_example() -> list[str]:
    path = ROOT / ".env.example"
    if not path.is_file():
        return ["Missing .env.example"]
    text = path.read_text(encoding="utf-8")
    return [
        f".env.example is missing key: {key}"
        for key in REQUIRED_ENV_KEYS
        if not re.search(rf"^{re.escape(key)}=", text, flags=re.MULTILINE)
    ]


def check_runbook_ports() -> list[str]:
    path = ROOT / "docs" / "runbook.md"
    text = path.read_text(encoding="utf-8") if path.is_file() else ""
    return [
        f"docs/runbook.md does not mention local port {port}"
        for port in REQUIRED_PORT_NOTES
        if port not in text
    ]


def check_release_checklist() -> list[str]:
    path = ROOT / "docs" / "release-checklist.md"
    text = path.read_text(encoding="utf-8") if path.is_file() else ""
    required_terms = [
        "docker compose up -d",
        "python -m pytest -p no:cacheprovider",
        "dbt build",
        "Airflow",
        "MinIO",
        "FastAPI",
        "Streamlit",
        "demo scenarios",
        "security",
    ]
    return [
        f"docs/release-checklist.md is missing release topic: {term}"
        for term in required_terms
        if term not in text
    ]


def check_no_tracked_env() -> list[str]:
    try:
        result = subprocess.run(
            ["git", "ls-files", ".env", ".env.*"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        return [f"Could not inspect tracked env files: {exc}"]

    tracked = [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip() and line.strip() != ".env.example"
    ]
    return [f"Environment file is tracked and should not be: {path}" for path in tracked]


def check_no_obvious_doc_secrets() -> list[str]:
    failures: list[str] = []
    for path in [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]:
        text = path.read_text(encoding="utf-8")
        for pattern in SECRET_PATTERNS:
            if pattern.search(text):
                failures.append(f"Potential secret found in {path.relative_to(ROOT)}")
    return failures


if __name__ == "__main__":
    raise SystemExit(main())
