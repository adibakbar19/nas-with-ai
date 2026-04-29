import argparse
import os
import sys
from pathlib import Path

from nas_core.config.env import validate_backend_env, validate_run_all_env


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            # Force .env precedence during validation to match runtime behavior.
            os.environ[key] = value


def _as_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate required environment variables")
    parser.add_argument("--target", choices=["backend", "run_all", "all"], default="all")
    parser.add_argument("--env-file", default=".env", help="Env file path to load before checks")
    parser.add_argument("--skip-llm", default="1", help="run_all option")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _load_env_file(Path(args.env_file))

    missing: list[str] = []
    if args.target in {"backend", "all"}:
        missing.extend(validate_backend_env())
    if args.target in {"run_all", "all"}:
        missing.extend(
            validate_run_all_env(
                skip_llm=_as_bool(args.skip_llm),
            )
        )

    # unique + stable order
    seen = set()
    ordered: list[str] = []
    for key in missing:
        if key not in seen:
            seen.add(key)
            ordered.append(key)

    if ordered:
        print("Missing required env vars:")
        for key in ordered:
            print(f"- {key}")
        sys.exit(1)

    print("Environment validation passed.")


if __name__ == "__main__":
    main()
