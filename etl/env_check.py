import os
from typing import Iterable


class EnvValidationError(RuntimeError):
    pass


def _missing(env: dict[str, str], keys: Iterable[str]) -> list[str]:
    out: list[str] = []
    for key in keys:
        value = env.get(key)
        if value is None or str(value).strip() == "":
            out.append(key)
    return out


def validate_backend_env(*, env: dict[str, str] | None = None, raise_on_error: bool = False) -> list[str]:
    ctx = env if env is not None else dict(os.environ)
    required = ["ES_URL", "ES_INDEX", "NAS_AUDIT_LOG"]
    missing = _missing(ctx, required)
    bucket = (
        ctx.get("OBJECT_STORE_BUCKET")
        or ctx.get("S3_BUCKET")
        or ctx.get("AWS_S3_BUCKET")
        or ctx.get("MINIO_BUCKET")
    )
    if bucket is None or str(bucket).strip() == "":
        missing.append("OBJECT_STORE_BUCKET|S3_BUCKET|MINIO_BUCKET")
    if missing and raise_on_error:
        raise EnvValidationError(f"Missing required backend env vars: {', '.join(missing)}")
    return missing


def validate_run_all_env(
    *,
    env: dict[str, str] | None = None,
    skip_load: bool = False,
    skip_es: bool = True,
    skip_llm: bool = True,
    openai_required_when_llm_enabled: bool = True,
    raise_on_error: bool = False,
) -> list[str]:
    ctx = env if env is not None else dict(os.environ)

    required = ["NAS_AUDIT_LOG"]
    if not skip_load:
        required.extend(
            [
                "PGHOST",
                "PGPORT",
                "PGDATABASE",
                "PGUSER",
                "PGPASSWORD",
                "PGSCHEMA",
            ]
        )
    if not skip_es:
        required.extend(["ES_URL", "ES_INDEX"])
    if not skip_llm and openai_required_when_llm_enabled:
        required.append("OPENAI_API_KEY")

    missing = _missing(ctx, required)
    if missing and raise_on_error:
        raise EnvValidationError(f"Missing required run_all env vars: {', '.join(missing)}")
    return missing
