from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


def _env_first(*keys: str, env: dict[str, str] | None = None) -> str:
    ctx = env if env is not None else os.environ
    for key in keys:
        value = str(ctx.get(key, "")).strip()
        if value:
            return value
    return ""


def _env_bool(*keys: str, default: bool, env: dict[str, str] | None = None) -> bool:
    raw = _env_first(*keys, env=env)
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class ObjectStoreSettings:
    bucket: str
    region: str
    endpoint: str
    public_endpoint: str
    access_key_id: str
    secret_access_key: str
    session_token: str
    secure: bool
    public_secure: bool
    use_path_style: bool

    @classmethod
    def from_env(cls, *, env: dict[str, str] | None = None) -> "ObjectStoreSettings":
        endpoint = _env_first(
            "OBJECT_STORE_ENDPOINT",
            "S3_ENDPOINT_URL",
            "AWS_S3_ENDPOINT",
            env=env,
        )
        public_endpoint = _env_first(
            "OBJECT_STORE_PUBLIC_ENDPOINT",
            "S3_PUBLIC_ENDPOINT_URL",
            "AWS_S3_PUBLIC_ENDPOINT",
            env=env,
        )
        secure = _env_bool(
            "OBJECT_STORE_SECURE",
            "S3_SECURE",
            "AWS_S3_SECURE",
            default=endpoint.startswith("https://"),
            env=env,
        )
        public_secure = _env_bool(
            "OBJECT_STORE_PUBLIC_SECURE",
            "S3_PUBLIC_SECURE",
            "AWS_S3_PUBLIC_SECURE",
            default=secure if public_endpoint else secure,
            env=env,
        )
        use_path_style = _env_bool(
            "OBJECT_STORE_USE_PATH_STYLE",
            "S3_USE_PATH_STYLE",
            "AWS_S3_USE_PATH_STYLE",
            default=bool(endpoint),
            env=env,
        )
        bucket = _env_first("OBJECT_STORE_BUCKET", "S3_BUCKET", "AWS_S3_BUCKET", env=env)
        if not bucket:
            raise RuntimeError("OBJECT_STORE_BUCKET is required")
        return cls(
            bucket=bucket,
            region=_env_first("AWS_REGION", "OBJECT_STORE_REGION", "S3_REGION", env=env) or "us-east-1",
            endpoint=endpoint,
            public_endpoint=public_endpoint,
            access_key_id=_env_first("AWS_ACCESS_KEY_ID", "OBJECT_STORE_ACCESS_KEY", env=env),
            secret_access_key=_env_first(
                "AWS_SECRET_ACCESS_KEY",
                "OBJECT_STORE_SECRET_KEY",
                env=env,
            ),
            session_token=_env_first("AWS_SESSION_TOKEN", "OBJECT_STORE_SESSION_TOKEN", env=env),
            secure=secure,
            public_secure=public_secure,
            use_path_style=use_path_style,
        )

    @staticmethod
    def _normalize_endpoint(raw: str, *, secure: bool) -> str | None:
        value = raw.strip()
        if not value:
            return None
        if value.startswith("http://") or value.startswith("https://"):
            return value
        scheme = "https" if secure else "http"
        return f"{scheme}://{value}"

    def endpoint_url(self, *, public: bool = False) -> str | None:
        if public and self.public_endpoint:
            return self._normalize_endpoint(self.public_endpoint, secure=self.public_secure)
        return self._normalize_endpoint(self.endpoint, secure=self.secure)


def build_s3_client(settings: ObjectStoreSettings, *, public: bool = False):
    try:
        import boto3
        from botocore.client import Config
    except Exception as exc:  # pragma: no cover - dependency provided in runtime image
        raise RuntimeError("boto3 is required for object storage access") from exc

    kwargs: dict[str, Any] = {
        "service_name": "s3",
        "region_name": settings.region,
        "config": Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if settings.use_path_style else "virtual"},
        ),
    }
    endpoint_url = settings.endpoint_url(public=public)
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    if settings.access_key_id:
        kwargs["aws_access_key_id"] = settings.access_key_id
    if settings.secret_access_key:
        kwargs["aws_secret_access_key"] = settings.secret_access_key
    if settings.session_token:
        kwargs["aws_session_token"] = settings.session_token
    return boto3.client(**kwargs)


def is_missing_object_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    code = str(response.get("Error", {}).get("Code") or "").strip()
    status = int(response.get("ResponseMetadata", {}).get("HTTPStatusCode") or 0)
    return code in {"404", "NoSuchKey", "NoSuchBucket", "NotFound"} or status == 404

