from __future__ import annotations

import os
from typing import Any

DEFAULT_SEDONA_SPARK_PACKAGES = ",".join(
    [
        "org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.8.1",
        "org.datasyslab:geotools-wrapper:1.8.1-33.1",
    ]
)


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y"}


def merge_spark_packages(*package_specs: str | None) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for spec in package_specs:
        if not spec:
            continue
        for item in str(spec).split(","):
            pkg = item.strip()
            if not pkg or pkg in seen:
                continue
            seen.add(pkg)
            parts.append(pkg)
    return ",".join(parts)


def resolve_sedona_spark_packages(existing: str | None = None) -> str | None:
    configured = (existing or os.getenv("SPARK_JARS_PACKAGES", "")).strip()
    if configured:
        return configured
    if _is_truthy(os.getenv("SEDONA_AUTO_PACKAGES", "1")):
        return DEFAULT_SEDONA_SPARK_PACKAGES
    return None


def configure_sedona_builder(builder: Any, *, enabled: bool = True) -> Any:
    if not enabled:
        return builder
    enable_kryo = os.getenv("SEDONA_ENABLE_KRYO", "").strip().lower()
    if enable_kryo not in {"1", "true", "yes", "y"}:
        return builder
    try:
        from sedona.utils import KryoSerializer, SedonaKryoRegistrator
    except Exception:
        return builder
    return (
        builder.config("spark.serializer", KryoSerializer.getName)
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
    )


def register_sedona(spark: Any) -> None:
    try:
        from sedona.register import SedonaRegistrator
    except Exception as exc:
        raise RuntimeError(
            "Sedona is required for PBT mapping. Install it with: pip install apache-sedona"
        ) from exc
    SedonaRegistrator.registerAll(spark)
