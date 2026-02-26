from __future__ import annotations

import os
from typing import Any


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
