from __future__ import annotations

import os
from typing import Any

DEFAULT_SEDONA_VERSION = os.getenv("SEDONA_VERSION", "1.8.1")
DEFAULT_GEOTOOLS_VERSION = os.getenv("SEDONA_GEOTOOLS_VERSION", "1.8.1-33.1")
_SPARK_ARTIFACT_BY_MINOR = {
    "4.0": "org.apache.sedona:sedona-spark-shaded-4.0_2.13",
}


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y"}


def _spark_minor_from_runtime() -> str | None:
    explicit = os.getenv("SPARK_VERSION", "").strip()
    if explicit:
        parts = explicit.split(".")
        if len(parts) >= 2:
            return f"{parts[0]}.{parts[1]}"
    try:
        import pyspark  # type: ignore

        ver = getattr(pyspark, "__version__", "")
        parts = str(ver).split(".")
        if len(parts) >= 2:
            return f"{parts[0]}.{parts[1]}"
    except Exception:
        return None
    return None


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
    if not _is_truthy(os.getenv("SEDONA_AUTO_PACKAGES", "1")):
        return None

    spark_minor = _spark_minor_from_runtime()
    artifact = _SPARK_ARTIFACT_BY_MINOR.get(spark_minor or "")
    if not artifact:
        artifact = _SPARK_ARTIFACT_BY_MINOR["4.0"]
        if spark_minor:
            print(
                "Warning: Spark "
                f"{spark_minor} is not in tested Sedona package mapping. "
                "Falling back to sedona-spark-shaded-4.0_2.13. "
                "For full compatibility, use pyspark==4.0.x or set SPARK_JARS_PACKAGES explicitly."
            )
    sedona_pkg = f"{artifact}:{DEFAULT_SEDONA_VERSION}"
    geotools_pkg = f"org.datasyslab:geotools-wrapper:{DEFAULT_GEOTOOLS_VERSION}"
    return merge_spark_packages(sedona_pkg, geotools_pkg)


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


def configure_common_spark_builder(builder: Any) -> Any:
    spark_driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "").strip()
    spark_executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "").strip()
    spark_shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "").strip()
    spark_adaptive_enabled = os.getenv("SPARK_SQL_ADAPTIVE_ENABLED", "true").strip().lower()

    if spark_driver_memory:
        builder = builder.config("spark.driver.memory", spark_driver_memory)
    if spark_executor_memory:
        builder = builder.config("spark.executor.memory", spark_executor_memory)
    if spark_shuffle_partitions:
        builder = builder.config("spark.sql.shuffle.partitions", spark_shuffle_partitions)
    if spark_adaptive_enabled in {"1", "true", "yes", "y"}:
        builder = (
            builder.config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
        )
    return builder


def register_sedona(spark: Any) -> None:
    # Register Sedona SQL functions only once per SparkSession to avoid duplicate-function spam.
    if bool(getattr(spark, "_nas_sedona_registered", False)):
        return
    try:
        from sedona.spark import SedonaContext

        SedonaContext.create(spark)
    except Exception:
        try:
            from sedona.register import SedonaRegistrator
        except Exception as exc:
            raise RuntimeError(
                "Sedona is required for spatial mapping. Install it with: pip install apache-sedona"
            ) from exc
        SedonaRegistrator.registerAll(spark)
    spark._nas_sedona_registered = True


def quiet_spark_spatial_warnings(spark: Any) -> None:
    # Sedona's shapefile datasource triggers verbose Spark warnings while probing companion files.
    # They are noisy but harmless for our batch jobs, so keep them out of job logs.
    if bool(getattr(spark, "_nas_spatial_warnings_quieted", False)):
        return

    logger_names = [
        "org.apache.spark.sql.execution.streaming.FileStreamSink",
        "org.apache.spark.sql.execution.WindowExec",
    ]
    try:
        jvm = spark._jvm
        configurator = jvm.org.apache.logging.log4j.core.config.Configurator
        level = jvm.org.apache.logging.log4j.Level.ERROR
        for logger_name in logger_names:
            configurator.setLevel(logger_name, level)
    except Exception:
        try:
            jvm = spark._jvm
            level = jvm.org.apache.log4j.Level.ERROR
            for logger_name in logger_names:
                jvm.org.apache.log4j.Logger.getLogger(logger_name).setLevel(level)
        except Exception:
            return
    spark._nas_spatial_warnings_quieted = True
