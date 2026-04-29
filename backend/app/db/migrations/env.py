from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from backend.app.db.migration_settings import get_runtime_db_schema, get_runtime_db_sqlalchemy_url, load_env_file


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None

load_env_file()
database_url = get_runtime_db_sqlalchemy_url()
schema_name = get_runtime_db_schema()

config.set_main_option("sqlalchemy.url", database_url)
config.set_main_option("nas_schema", schema_name)


def run_migrations_offline() -> None:
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(database_url, poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_schemas=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
