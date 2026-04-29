from typing import Any, Iterator

import sqlalchemy as sa


INDEXABLE_FIELDS = {
    "record_id",
    "naskod",
    "address_clean",
    "premise_no",
    "lot_no",
    "unit_no",
    "floor_no",
    "floor_level",
    "building_name",
    "street_name_prefix",
    "street_name",
    "sub_locality_1",
    "sub_locality_2",
    "sub_locality_levels",
    "postcode",
    "postcode_code",
    "postcode_name",
    "locality_name",
    "district_code",
    "district_name",
    "mukim_code",
    "mukim_name",
    "state_code",
    "state_name",
    "address_type",
    "confidence_score",
    "confidence_band",
    "validation_status",
    "reason_codes",
    "latitude",
    "longitude",
    "geom",
    "geometry",
}


def sqlalchemy_postgres_url(db_url: str) -> str:
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return db_url


def clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (value != value):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    if isinstance(value, list):
        cleaned_items = [clean_value(item) for item in value]
        return [item for item in cleaned_items if item is not None] or None
    return value


def autocomplete_text(doc: dict[str, Any]) -> str:
    pieces = []
    for key in [
        "address_clean",
        "building_name",
        "street_name",
        "sub_locality_1",
        "sub_locality_2",
        "locality_name",
        "postcode",
        "postcode_code",
        "postcode_name",
        "district_name",
        "mukim_name",
        "state_name",
        "naskod",
    ]:
        value = doc.get(key)
        if isinstance(value, str) and value:
            pieces.append(value)
    return " ".join(dict.fromkeys(pieces))


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _table_ref(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _table_columns(inspector, *, schema: str, table: str) -> set[str]:
    if not inspector.has_table(table, schema=schema):
        return set()
    return {str(column["name"]) for column in inspector.get_columns(table, schema=schema)}


def _coalesce_expr(expressions: list[str], alias: str) -> str:
    if not expressions:
        return f"NULL AS {_quote_ident(alias)}"
    if len(expressions) == 1:
        return f"{expressions[0]} AS {_quote_ident(alias)}"
    return f"COALESCE({', '.join(expressions)}) AS {_quote_ident(alias)}"


def _build_postgres_select(*, schema: str, inspector) -> str:
    sa_cols = _table_columns(inspector, schema=schema, table="standardized_address")
    if not sa_cols:
        raise RuntimeError(f"Missing table: {schema}.standardized_address")

    table_columns = {
        "naskod": _table_columns(inspector, schema=schema, table="naskod"),
        "street": _table_columns(inspector, schema=schema, table="street"),
        "locality": _table_columns(inspector, schema=schema, table="locality"),
        "mukim": _table_columns(inspector, schema=schema, table="mukim"),
        "district": _table_columns(inspector, schema=schema, table="district"),
        "state": _table_columns(inspector, schema=schema, table="state"),
        "postcode": _table_columns(inspector, schema=schema, table="postcode"),
    }

    select_parts: list[str] = []
    joins: list[str] = []
    selected_aliases: set[str] = set()

    def add_select(alias: str, expression: str) -> None:
        if alias in selected_aliases:
            return
        selected_aliases.add(alias)
        select_parts.append(f"{expression} AS {_quote_ident(alias)}")

    if "record_id" in sa_cols:
        add_select("record_id", 'sa."record_id"::text')
    elif "address_id" in sa_cols:
        add_select("record_id", 'sa."address_id"::text')
    else:
        add_select("record_id", "NULL")

    direct_fields = [
        "address_clean",
        "premise_no",
        "lot_no",
        "unit_no",
        "floor_no",
        "floor_level",
        "building_name",
        "sub_locality_1",
        "sub_locality_2",
        "sub_locality_levels",
        "address_type",
        "confidence_score",
        "confidence_band",
        "validation_status",
        "reason_codes",
        "latitude",
        "longitude",
        "geom",
        "geometry",
    ]
    for field in direct_fields:
        if field not in sa_cols:
            continue
        expression = f'sa.{_quote_ident(field)}::text' if field in {"geom", "geometry"} else f"sa.{_quote_ident(field)}"
        add_select(field, expression)

    if "address_id" in sa_cols and {"address_id", "code", "is_vanity"}.issubset(table_columns["naskod"]):
        joins.append(f"LEFT JOIN {_table_ref(schema, 'naskod')} n ON n.address_id = sa.address_id AND n.is_vanity = false")
        add_select("naskod", 'n."code"')
    elif "naskod" in sa_cols:
        add_select("naskod", 'sa."naskod"')

    if "street_id" in sa_cols and table_columns["street"]:
        joins.append(f"LEFT JOIN {_table_ref(schema, 'street')} st ON st.street_id = sa.street_id")
        street_prefix_exprs = []
        if "street_name_prefix" in sa_cols:
            street_prefix_exprs.append('sa."street_name_prefix"')
        if "street_name_prefix" in table_columns["street"]:
            street_prefix_exprs.append('st."street_name_prefix"')
        street_name_exprs = []
        if "street_name" in sa_cols:
            street_name_exprs.append('sa."street_name"')
        if "street_name" in table_columns["street"]:
            street_name_exprs.append('st."street_name"')
        select_parts.append(_coalesce_expr(street_prefix_exprs, "street_name_prefix"))
        select_parts.append(_coalesce_expr(street_name_exprs, "street_name"))
        selected_aliases.update({"street_name_prefix", "street_name"})
    else:
        for field in ["street_name_prefix", "street_name"]:
            if field in sa_cols:
                add_select(field, f"sa.{_quote_ident(field)}")

    join_specs = [
        ("locality", "l", "locality_id", ["locality_name"]),
        ("mukim", "m", "mukim_id", ["mukim_code", "mukim_name"]),
        ("district", "d", "district_id", ["district_code", "district_name"]),
        ("state", "s", "state_id", ["state_code", "state_name"]),
        ("postcode", "p", "postcode_id", ["postcode", "postcode_code", "postcode_name"]),
    ]
    for table, alias, fk_col, fields in join_specs:
        if fk_col in sa_cols and table_columns[table]:
            joins.append(f"LEFT JOIN {_table_ref(schema, table)} {alias} ON {alias}.{fk_col} = sa.{fk_col}")
            for field in fields:
                if field in selected_aliases:
                    continue
                expressions = []
                if field in sa_cols:
                    expressions.append(f"sa.{_quote_ident(field)}")
                if field in table_columns[table]:
                    expressions.append(f"{alias}.{_quote_ident(field)}")
                select_parts.append(_coalesce_expr(expressions, field))
                selected_aliases.add(field)
        else:
            for field in fields:
                if field in sa_cols:
                    add_select(field, f"sa.{_quote_ident(field)}")

    order_sql = 'ORDER BY sa."address_id" ASC' if "address_id" in sa_cols else ""
    return f"""
        SELECT {", ".join(select_parts)}
        FROM {_table_ref(schema, "standardized_address")} sa
        {" ".join(joins)}
        {order_sql}
    """


def iter_search_documents(*, db_url: str, schema: str, batch_size: int) -> Iterator[dict[str, Any]]:
    engine = sa.create_engine(sqlalchemy_postgres_url(db_url))
    inspector = sa.inspect(engine)
    query = _build_postgres_select(schema=schema, inspector=inspector)
    with engine.connect().execution_options(stream_results=True) as conn:
        result = conn.execute(sa.text(query))
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                doc = {}
                for key, value in dict(row._mapping).items():
                    cleaned = clean_value(value)
                    if cleaned is not None:
                        doc[key] = cleaned
                if not doc:
                    continue
                doc["autocomplete"] = autocomplete_text(doc)
                yield doc
