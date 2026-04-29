from __future__ import annotations

import json
from typing import Any

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    sql = None
    dict_row = None


_SUPPORTED_BOUNDARY_TYPES = {
    "state_boundary",
    "district_boundary",
    "mukim_boundary",
    "postcode_boundary",
    "pbt",
}


class BoundaryAdminRepository:
    def __init__(self, *, dsn: str, lookup_schema: str) -> None:
        self._dsn = dsn
        self._lookup_schema = lookup_schema

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for BoundaryAdminRepository")
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _tbl(self, table_name: str):
        assert sql is not None
        return sql.SQL("{}.{}").format(sql.Identifier(self._lookup_schema), sql.Identifier(table_name))

    def ensure_schema(self) -> None:
        assert sql is not None
        schema_ident = sql.Identifier(self._lookup_schema)
        version_tbl = self._tbl("boundary_upload_version")
        feature_tbl = self._tbl("boundary_upload_feature")
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema_ident))
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                      version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                      boundary_type TEXT NOT NULL,
                      version_label TEXT NOT NULL,
                      status TEXT NOT NULL DEFAULT 'draft',
                      row_count INTEGER NOT NULL DEFAULT 0,
                      uploaded_by TEXT NULL,
                      activated_by TEXT NULL,
                      source_note TEXT NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      activated_at TIMESTAMPTZ NULL
                    )
                    """
                ).format(version_tbl)
            )
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                      feature_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                      version_id BIGINT NOT NULL REFERENCES {} (version_id) ON DELETE CASCADE,
                      row_num INTEGER NOT NULL,
                      feature_payload JSONB NOT NULL,
                      boundary_wkt TEXT NOT NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                ).format(feature_tbl, version_tbl)
            )
            cur.execute(
                sql.SQL(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS boundary_upload_version_type_label_uniq
                    ON {} (boundary_type, version_label)
                    """
                ).format(version_tbl)
            )
            cur.execute(
                sql.SQL(
                    """
                    CREATE INDEX IF NOT EXISTS boundary_upload_version_type_status_idx
                    ON {} (boundary_type, status, created_at DESC)
                    """
                ).format(version_tbl)
            )
            cur.execute(
                sql.SQL(
                    """
                    CREATE INDEX IF NOT EXISTS boundary_upload_feature_version_idx
                    ON {} (version_id, row_num)
                    """
                ).format(feature_tbl)
            )
            self._ensure_boundary_linkage_columns(cur=cur)
            conn.commit()

    def _ensure_boundary_linkage_columns(self, *, cur) -> None:
        assert sql is not None
        cur.execute(
            sql.SQL(
                """
                ALTER TABLE IF EXISTS {}
                ADD COLUMN IF NOT EXISTS state_id INTEGER NULL
                """
            ).format(self._tbl("state_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                ALTER TABLE IF EXISTS {}
                ADD COLUMN IF NOT EXISTS state_id INTEGER NULL
                """
            ).format(self._tbl("district_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                ALTER TABLE IF EXISTS {}
                ADD COLUMN IF NOT EXISTS district_id INTEGER NULL
                """
            ).format(self._tbl("district_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                ALTER TABLE IF EXISTS {}
                ADD COLUMN IF NOT EXISTS state_id INTEGER NULL
                """
            ).format(self._tbl("mukim_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                ALTER TABLE IF EXISTS {}
                ADD COLUMN IF NOT EXISTS district_id INTEGER NULL
                """
            ).format(self._tbl("mukim_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                ALTER TABLE IF EXISTS {}
                ADD COLUMN IF NOT EXISTS postcode_id INTEGER NULL
                """
            ).format(self._tbl("postcode_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS state_boundary_state_id_idx
                ON {} (state_id)
                """
            ).format(self._tbl("state_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS district_boundary_state_id_idx
                ON {} (state_id)
                """
            ).format(self._tbl("district_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS district_boundary_district_id_idx
                ON {} (district_id)
                """
            ).format(self._tbl("district_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS mukim_boundary_state_id_idx
                ON {} (state_id)
                """
            ).format(self._tbl("mukim_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS mukim_boundary_district_id_idx
                ON {} (district_id)
                """
            ).format(self._tbl("mukim_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS mukim_boundary_mukim_id_idx
                ON {} (mukim_id)
                """
            ).format(self._tbl("mukim_boundary"))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS postcode_boundary_postcode_id_idx
                ON {} (postcode_id)
                """
            ).format(self._tbl("postcode_boundary"))
        )

    def list_versions(self, *, boundary_type: str | None, status: str | None, limit: int) -> list[dict[str, Any]]:
        assert sql is not None
        self.ensure_schema()
        stmt = sql.SQL(
            """
            SELECT
              version_id,
              boundary_type,
              version_label,
              status,
              row_count,
              uploaded_by,
              activated_by,
              source_note,
              created_at,
              updated_at,
              activated_at
            FROM {}
            WHERE (%s IS NULL OR boundary_type = %s)
              AND (%s IS NULL OR status = %s)
            ORDER BY created_at DESC, version_id DESC
            LIMIT %s
            """
        ).format(self._tbl("boundary_upload_version"))
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (boundary_type, boundary_type, status, status, limit))
            return [dict(row) for row in cur.fetchall()]

    def get_version(self, *, version_id: int) -> dict[str, Any] | None:
        assert sql is not None
        self.ensure_schema()
        stmt = sql.SQL(
            """
            SELECT
              version_id,
              boundary_type,
              version_label,
              status,
              row_count,
              uploaded_by,
              activated_by,
              source_note,
              created_at,
              updated_at,
              activated_at
            FROM {}
            WHERE version_id = %s
            LIMIT 1
            """
        ).format(self._tbl("boundary_upload_version"))
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (version_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def create_version(
        self,
        *,
        boundary_type: str,
        version_label: str,
        uploaded_by: str,
        source_note: str | None,
        features: list[dict[str, Any]],
    ) -> dict[str, Any]:
        assert sql is not None
        if boundary_type not in _SUPPORTED_BOUNDARY_TYPES:
            raise ValueError("unsupported boundary_type")
        self.ensure_schema()
        version_tbl = self._tbl("boundary_upload_version")
        feature_tbl = self._tbl("boundary_upload_feature")
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (
                      boundary_type,
                      version_label,
                      status,
                      row_count,
                      uploaded_by,
                      source_note
                    )
                    VALUES (%s, %s, 'draft', %s, %s, %s)
                    RETURNING version_id
                    """
                ).format(version_tbl),
                (boundary_type, version_label, len(features), uploaded_by, source_note),
            )
            version_id = int(cur.fetchone()["version_id"])
            for idx, feature in enumerate(features, start=1):
                payload = dict(feature)
                boundary_wkt = str(payload.pop("boundary_wkt"))
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {} (
                          version_id,
                          row_num,
                          feature_payload,
                          boundary_wkt
                        )
                        VALUES (%s, %s, %s::jsonb, %s)
                        """
                    ).format(feature_tbl),
                    (version_id, idx, json.dumps(payload), boundary_wkt),
                )
            conn.commit()
        row = self.get_version(version_id=version_id)
        if row is None:
            raise RuntimeError("boundary upload version create succeeded but fetch failed")
        return row

    def _validate_features(self, *, cur, version_id: int) -> dict[str, int]:
        assert sql is not None
        feature_tbl = self._tbl("boundary_upload_feature")
        stmt = sql.SQL(
            """
            SELECT
              COUNT(*)::INTEGER AS row_count,
              COUNT(*) FILTER (
                WHERE boundary_wkt IS NULL
                   OR trim(boundary_wkt) = ''
                   OR ST_GeomFromText(boundary_wkt, 4326) IS NULL
                   OR NOT ST_IsValid(ST_GeomFromText(boundary_wkt, 4326))
              )::INTEGER AS invalid_geom_count
            FROM {}
            WHERE version_id = %s
            """
        ).format(feature_tbl)
        cur.execute(stmt, (version_id,))
        row = cur.fetchone()
        return {"row_count": int(row["row_count"] or 0), "invalid_geom_count": int(row["invalid_geom_count"] or 0)}

    def _replace_canonical_table(self, *, cur, version_id: int, boundary_type: str) -> None:
        assert sql is not None
        feature_tbl = self._tbl("boundary_upload_feature")
        if boundary_type == "state_boundary":
            cur.execute(sql.SQL("DELETE FROM {}").format(self._tbl("state_boundary")))
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (state_id, state_code, state_name, boundary_geom)
                    SELECT
                      st.state_id,
                      src.state_code,
                      src.state_name,
                      src.boundary_geom
                    FROM (
                      SELECT
                        nullif(trim(feature_payload->>'state_code'), '') AS state_code,
                        upper(nullif(trim(feature_payload->>'state_name'), '')) AS state_name,
                        ST_Force2D(ST_GeomFromText(boundary_wkt, 4326)) AS boundary_geom
                      FROM {}
                      WHERE version_id = %s
                    ) src
                    LEFT JOIN LATERAL (
                      SELECT st.state_id
                      FROM {} st
                      WHERE (
                        src.state_code IS NOT NULL
                        AND upper(trim(coalesce(st.state_code::text, ''))) = upper(trim(src.state_code))
                      )
                      OR (
                        src.state_name IS NOT NULL
                        AND upper(trim(coalesce(st.state_name::text, ''))) = upper(trim(src.state_name))
                      )
                      ORDER BY st.state_id ASC
                      LIMIT 1
                    ) st ON TRUE
                    """
                ).format(self._tbl("state_boundary"), feature_tbl, self._tbl("state")),
                (version_id,),
            )
            return
        if boundary_type == "district_boundary":
            cur.execute(sql.SQL("DELETE FROM {}").format(self._tbl("district_boundary")))
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (state_id, district_id, state_code, district_code, district_name, boundary_geom)
                    SELECT
                      match.state_id,
                      match.district_id,
                      src.state_code,
                      src.district_code,
                      src.district_name,
                      src.boundary_geom
                    FROM (
                      SELECT
                        nullif(trim(feature_payload->>'state_code'), '') AS state_code,
                        nullif(trim(feature_payload->>'district_code'), '') AS district_code,
                        upper(nullif(trim(feature_payload->>'district_name'), '')) AS district_name,
                        ST_Force2D(ST_GeomFromText(boundary_wkt, 4326)) AS boundary_geom
                      FROM {}
                      WHERE version_id = %s
                    ) src
                    LEFT JOIN LATERAL (
                      SELECT
                        d.district_id,
                        st.state_id
                      FROM {} d
                      JOIN {} st
                        ON st.state_id = d.state_id
                      WHERE (
                        (
                          src.district_code IS NOT NULL
                          AND upper(trim(coalesce(d.district_code::text, ''))) = upper(trim(src.district_code))
                        )
                        OR (
                          src.district_name IS NOT NULL
                          AND upper(trim(coalesce(d.district_name::text, ''))) = upper(trim(src.district_name))
                        )
                      )
                      AND (
                        src.state_code IS NULL
                        OR upper(trim(coalesce(st.state_code::text, ''))) = upper(trim(src.state_code))
                      )
                      ORDER BY d.district_id ASC
                      LIMIT 1
                    ) match ON TRUE
                    """
                ).format(self._tbl("district_boundary"), feature_tbl, self._tbl("district"), self._tbl("state")),
                (version_id,),
            )
            return
        if boundary_type == "mukim_boundary":
            cur.execute(sql.SQL("DELETE FROM {}").format(self._tbl("mukim_boundary")))
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (state_id, district_id, state_code, district_code, district_name, mukim_code, mukim_name, mukim_id, boundary_geom)
                    SELECT
                      match.state_id,
                      match.district_id,
                      src.state_code,
                      src.district_code,
                      src.district_name,
                      src.mukim_code,
                      src.mukim_name,
                      coalesce(match.mukim_id, src.mukim_id),
                      src.boundary_geom
                    FROM (
                      SELECT
                        nullif(trim(feature_payload->>'state_code'), '') AS state_code,
                        nullif(trim(feature_payload->>'district_code'), '') AS district_code,
                        upper(nullif(trim(feature_payload->>'district_name'), '')) AS district_name,
                        nullif(trim(feature_payload->>'mukim_code'), '') AS mukim_code,
                        upper(nullif(trim(feature_payload->>'mukim_name'), '')) AS mukim_name,
                        CASE
                          WHEN nullif(trim(feature_payload->>'mukim_id'), '') ~ '^[0-9]+$'
                          THEN (feature_payload->>'mukim_id')::INTEGER
                          ELSE NULL
                        END AS mukim_id,
                        ST_Force2D(ST_GeomFromText(boundary_wkt, 4326)) AS boundary_geom
                      FROM {}
                      WHERE version_id = %s
                    ) src
                    LEFT JOIN LATERAL (
                      SELECT
                        m.mukim_id,
                        d.district_id,
                        st.state_id
                      FROM {} m
                      JOIN {} d
                        ON d.district_id = m.district_id
                      JOIN {} st
                        ON st.state_id = d.state_id
                      WHERE (
                        (
                          src.mukim_id IS NOT NULL
                          AND m.mukim_id = src.mukim_id
                        )
                        OR (
                          src.mukim_id IS NULL
                          AND src.mukim_code IS NOT NULL
                          AND upper(trim(coalesce(m.mukim_code::text, ''))) = upper(trim(src.mukim_code))
                        )
                        OR (
                          src.mukim_id IS NULL
                          AND src.mukim_code IS NULL
                          AND src.mukim_name IS NOT NULL
                          AND upper(trim(coalesce(m.mukim_name::text, ''))) = upper(trim(src.mukim_name))
                        )
                      )
                      AND (
                        src.district_code IS NULL
                        OR upper(trim(coalesce(d.district_code::text, ''))) = upper(trim(src.district_code))
                      )
                      AND (
                        src.state_code IS NULL
                        OR upper(trim(coalesce(st.state_code::text, ''))) = upper(trim(src.state_code))
                      )
                      ORDER BY m.mukim_id ASC
                      LIMIT 1
                    ) match ON TRUE
                    """
                ).format(self._tbl("mukim_boundary"), feature_tbl, self._tbl("mukim"), self._tbl("district"), self._tbl("state")),
                (version_id,),
            )
            return
        if boundary_type == "postcode_boundary":
            cur.execute(sql.SQL("DELETE FROM {}").format(self._tbl("postcode_boundary")))
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (postcode_id, postcode, city, state, boundary_geom)
                    SELECT
                      p.postcode_id,
                      src.postcode,
                      src.city,
                      src.state,
                      src.boundary_geom
                    FROM (
                      SELECT
                        nullif(trim(feature_payload->>'postcode'), '') AS postcode,
                        upper(nullif(trim(feature_payload->>'city'), '')) AS city,
                        upper(nullif(trim(feature_payload->>'state'), '')) AS state,
                        ST_Force2D(ST_GeomFromText(boundary_wkt, 4326)) AS boundary_geom
                      FROM {}
                      WHERE version_id = %s
                    ) src
                    LEFT JOIN LATERAL (
                      SELECT p.postcode_id
                      FROM {} p
                      WHERE trim(coalesce(p.postcode::text, '')) = trim(coalesce(src.postcode::text, ''))
                      ORDER BY p.postcode_id ASC
                      LIMIT 1
                    ) p ON TRUE
                    """
                ).format(self._tbl("postcode_boundary"), feature_tbl, self._tbl("postcode")),
                (version_id,),
            )
            return
        cur.execute(sql.SQL("DELETE FROM {}").format(self._tbl("pbt")))
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} (pbt_id, pbt_name, boundary_geom, is_active, created_at, updated_at)
                SELECT
                  CASE
                    WHEN nullif(trim(feature_payload->>'pbt_id'), '') ~ '^[0-9]+$'
                    THEN (feature_payload->>'pbt_id')::INTEGER
                    ELSE NULL
                  END,
                  nullif(trim(feature_payload->>'pbt_name'), ''),
                  ST_Force2D(ST_GeomFromText(boundary_wkt, 4326)),
                  TRUE,
                  NOW(),
                  NOW()
                FROM {}
                WHERE version_id = %s
                """
            ).format(self._tbl("pbt"), feature_tbl),
            (version_id,),
        )

    def activate_version(self, *, version_id: int, activated_by: str, activation_note: str | None) -> dict[str, Any] | None:
        assert sql is not None
        self.ensure_schema()
        version_tbl = self._tbl("boundary_upload_version")
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT version_id, boundary_type, version_label, status
                    FROM {}
                    WHERE version_id = %s
                    FOR UPDATE
                    """
                ).format(version_tbl),
                (version_id,),
            )
            version_row = cur.fetchone()
            if version_row is None:
                return None
            version = dict(version_row)
            validation = self._validate_features(cur=cur, version_id=version_id)
            if validation["row_count"] == 0:
                raise ValueError("uploaded boundary version has no rows")
            if validation["invalid_geom_count"] > 0:
                raise ValueError("uploaded boundary version contains invalid geometry")
            boundary_type = str(version["boundary_type"])
            self._replace_canonical_table(cur=cur, version_id=version_id, boundary_type=boundary_type)
            cur.execute(
                sql.SQL(
                    """
                    UPDATE {}
                    SET status = 'superseded',
                        updated_at = NOW()
                    WHERE boundary_type = %s
                      AND status = 'active'
                      AND version_id <> %s
                    """
                ).format(version_tbl),
                (boundary_type, version_id),
            )
            cur.execute(
                sql.SQL(
                    """
                    UPDATE {}
                    SET status = 'active',
                        activated_by = %s,
                        source_note = COALESCE(%s, source_note),
                        activated_at = NOW(),
                        updated_at = NOW()
                    WHERE version_id = %s
                    """
                ).format(version_tbl),
                (activated_by, activation_note, version_id),
            )
            conn.commit()
        return self.get_version(version_id=version_id)
