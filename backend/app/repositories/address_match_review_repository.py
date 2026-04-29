from typing import Any

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    sql = None
    dict_row = None


class AddressMatchReviewRepository:
    def __init__(self, *, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for AddressMatchReviewRepository")
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _tbl(self, name: str):
        assert sql is not None
        return sql.SQL("{}.{}").format(sql.Identifier(self._schema), sql.Identifier(name))

    def _review_select(self):
        assert sql is not None
        return sql.SQL(
            """
            SELECT
              r.review_id,
              r.candidate_canonical_address_key,
              r.candidate_checksum,
              r.candidate_raw_address_variant,
              r.candidate_normalized_address_variant,
              r.candidate_state_id,
              r.candidate_district_id,
              r.candidate_postcode_id,
              r.candidate_street_id,
              r.candidate_locality_id,
              r.matched_address_id,
              r.matched_canonical_address_key,
              r.match_score,
              r.match_reasons,
              r.review_status,
              r.reviewed_by,
              r.reviewed_at,
              r.review_note,
              r.created_at,
              r.updated_at,
              n.code AS matched_naskod,
              concat_ws(
                ', ',
                sa.premise_no,
                sa.building_name,
                st.street_name_prefix,
                st.street_name,
                l.locality_name,
                d.district_name,
                s.state_name,
                p.postcode
              ) AS matched_address_summary
            FROM {review} r
            LEFT JOIN {sa} sa
              ON sa.address_id = r.matched_address_id
            LEFT JOIN {naskod} n
              ON n.address_id = r.matched_address_id
             AND n.is_vanity = false
            LEFT JOIN {street} st
              ON st.street_id = sa.street_id
            LEFT JOIN {locality} l
              ON l.locality_id = sa.locality_id
            LEFT JOIN {district} d
              ON d.district_id = sa.district_id
            LEFT JOIN {state} s
              ON s.state_id = sa.state_id
            LEFT JOIN {postcode} p
              ON p.postcode_id = sa.postcode_id
            """
        ).format(
            review=self._tbl("address_match_review"),
            sa=self._tbl("standardized_address"),
            naskod=self._tbl("naskod"),
            street=self._tbl("street"),
            locality=self._tbl("locality"),
            district=self._tbl("district"),
            state=self._tbl("state"),
            postcode=self._tbl("postcode"),
        )

    def list_reviews(self, *, status: str | None, limit: int) -> list[dict[str, Any]]:
        assert sql is not None
        stmt = sql.SQL(
            """
            {base}
            WHERE (%s IS NULL OR lower(r.review_status) = %s)
            ORDER BY
              CASE lower(r.review_status)
                WHEN 'open' THEN 0
                ELSE 1
              END,
              r.updated_at DESC,
              r.review_id DESC
            LIMIT %s
            """
        ).format(base=self._review_select())
        status_norm = (status or "").strip().lower() or None
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (status_norm, status_norm, limit))
            return [dict(row) for row in cur.fetchall()]

    def get_review(self, *, review_id: int) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            {base}
            WHERE r.review_id = %s
            LIMIT 1
            """
        ).format(base=self._review_select())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (review_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def _get_review_for_update(self, *, cur, review_id: int) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              review_id,
              matched_address_id,
              candidate_raw_address_variant,
              candidate_normalized_address_variant,
              review_status
            FROM {review}
            WHERE review_id = %s
            FOR UPDATE
            """
        ).format(review=self._tbl("address_match_review"))
        cur.execute(stmt, (review_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def approve_merge(self, *, review_id: int, reviewed_by: str, review_note: str | None) -> dict[str, Any] | None:
        assert sql is not None
        review_tbl = self._tbl("address_match_review")
        alias_tbl = self._tbl("address_alias")
        with self._connect() as conn, conn.cursor() as cur:
            row = self._get_review_for_update(cur=cur, review_id=review_id)
            if row is None:
                return None
            current_status = str(row.get("review_status") or "").strip().lower()
            if current_status != "approved_merge":
                cur.execute(
                    sql.SQL(
                        """
                        UPDATE {review}
                        SET
                          review_status = 'approved_merge',
                          reviewed_by = %s,
                          reviewed_at = NOW(),
                          review_note = %s,
                          updated_at = NOW()
                        WHERE review_id = %s
                        """
                    ).format(review=review_tbl),
                    (reviewed_by, review_note, review_id),
                )
                normalized_variant = str(row.get("candidate_normalized_address_variant") or "").strip()
                if normalized_variant:
                    cur.execute(
                        sql.SQL(
                            """
                            INSERT INTO {alias} (
                              alias_id,
                              address_id,
                              raw_address_variant,
                              normalized_address_variant,
                              alias_checksum,
                              created_at,
                              updated_at
                            )
                            SELECT
                              coalesce((SELECT MAX(alias_id) FROM {alias}), 0) + 1,
                              %s,
                              %s,
                              %s,
                              encode(digest(%s, 'sha256'), 'hex'),
                              NOW(),
                              NOW()
                            ON CONFLICT DO NOTHING
                            """
                        ).format(alias=alias_tbl),
                        (
                            int(row["matched_address_id"]),
                            row.get("candidate_raw_address_variant"),
                            normalized_variant,
                            normalized_variant,
                        ),
                    )
            conn.commit()
        return self.get_review(review_id=review_id)

    def reject_review(self, *, review_id: int, reviewed_by: str, review_note: str | None) -> dict[str, Any] | None:
        assert sql is not None
        review_tbl = self._tbl("address_match_review")
        with self._connect() as conn, conn.cursor() as cur:
            row = self._get_review_for_update(cur=cur, review_id=review_id)
            if row is None:
                return None
            if str(row.get("review_status") or "").strip().lower() != "rejected_new_address":
                cur.execute(
                    sql.SQL(
                        """
                        UPDATE {review}
                        SET
                          review_status = 'rejected_new_address',
                          reviewed_by = %s,
                          reviewed_at = NOW(),
                          review_note = %s,
                          updated_at = NOW()
                        WHERE review_id = %s
                        """
                    ).format(review=review_tbl),
                    (reviewed_by, review_note, review_id),
                )
            conn.commit()
        return self.get_review(review_id=review_id)
