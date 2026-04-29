from typing import Any

from backend.app.repositories.lookup_admin_repository import LookupAdminRepository
from backend.app.services.errors import ServiceError


class LookupAdminService:
    def __init__(self, *, repository: LookupAdminRepository, lookup_schema: str, runtime_schema: str) -> None:
        self._repository = repository
        self._lookup_schema = lookup_schema
        self._runtime_schema = runtime_schema

    def _meta(self) -> dict[str, Any]:
        return {
            "lookup_schema": self._lookup_schema,
            "runtime_schema": self._runtime_schema,
        }

    def _wrap_item(self, item: dict[str, Any]) -> dict[str, Any]:
        return {**item, **self._meta(), "mirrored_to_runtime": True}

    def _list_response(self, *, items: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
        return {**self._meta(), **extra, "count": len(items), "items": items}

    def _handle_lookup_error(self, *, action: str, exc: Exception) -> ServiceError:
        return ServiceError(status_code=503, detail=f"failed to {action}: {exc}")

    def list_states(self) -> dict[str, Any]:
        try:
            items = self._repository.list_states()
        except Exception as exc:
            raise self._handle_lookup_error(action="query states", exc=exc) from exc
        return self._list_response(items=items)

    def list_districts(self, *, state_id: int | None) -> dict[str, Any]:
        try:
            items = self._repository.list_districts(state_id=state_id)
        except Exception as exc:
            raise self._handle_lookup_error(action="query districts", exc=exc) from exc
        return self._list_response(items=items, state_id=state_id)

    def list_mukim(self, *, district_id: int | None, state_id: int | None) -> dict[str, Any]:
        try:
            items = self._repository.list_mukim(district_id=district_id, state_id=state_id)
        except Exception as exc:
            raise self._handle_lookup_error(action="query mukim", exc=exc) from exc
        return self._list_response(items=items, district_id=district_id, state_id=state_id)

    def list_localities(self, *, mukim_id: int | None, district_id: int | None, state_id: int | None) -> dict[str, Any]:
        try:
            items = self._repository.list_localities(mukim_id=mukim_id, district_id=district_id, state_id=state_id)
        except Exception as exc:
            raise self._handle_lookup_error(action="query localities", exc=exc) from exc
        return self._list_response(items=items, mukim_id=mukim_id, district_id=district_id, state_id=state_id)

    def list_postcodes(self, *, locality_id: int | None, mukim_id: int | None, district_id: int | None, state_id: int | None) -> dict[str, Any]:
        try:
            items = self._repository.list_postcodes(
                locality_id=locality_id,
                mukim_id=mukim_id,
                district_id=district_id,
                state_id=state_id,
            )
        except Exception as exc:
            raise self._handle_lookup_error(action="query postcodes", exc=exc) from exc
        return self._list_response(
            items=items,
            locality_id=locality_id,
            mukim_id=mukim_id,
            district_id=district_id,
            state_id=state_id,
        )

    def get_district(self, *, district_id: int) -> dict[str, Any]:
        try:
            row = self._repository.get_district(district_id=district_id)
        except Exception as exc:
            raise self._handle_lookup_error(action="query district", exc=exc) from exc
        if row is None:
            raise ServiceError(status_code=404, detail="district_id not found")
        return self._wrap_item(row)

    def get_mukim(self, *, mukim_id: int) -> dict[str, Any]:
        try:
            row = self._repository.get_mukim(mukim_id=mukim_id)
        except Exception as exc:
            raise self._handle_lookup_error(action="query mukim", exc=exc) from exc
        if row is None:
            raise ServiceError(status_code=404, detail="mukim_id not found")
        return self._wrap_item(row)

    def get_locality(self, *, locality_id: int) -> dict[str, Any]:
        try:
            row = self._repository.get_locality(locality_id=locality_id)
        except Exception as exc:
            raise self._handle_lookup_error(action="query locality", exc=exc) from exc
        if row is None:
            raise ServiceError(status_code=404, detail="locality_id not found")
        return self._wrap_item(row)

    def get_postcode(self, *, postcode_id: int) -> dict[str, Any]:
        try:
            row = self._repository.get_postcode(postcode_id=postcode_id)
        except Exception as exc:
            raise self._handle_lookup_error(action="query postcode", exc=exc) from exc
        if row is None:
            raise ServiceError(status_code=404, detail="postcode_id not found")
        return self._wrap_item(row)

    def create_district(self, *, district_name: str, district_code: str, state_id: int) -> dict[str, Any]:
        name = district_name.strip().upper()
        code = district_code.strip().upper()
        if not name:
            raise ServiceError(status_code=400, detail="district_name is required")
        if not code:
            raise ServiceError(status_code=400, detail="district_code is required")
        if state_id <= 0:
            raise ServiceError(status_code=400, detail="state_id must be positive")
        try:
            row = self._repository.create_district(district_name=name, district_code=code, state_id=state_id)
        except ValueError as exc:
            raise ServiceError(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise self._handle_lookup_error(action="create district", exc=exc) from exc
        return self._wrap_item(row)

    def update_district(self, *, district_id: int, district_name: str | None, district_code: str | None, state_id: int | None) -> dict[str, Any]:
        name = district_name.strip().upper() if district_name is not None else None
        code = district_code.strip().upper() if district_code is not None else None
        if name is not None and not name:
            raise ServiceError(status_code=400, detail="district_name must not be empty")
        if code is not None and not code:
            raise ServiceError(status_code=400, detail="district_code must not be empty")
        if state_id is not None and state_id <= 0:
            raise ServiceError(status_code=400, detail="state_id must be positive")
        if name is None and code is None and state_id is None:
            raise ServiceError(status_code=400, detail="no district fields were provided to update")
        try:
            row = self._repository.update_district(
                district_id=district_id,
                district_name=name,
                district_code=code,
                state_id=state_id,
            )
        except ValueError as exc:
            raise ServiceError(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise self._handle_lookup_error(action="update district", exc=exc) from exc
        if row is None:
            raise ServiceError(status_code=404, detail="district_id not found")
        return self._wrap_item(row)

    def create_mukim(self, *, mukim_name: str, mukim_code: str, district_id: int) -> dict[str, Any]:
        name = mukim_name.strip().upper()
        code = mukim_code.strip().upper()
        if not name:
            raise ServiceError(status_code=400, detail="mukim_name is required")
        if not code:
            raise ServiceError(status_code=400, detail="mukim_code is required")
        if district_id <= 0:
            raise ServiceError(status_code=400, detail="district_id must be positive")
        try:
            row = self._repository.create_mukim(mukim_name=name, mukim_code=code, district_id=district_id)
        except ValueError as exc:
            raise ServiceError(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise self._handle_lookup_error(action="create mukim", exc=exc) from exc
        return self._wrap_item(row)

    def update_mukim(self, *, mukim_id: int, mukim_name: str | None, mukim_code: str | None, district_id: int | None) -> dict[str, Any]:
        name = mukim_name.strip().upper() if mukim_name is not None else None
        code = mukim_code.strip().upper() if mukim_code is not None else None
        if name is not None and not name:
            raise ServiceError(status_code=400, detail="mukim_name must not be empty")
        if code is not None and not code:
            raise ServiceError(status_code=400, detail="mukim_code must not be empty")
        if district_id is not None and district_id <= 0:
            raise ServiceError(status_code=400, detail="district_id must be positive")
        if name is None and code is None and district_id is None:
            raise ServiceError(status_code=400, detail="no mukim fields were provided to update")
        try:
            row = self._repository.update_mukim(
                mukim_id=mukim_id,
                mukim_name=name,
                mukim_code=code,
                district_id=district_id,
            )
        except ValueError as exc:
            raise ServiceError(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise self._handle_lookup_error(action="update mukim", exc=exc) from exc
        if row is None:
            raise ServiceError(status_code=404, detail="mukim_id not found")
        return self._wrap_item(row)

    def create_locality(self, *, locality_name: str, locality_code: str | None, mukim_id: int | None) -> dict[str, Any]:
        name = locality_name.strip().upper()
        code = locality_code.strip().upper() if locality_code is not None else None
        if not name:
            raise ServiceError(status_code=400, detail="locality_name is required")
        if mukim_id is not None and mukim_id <= 0:
            raise ServiceError(status_code=400, detail="mukim_id must be positive")
        try:
            row = self._repository.create_locality(locality_name=name, locality_code=code, mukim_id=mukim_id)
        except ValueError as exc:
            raise ServiceError(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise self._handle_lookup_error(action="create locality", exc=exc) from exc
        return self._wrap_item(row)

    def update_locality(self, *, locality_id: int, locality_name: str | None, locality_code: str | None, mukim_id: int | None) -> dict[str, Any]:
        name = locality_name.strip().upper() if locality_name is not None else None
        code = locality_code.strip().upper() if locality_code is not None else None
        if name is not None and not name:
            raise ServiceError(status_code=400, detail="locality_name must not be empty")
        if mukim_id is not None and mukim_id <= 0:
            raise ServiceError(status_code=400, detail="mukim_id must be positive")
        if name is None and locality_code is None and mukim_id is None:
            raise ServiceError(status_code=400, detail="no locality fields were provided to update")
        try:
            row = self._repository.update_locality(
                locality_id=locality_id,
                locality_name=name,
                locality_code=code if locality_code is not None else None,
                mukim_id=mukim_id,
            )
        except ValueError as exc:
            raise ServiceError(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise self._handle_lookup_error(action="update locality", exc=exc) from exc
        if row is None:
            raise ServiceError(status_code=404, detail="locality_id not found")
        return self._wrap_item(row)

    def create_postcode(self, *, postcode_name: str, postcode: str, locality_id: int | None) -> dict[str, Any]:
        name = postcode_name.strip().upper()
        value = postcode.strip()
        if not name:
            raise ServiceError(status_code=400, detail="postcode_name is required")
        if not value:
            raise ServiceError(status_code=400, detail="postcode is required")
        if locality_id is not None and locality_id <= 0:
            raise ServiceError(status_code=400, detail="locality_id must be positive")
        try:
            row = self._repository.create_postcode(postcode_name=name, postcode=value, locality_id=locality_id)
        except ValueError as exc:
            raise ServiceError(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise self._handle_lookup_error(action="create postcode", exc=exc) from exc
        return self._wrap_item(row)

    def update_postcode(self, *, postcode_id: int, postcode_name: str | None, postcode: str | None, locality_id: int | None) -> dict[str, Any]:
        name = postcode_name.strip().upper() if postcode_name is not None else None
        value = postcode.strip() if postcode is not None else None
        if name is not None and not name:
            raise ServiceError(status_code=400, detail="postcode_name must not be empty")
        if value is not None and not value:
            raise ServiceError(status_code=400, detail="postcode must not be empty")
        if locality_id is not None and locality_id <= 0:
            raise ServiceError(status_code=400, detail="locality_id must be positive")
        if name is None and value is None and locality_id is None:
            raise ServiceError(status_code=400, detail="no postcode fields were provided to update")
        try:
            row = self._repository.update_postcode(
                postcode_id=postcode_id,
                postcode_name=name,
                postcode=value,
                locality_id=locality_id,
            )
        except ValueError as exc:
            raise ServiceError(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise self._handle_lookup_error(action="update postcode", exc=exc) from exc
        if row is None:
            raise ServiceError(status_code=404, detail="postcode_id not found")
        return self._wrap_item(row)
