from dataclasses import dataclass
from typing import Any

from nas_core.config.config_loader import load_config
from ..repository.boundary_repository import load_admin_boundaries, load_pbt_boundaries, load_postcode_boundaries
from ..repository.lookup_repository import load_lookup_frames


@dataclass(frozen=True)
class ReferenceData:
    config: dict[str, Any]
    lookups: Any
    postcode_boundaries: Any
    admin_boundaries: tuple[Any, Any, Any]
    pbt_boundaries: Any


def load_reference_data(config_path: str | None) -> ReferenceData:
    config = load_config(config_path)
    return ReferenceData(
        config=config,
        lookups=load_lookup_frames(config=config),
        postcode_boundaries=load_postcode_boundaries(config),
        admin_boundaries=load_admin_boundaries(config),
        pbt_boundaries=load_pbt_boundaries(config),
    )
