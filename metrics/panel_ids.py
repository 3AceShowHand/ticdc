# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Stable panel ID registry for Grafana dashboard generation.

Panel IDs should never drift when panels are inserted or removed. We treat
`row.key` + `panel.key` as the stable identity, inherit existing IDs from the
checked-in registry, and only allocate a larger ID for newly introduced panels.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Self

from metrics.dsl.render import PanelIdResolver
from metrics.dsl.specs import DashboardSpec, PanelSpecLike, RowSpec

PANEL_ID_REGISTRY_FILE: Final = "metrics/grafana/panel_ids.json"


@dataclass(frozen=True, slots=True)
class PanelIdEntry:
    row_key: str
    panel_key: str
    id: int
    row_title: str
    panel_title: str


@dataclass(frozen=True, slots=True)
class PanelIdRegistry:
    entries: tuple[PanelIdEntry, ...]
    next_id: int

    @classmethod
    def empty(cls) -> Self:
        return cls(entries=(), next_id=1)


def _row_identity(row_spec: RowSpec) -> str:
    return row_spec.key if row_spec.key is not None else row_spec.title


def _panel_identity(panel_spec: PanelSpecLike) -> str:
    return panel_spec.key if panel_spec.key is not None else panel_spec.title


def _identity_key(row_key: str, panel_key: str) -> tuple[str, str]:
    return (row_key, panel_key)


def load_panel_id_registry(path: Path) -> PanelIdRegistry:
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = tuple(
        PanelIdEntry(
            row_key=item["row_key"],
            panel_key=item["panel_key"],
            id=item["id"],
            row_title=item["row_title"],
            panel_title=item["panel_title"],
        )
        for item in payload.get("panels", [])
    )
    return PanelIdRegistry(entries=entries, next_id=payload["next_id"])


def write_panel_id_registry(path: Path, registry: PanelIdRegistry) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "next_id": registry.next_id,
        "panels": [
            {
                "row_key": entry.row_key,
                "panel_key": entry.panel_key,
                "id": entry.id,
                "row_title": entry.row_title,
                "panel_title": entry.panel_title,
            }
            for entry in sorted(registry.entries, key=lambda item: item.id)
        ],
    }
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def sync_panel_ids(
    spec: DashboardSpec,
    registry: PanelIdRegistry,
) -> PanelIdRegistry:
    entries_by_identity = {
        _identity_key(entry.row_key, entry.panel_key): entry for entry in registry.entries
    }
    seen_row_keys: set[str] = set()
    seen_panel_identities: set[tuple[str, str]] = set()
    next_id = registry.next_id

    for row_spec in spec.rows:
        row_key = _row_identity(row_spec)
        if row_key in seen_row_keys:
            raise ValueError(f"duplicate row identity {row_key!r}")
        seen_row_keys.add(row_key)
        for panel_spec in row_spec.panels:
            panel_key = _panel_identity(panel_spec)
            identity = _identity_key(row_key, panel_key)
            if identity in seen_panel_identities:
                raise ValueError(f"duplicate panel identity {panel_key!r} in row {row_key!r}")
            seen_panel_identities.add(identity)
            entry = entries_by_identity.get(identity)
            if entry is None:
                entries_by_identity[identity] = PanelIdEntry(
                    row_key=row_key,
                    panel_key=panel_key,
                    id=next_id,
                    row_title=row_spec.title,
                    panel_title=panel_spec.title,
                )
                next_id += 1
                continue
            entries_by_identity[identity] = PanelIdEntry(
                row_key=row_key,
                panel_key=panel_key,
                id=entry.id,
                row_title=row_spec.title,
                panel_title=panel_spec.title,
            )

    return PanelIdRegistry(
        entries=tuple(sorted(entries_by_identity.values(), key=lambda item: item.id)),
        next_id=next_id,
    )


def seed_panel_id_registry(
    spec: DashboardSpec,
    dashboard: dict[str, object],
) -> PanelIdRegistry:
    entries: list[PanelIdEntry] = []
    max_id = 0
    dashboard_rows = dashboard.get("panels", [])
    if not isinstance(dashboard_rows, list):
        raise ValueError("dashboard panels must be a list")

    if len(spec.rows) != len(dashboard_rows):
        raise ValueError("dashboard row count does not match the current spec")

    for row_spec, row_json in zip(spec.rows, dashboard_rows, strict=True):
        if not isinstance(row_json, dict):
            raise ValueError("dashboard row payload must be an object")
        if row_json.get("title") != row_spec.title:
            raise ValueError(f"dashboard row title mismatch: {row_spec.title!r}")
        dashboard_panels = row_json.get("panels", [])
        if not isinstance(dashboard_panels, list):
            raise ValueError("dashboard row panels must be a list")
        if len(row_spec.panels) != len(dashboard_panels):
            raise ValueError(f"dashboard panel count mismatch in row {row_spec.title!r}")

        row_key = _row_identity(row_spec)
        for panel_spec, panel_json in zip(row_spec.panels, dashboard_panels, strict=True):
            if not isinstance(panel_json, dict):
                raise ValueError("dashboard panel payload must be an object")
            panel_id = panel_json.get("id")
            if not isinstance(panel_id, int):
                raise ValueError(f"dashboard panel {panel_spec.title!r} is missing an integer id")
            max_id = max(max_id, panel_id)
            entries.append(
                PanelIdEntry(
                    row_key=row_key,
                    panel_key=_panel_identity(panel_spec),
                    id=panel_id,
                    row_title=row_spec.title,
                    panel_title=panel_spec.title,
                )
            )

    return PanelIdRegistry(entries=tuple(entries), next_id=max_id + 1)


def build_panel_id_resolver(registry: PanelIdRegistry) -> PanelIdResolver:
    ids_by_identity = {
        _identity_key(entry.row_key, entry.panel_key): entry.id for entry in registry.entries
    }

    def resolve_panel_id(
        row_key: str,
        panel_spec: PanelSpecLike,
        default_id: int,
        used_ids: set[int],
    ) -> int:
        del default_id
        identity = _identity_key(row_key, _panel_identity(panel_spec))
        panel_id = ids_by_identity.get(identity)
        if panel_id is None:
            panel_key = _panel_identity(panel_spec)
            raise KeyError(f"missing panel id mapping for row {row_key!r} panel {panel_key!r}")
        if panel_id in used_ids:
            raise ValueError(f"duplicate panel id {panel_id} for row {row_key!r}")
        return panel_id

    return resolve_panel_id
