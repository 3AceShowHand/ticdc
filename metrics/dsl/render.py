# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Renderer from immutable DSL specs to Grafana JSON dictionaries."""

from __future__ import annotations

from collections.abc import Callable
from typing import Final

from metrics.dashboard_identity import DATASOURCE, DATASOURCE_INPUT

from .specs import (
    CustomVarSpec,
    DashboardSpec,
    GraphPanelSpec,
    HeatmapPanelSpec,
    PanelSpecLike,
    RowSpec,
    TablePanelSpec,
    TargetSpec,
    VariableSpecLike,
)

ROW_WIDTH: Final = 24
DEFAULT_TIME_RANGE: Final = {"from": "now-1h", "to": "now"}
DEFAULT_TIMEPICKER: Final = {
    "refresh_intervals": [
        "5s",
        "10s",
        "30s",
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "1d",
    ],
    "time_options": ["5m", "15m", "1h", "6h", "12h", "24h", "2d", "7d", "30d"],
}
DEFAULT_GRAPH_TOOLTIP: Final = {"shared": True, "sort": 0, "value_type": "individual"}
DEFAULT_GRAPH_LEGEND: Final = {
    "alignAsTable": True,
    "avg": False,
    "current": True,
    "max": True,
    "min": False,
    "rightSide": False,
    "show": True,
    "sort": "current",
    "sortDesc": True,
    "total": False,
    "values": True,
}
PanelIdResolver = Callable[[str, PanelSpecLike, int, set[int]], int]
STANDARD_VARIABLE_QUERY_REF_ID: Final = "StandardVariableQuery"


def _stringify(value: str | int | float | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _grid_pos(*, width: int, height: int, x: int, y: int) -> dict[str, int]:
    return {"h": height, "w": width, "x": x, "y": y}


def _dashboard_inputs() -> list[dict[str, object]]:
    return [dict(DATASOURCE_INPUT)]


def _graph_yaxes(spec: GraphPanelSpec) -> list[dict[str, object]]:
    return [
        {
            "decimals": spec.decimals,
            "format": spec.unit,
            "logBase": 1,
            "max": _stringify(spec.max),
            "min": _stringify(spec.min),
            "show": True,
        },
        {
            "format": "short",
            "logBase": 1,
            "max": None,
            "min": None,
            "show": False,
        },
    ]


def render_target(spec: TargetSpec) -> dict[str, object]:
    data: dict[str, object] = {
        "expr": spec.expr,
        "refId": spec.ref,
        "hide": spec.hide,
    }
    if spec.legend is not None:
        data["legendFormat"] = spec.legend
    if spec.format is not None:
        data["format"] = spec.format
    if spec.instant is not None:
        data["instant"] = spec.instant
    return data


def _render_query_variable_current(spec) -> dict[str, object]:
    if spec.include_all:
        return {"selected": False, "text": "All", "value": "$__all"}
    return {"isNone": True, "selected": False, "text": "None", "value": ""}


def _render_query_variable_definition(spec) -> str:
    if spec.include_all:
        return spec.query
    return ""


def _render_query_variable_ref_id(spec) -> str:
    if spec.multi:
        return STANDARD_VARIABLE_QUERY_REF_ID
    return f"local-{spec.name}-Variable-Query"


def render_variable(spec: VariableSpecLike) -> dict[str, object]:
    if isinstance(spec, CustomVarSpec):
        first_option = spec.options[0] if spec.options else ""
        options = []
        if spec.include_all:
            options.append({"selected": True, "text": "All", "value": "$__all"})
        options.extend(
            {"selected": False, "text": option, "value": option} for option in spec.options
        )
        return {
            "allValue": spec.all_value,
            "current": {
                "selected": spec.include_all,
                "text": "All" if spec.include_all else first_option,
                "value": "$__all" if spec.include_all else first_option,
            },
            "description": None,
            "error": None,
            "hide": spec.hide,
            "includeAll": spec.include_all,
            "label": spec.label,
            "multi": False,
            "name": spec.name,
            "options": options,
            "query": ", ".join(spec.options),
            "queryValue": "",
            "skipUrlSync": False,
            "type": "custom",
        }
    return {
        "allValue": spec.all_value,
        "current": _render_query_variable_current(spec),
        "datasource": DATASOURCE,
        "definition": _render_query_variable_definition(spec),
        "description": None,
        "error": None,
        "hide": spec.hide,
        "includeAll": spec.include_all,
        "label": spec.label,
        "multi": spec.multi,
        "name": spec.name,
        "options": [],
        "query": {
            "query": spec.query,
            "refId": _render_query_variable_ref_id(spec),
        },
        "refresh": 2,
        "regex": spec.regex,
        "skipUrlSync": False,
        "sort": spec.sort,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False,
    }


def render_graph_panel(
    spec: GraphPanelSpec,
    *,
    panel_id: int,
    x: int = 0,
    y: int = 0,
) -> dict[str, object]:
    return {
        "datasource": DATASOURCE,
        "description": spec.description,
        "fieldConfig": {"defaults": {"links": []}, "overrides": []},
        "fill": spec.fill,
        "gridPos": _grid_pos(width=spec.span, height=spec.height, x=x, y=y),
        "id": panel_id,
        "legend": dict(DEFAULT_GRAPH_LEGEND),
        "lines": True,
        "options": {"alertThreshold": True},
        "targets": [render_target(target) for target in spec.targets],
        "title": spec.title,
        "tooltip": dict(DEFAULT_GRAPH_TOOLTIP),
        "type": "graph",
        "xaxis": {
            "buckets": None,
            "mode": "time",
            "name": None,
            "show": True,
            "values": [],
        },
        "yaxes": _graph_yaxes(spec),
    }


def render_heatmap_panel(
    spec: HeatmapPanelSpec,
    *,
    panel_id: int,
    x: int = 0,
    y: int = 0,
) -> dict[str, object]:
    return {
        "datasource": DATASOURCE,
        "description": spec.description,
        "fieldConfig": {"defaults": {}, "overrides": []},
        "gridPos": _grid_pos(width=spec.span, height=spec.height, x=x, y=y),
        "id": panel_id,
        "legend": {"show": True},
        "targets": [render_target(target) for target in spec.targets],
        "title": spec.title,
        "tooltip": {"show": True},
        "type": "heatmap",
        "yAxis": {
            "decimals": 1,
            "format": spec.unit,
            "logBase": 1,
            "max": None,
            "min": None,
            "show": True,
        },
    }


def render_table_panel(
    spec: TablePanelSpec,
    *,
    panel_id: int,
    x: int = 0,
    y: int = 0,
) -> dict[str, object]:
    return {
        "datasource": DATASOURCE,
        "description": spec.description,
        "fieldConfig": {
            "defaults": {
                "custom": {"align": None, "filterable": False},
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                        {"color": "red", "value": 80},
                    ],
                },
            },
            "overrides": [],
        },
        "gridPos": _grid_pos(width=spec.span, height=spec.height, x=x, y=y),
        "id": panel_id,
        "options": {"showHeader": True, "sortBy": []},
        "targets": [render_target(target) for target in spec.targets],
        "title": spec.title,
        "transformations": [
            {"id": transformation.id, "options": dict(transformation.options)}
            for transformation in spec.transformations
        ],
        "type": "table",
    }


def render_panel(
    panel_spec: PanelSpecLike,
    *,
    panel_id: int,
    x: int,
    y: int,
) -> dict[str, object]:
    if isinstance(panel_spec, GraphPanelSpec):
        return render_graph_panel(panel_spec, panel_id=panel_id, x=x, y=y)
    if isinstance(panel_spec, HeatmapPanelSpec):
        return render_heatmap_panel(panel_spec, panel_id=panel_id, x=x, y=y)
    if isinstance(panel_spec, TablePanelSpec):
        return render_table_panel(panel_spec, panel_id=panel_id, x=x, y=y)
    raise TypeError(f"unsupported panel spec: {type(panel_spec)!r}")


def render_row(
    spec: RowSpec,
    *,
    row_index: int,
    start_panel_id: int,
    panel_id_resolver: PanelIdResolver | None = None,
    used_panel_ids: set[int] | None = None,
) -> dict[str, object]:
    panels: list[dict[str, object]] = []
    x = 0
    y = 0
    line_height = 0
    next_panel_id = start_panel_id
    used_ids = set() if used_panel_ids is None else used_panel_ids

    for panel_spec in spec.panels:
        if panel_spec.x is None:
            if x + panel_spec.span > ROW_WIDTH:
                y += line_height
                x = 0
                line_height = 0
            panel_x = x
        else:
            panel_x = panel_spec.x
            if panel_x + panel_spec.span > ROW_WIDTH:
                raise ValueError(f"panel {panel_spec.title!r} exceeds row width")
            if x != 0 and panel_x < x:
                y += line_height
                x = 0
                line_height = 0

        panel_id = next_panel_id
        if panel_id_resolver is not None:
            panel_id = panel_id_resolver(
                spec.title,
                panel_spec,
                next_panel_id,
                used_ids,
            )
        panel = render_panel(panel_spec, panel_id=panel_id, x=panel_x, y=y)
        panels.append(panel)
        used_ids.add(panel_id)
        x = panel_x + panel_spec.span
        line_height = max(line_height, panel_spec.height)
        next_panel_id += 1

    return {
        "collapsed": spec.collapsed,
        "gridPos": _grid_pos(width=ROW_WIDTH, height=1, x=0, y=row_index),
        "panels": panels,
        "title": spec.title,
        "type": "row",
    }


def render_dashboard(
    spec: DashboardSpec,
    *,
    panel_id_resolver: PanelIdResolver | None = None,
) -> dict[str, object]:
    panels = []
    next_panel_id = 1
    used_panel_ids: set[int] = set()
    for row_index, row_spec in enumerate(spec.rows):
        panels.append(
            render_row(
                row_spec,
                row_index=row_index,
                start_panel_id=next_panel_id,
                panel_id_resolver=panel_id_resolver,
                used_panel_ids=used_panel_ids,
            )
        )
        next_panel_id += len(row_spec.panels)

    return {
        "__inputs": _dashboard_inputs(),
        "annotations": {"list": list(spec.annotations)},
        "editable": True,
        "graphTooltip": 1,
        "panels": panels,
        "refresh": spec.refresh,
        "schemaVersion": 27,
        "templating": {"list": [render_variable(variable) for variable in spec.variables]},
        "time": dict(DEFAULT_TIME_RANGE),
        "timepicker": dict(DEFAULT_TIMEPICKER),
        "title": spec.title,
        "uid": spec.uid,
        "version": spec.version,
    }
