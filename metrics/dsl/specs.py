# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Immutable spec objects for the primitive Grafana dashboard DSL.

The DSL is intentionally split into two layers:

1. `metrics.dsl.api` provides the author-facing constructor helpers.
2. This module stores the normalized immutable objects that the renderer consumes.

Keeping the spec layer small and explicit makes the authoring API easier to learn
and the renderer easier to reason about.
"""

from __future__ import annotations

from dataclasses import dataclass, field

type Scalar = str | int | float
type ScalarOrNone = Scalar | None
type JsonObject = dict[str, object]


@dataclass(frozen=True, slots=True)
class TargetSpec:
    """One Prometheus query attached to a panel."""

    expr: str
    legend: str | None = None
    ref: str = "A"
    hide: bool = False
    format: str | None = None
    instant: bool | None = None


@dataclass(frozen=True, slots=True)
class TransformationSpec:
    """A Grafana table transformation."""

    id: str
    options: JsonObject = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PanelSpec:
    """Shared fields for all panel types.

    `span` uses Grafana's 24-column row layout.
    `x` is optional and only needed when a panel should start at an explicit
    horizontal offset instead of the next auto-layout position.
    """

    title: str
    targets: list[TargetSpec]
    span: int = 12
    height: int = 7
    description: str | None = None
    x: int | None = None
    key: str | None = None


@dataclass(frozen=True, slots=True)
class GraphPanelSpec(PanelSpec):
    """Specification for classic Grafana `graph` panels."""

    unit: str = "short"
    min: ScalarOrNone = None
    max: ScalarOrNone = None
    decimals: int | None = None
    fill: int = 1


@dataclass(frozen=True, slots=True)
class HeatmapPanelSpec(PanelSpec):
    """Specification for Grafana `heatmap` panels."""

    unit: str = "short"


@dataclass(frozen=True, slots=True)
class TablePanelSpec(PanelSpec):
    """Specification for Grafana `table` panels."""

    transformations: list[TransformationSpec] = field(default_factory=list)


type PanelSpecLike = GraphPanelSpec | HeatmapPanelSpec | TablePanelSpec


@dataclass(frozen=True, slots=True)
class RowSpec:
    """A logical dashboard row."""

    title: str
    panels: list[PanelSpecLike]
    key: str | None = None
    collapsed: bool = True
    repeat: str | None = None


@dataclass(frozen=True, slots=True)
class QueryVarSpec:
    """A query-backed Grafana template variable."""

    name: str
    query: str
    label: str | None = None
    multi: bool = False
    include_all: bool = False
    all_value: str | None = None
    hide: int = 0
    regex: str = ""
    sort: int = 0


@dataclass(frozen=True, slots=True)
class CustomVarSpec:
    """A fixed-option Grafana template variable."""

    name: str
    options: list[str]
    label: str | None = None
    include_all: bool = False
    all_value: str | None = None
    hide: int = 0


type VariableSpecLike = QueryVarSpec | CustomVarSpec
type Annotation = JsonObject


@dataclass(frozen=True, slots=True)
class DashboardSpec:
    """Top-level dashboard specification."""

    title: str
    uid: str
    variables: list[VariableSpecLike]
    rows: list[RowSpec]
    version: int = 1
    annotations: list[Annotation] = field(default_factory=list)
    refresh: str = "10s"
