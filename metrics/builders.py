# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Mutable authoring builders used by row files.

Use this layer when writing dashboard content:

- `dashboard()` only in `metrics/dashboard.py`
- `row()` inside one row module
- `graph()` / `heatmap()` / `table()` to create a panel
- `panel.add_query(...)` or a query shortcut to attach PromQL

The goal is to keep row files in a readable "create panel, add queries, attach
panel to row" style instead of hand-writing low-level spec objects.

For existing panels, keep one extra rule in mind: unless a panel already has an
explicit `key=...`, its local variable name becomes the stable panel identity
used for checked-in Grafana panel IDs. Do not casually rename existing panel
variables.
"""

from __future__ import annotations

import ast
import inspect
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import cache
from pathlib import Path
from typing import Generic, Literal, Self, TypeVar, cast

from metrics.dsl.api import dashboard as build_dashboard_spec
from metrics.dsl.api import graph as build_graph_panel
from metrics.dsl.api import heatmap as build_heatmap_panel
from metrics.dsl.api import row as build_row_spec
from metrics.dsl.api import table as build_table_panel
from metrics.dsl.api import transformation as build_transformation
from metrics.dsl.render import ROW_WIDTH
from metrics.dsl.specs import (
    Annotation,
    DashboardSpec,
    PanelSpecLike,
    RowSpec,
    TargetSpec,
    TransformationSpec,
    VariableSpecLike,
)
from metrics.queries import (
    Expr,
    LabelSeq,
    ScopeName,
    SelectorSeq,
    expr_histogram_avg,
    expr_histogram_quantile,
    legend_for,
)
from metrics.queries import target as build_target

DEFAULT_PANEL_HEIGHT = 7
LineAlign = Literal["left", "right"]


class _UnsetType:
    pass


_UNSET = _UnsetType()
_LABEL_TABLE_HIDDEN_COLUMNS = ("Metric", "Time", "Value", "__name__")
_PANEL_FACTORY_NAMES = {"graph", "heatmap", "table"}


def _next_ref(index: int) -> str:
    if index < 0:
        raise ValueError("query index must be non-negative")

    ref = []
    value = index + 1
    while value > 0:
        value, remainder = divmod(value - 1, 26)
        ref.append(chr(ord("A") + remainder))
    return "".join(reversed(ref))


def _next_available_ref(targets: list[TargetSpec]) -> str:
    used = {target.ref for target in targets}
    index = 0
    while True:
        candidate = _next_ref(index)
        if candidate not in used:
            return candidate
        index += 1


def _quantile_suffix(quantile: float) -> str:
    quantile_text = f"{quantile:.12g}"
    if quantile_text.startswith("0."):
        quantile_text = quantile_text[2:]
    return f"p{quantile_text.replace('.', '').rstrip('0') or '0'}"


def _label_table_transformations(
    columns: Sequence[str],
    *,
    rename: Mapping[str, str] | None = None,
) -> list[TransformationSpec]:
    rename_by_name = {} if rename is None else dict(rename)
    return [
        build_transformation("labelsToFields"),
        build_transformation(
            "organize",
            {
                "excludeByName": {name: True for name in _LABEL_TABLE_HIDDEN_COLUMNS},
                "indexByName": {name: index for index, name in enumerate(columns)},
                "renameByName": rename_by_name,
            },
        ),
    ]


def _called_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _contains_panel_factory_call(node: ast.AST) -> bool:
    return any(
        isinstance(child, ast.Call) and _called_name(child.func) in _PANEL_FACTORY_NAMES
        for child in ast.walk(node)
    )


@cache
def _panel_assignment_ranges(path: str, function_name: str) -> tuple[tuple[int, int, str], ...]:
    try:
        source = Path(path).read_text(encoding="utf-8")
    except OSError:
        return ()

    try:
        module = ast.parse(source, filename=path)
    except SyntaxError:
        return ()

    for node in module.body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if node.name != function_name:
            continue

        assignments: list[tuple[int, int, str]] = []
        for child in ast.walk(node):
            target_name: str | None = None
            value: ast.AST | None = None
            if isinstance(child, ast.Assign) and len(child.targets) == 1:
                target = child.targets[0]
                if isinstance(target, ast.Name):
                    target_name = target.id
                    value = child.value
            elif isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
                target_name = child.target.id
                value = child.value

            if target_name is None or value is None or not _contains_panel_factory_call(value):
                continue

            start = getattr(value, "lineno", None)
            end = getattr(value, "end_lineno", start)
            if start is None or end is None:
                continue
            assignments.append((start, end, target_name))
        return tuple(sorted(assignments))
    return ()


def _caller_frame(depth: int):
    frame = inspect.currentframe()
    if frame is None:
        return None
    try:
        current = frame
        for _ in range(depth):
            current = current.f_back
            if current is None:
                return None
        return current
    finally:
        del frame


def _infer_panel_key() -> str | None:
    # Stable panel IDs should survive title edits, so the default panel key
    # comes from the local variable name in the row builder function.
    caller = _caller_frame(3)
    if caller is None:
        return None
    try:
        for start, end, target_name in _panel_assignment_ranges(
            caller.f_code.co_filename,
            caller.f_code.co_name,
        ):
            if start <= caller.f_lineno <= end:
                return target_name
        return None
    finally:
        del caller


def _infer_row_key() -> str | None:
    # Row identity should not drift when the visible row title changes, so the
    # default row key comes from `build_xxx_row`.
    caller = _caller_frame(3)
    if caller is None:
        return None
    try:
        function_name = caller.f_code.co_name
    finally:
        del caller

    if function_name.startswith("build_") and function_name.endswith("_row"):
        return function_name.removeprefix("build_").removesuffix("_row")
    return None


@dataclass(frozen=True, slots=True)
class LineLayout:
    widths: tuple[int, ...]
    align: LineAlign = "left"

    def __post_init__(self) -> None:
        if not self.widths:
            raise ValueError("line layout must contain at least one width")
        if sum(self.widths) != ROW_WIDTH:
            raise ValueError("line layout widths must fill the full Grafana row width")
        if any(width <= 0 for width in self.widths):
            raise ValueError("line layout widths must be positive")


class LineLayouts:
    """Named row-line layouts used by `row.add_panel(...)` and `row.add_panels(...)`.

    Most rows rely on the default full / halves / thirds layouts. Keep custom
    layouts rare and tied to a concrete dashboard compatibility need.
    """

    FULL = LineLayout((ROW_WIDTH,))
    HALVES = LineLayout((ROW_WIDTH // 2, ROW_WIDTH // 2))
    HALVES_RIGHT = LineLayout((ROW_WIDTH // 2, ROW_WIDTH // 2), align="right")
    THIRDS = LineLayout((ROW_WIDTH // 3, ROW_WIDTH // 3, ROW_WIDTH // 3))
    QUARTER_QUARTER_HALF = LineLayout((ROW_WIDTH // 4, ROW_WIDTH // 4, ROW_WIDTH // 2))


def _default_layout_for_panel_count(panel_count: int) -> LineLayout:
    if panel_count == 1:
        return LineLayouts.FULL
    if panel_count == 2:
        return LineLayouts.HALVES
    if panel_count == 3:
        return LineLayouts.THIRDS
    raise ValueError("only 1 to 3 panels are supported per row line")


@dataclass(slots=True)
class DashboardBuilder:
    """Collect rows in display order and build the final dashboard spec once."""

    title: str
    uid: str
    variables: list[VariableSpecLike]
    version: int = 1
    annotations: list[Annotation] = field(default_factory=list)
    refresh: str = "10s"
    _rows: list[RowBuilder | RowSpec] = field(default_factory=list)

    def add_row(self, row_spec: RowBuilder | RowSpec) -> Self:
        self._rows.append(row_spec)
        return self

    def build(self) -> DashboardSpec:
        return build_dashboard_spec(
            title=self.title,
            uid=self.uid,
            variables=self.variables,
            version=self.version,
            annotations=self.annotations,
            rows=[
                row_spec.build() if isinstance(row_spec, RowBuilder) else row_spec
                for row_spec in self._rows
            ],
            refresh=self.refresh,
        )


PanelSpecT = TypeVar("PanelSpecT", bound=PanelSpecLike)


@dataclass(slots=True)
class BasePanelBuilder(Generic[PanelSpecT]):
    """Shared mutable panel builder.

    These query helpers are the main author-facing distinction:

    - `add_query()`: keep this panel type's default query behavior
    - `add_auto_query()`: avoid an explicit `format` field
    - `add_range_query()`: force `instant=False`
    """

    title: str
    key: str | None = None
    description: str | None = None
    _targets: list[TargetSpec] = field(default_factory=list)

    def _default_format(self) -> str | None:
        return "time_series"

    def _default_instant(self) -> bool | None:
        return None

    def add_query(
        self,
        expr: str | Expr,
        *,
        legend: str | None = None,
        legend_format: str | None = None,
        ref: str | None = None,
        hide: bool = False,
        format: str | None | object = _UNSET,
        instant: bool | None | object = _UNSET,
    ) -> Self:
        """Add one target while preserving this panel type's default query mode."""

        resolved_format = self._default_format() if format is _UNSET else cast("str | None", format)
        resolved_instant = (
            self._default_instant() if instant is _UNSET else cast("bool | None", instant)
        )
        self._targets.append(
            build_target(
                expr,
                legend=legend,
                legend_format=legend_format,
                ref=ref or _next_available_ref(self._targets),
                hide=hide,
                format=resolved_format,
                instant=resolved_instant,
            )
        )
        return self

    def add_auto_query(
        self,
        expr: str | Expr,
        *,
        legend: str | None = None,
        legend_format: str | None = None,
        ref: str | None = None,
        hide: bool = False,
        instant: bool | None | object = _UNSET,
    ) -> Self:
        """Add a target without pinning an explicit `format` field in output JSON."""

        return self.add_query(
            expr,
            legend=legend,
            legend_format=legend_format,
            ref=ref,
            hide=hide,
            format=None,
            instant=instant,
        )

    def add_range_query(
        self,
        expr: str | Expr,
        *,
        legend: str | None = None,
        legend_format: str | None = None,
        ref: str | None = None,
        hide: bool = False,
        format: str | None | object = _UNSET,
    ) -> Self:
        """Add a range query for cases that must render with `instant=False`."""

        return self.add_query(
            expr,
            legend=legend,
            legend_format=legend_format,
            ref=ref,
            hide=hide,
            format=format,
            instant=False,
        )

    def add_auto_range_query(
        self,
        expr: str | Expr,
        *,
        legend: str | None = None,
        legend_format: str | None = None,
        ref: str | None = None,
        hide: bool = False,
    ) -> Self:
        return self.add_auto_query(
            expr,
            legend=legend,
            legend_format=legend_format,
            ref=ref,
            hide=hide,
            instant=False,
        )

    def build(
        self,
        *,
        width: int | None = None,
        height: int | None = None,
        x: int | None = None,
    ) -> PanelSpecT:
        raise NotImplementedError


@dataclass(slots=True)
class GraphPanelBuilder(BasePanelBuilder):
    """Default builder for numeric time-series panels."""

    unit: str = "short"
    min: str | int | float | None = None
    max: str | int | float | None = None
    decimals: int | None = None

    def add_histogram(
        self,
        metric: str,
        *,
        by_labels: LabelSeq = (),
        scope: ScopeName = "instance",
        selectors: SelectorSeq = (),
        window: str = "1m",
        quantile: float = 0.99,
        quantile_legend: str | None = None,
        average_legend: str | None = None,
        range_query: bool = False,
        format: str | None = None,
    ) -> Self:
        """Add the common `quantile + average` graph series for one histogram."""

        quantile_suffix = _quantile_suffix(quantile)
        quantile_expr = expr_histogram_quantile(
            quantile,
            metric,
            by_labels=by_labels,
            scope=scope,
            selectors=selectors,
            window=window,
        )
        average_expr = expr_histogram_avg(
            metric,
            by_labels=by_labels,
            scope=scope,
            selectors=selectors,
            window=window,
        )
        quantile_legend_format = quantile_legend or legend_for(*by_labels, suffix=quantile_suffix)
        average_legend_format = average_legend or legend_for(*by_labels, suffix="avg")

        if format is None and range_query:
            self.add_auto_range_query(
                quantile_expr,
                legend_format=quantile_legend_format,
            )
            self.add_auto_range_query(
                average_expr,
                legend_format=average_legend_format,
            )
            return self

        if format is None:
            self.add_auto_query(
                quantile_expr,
                legend_format=quantile_legend_format,
            )
            self.add_auto_query(
                average_expr,
                legend_format=average_legend_format,
            )
            return self

        if range_query:
            self.add_range_query(
                quantile_expr,
                legend_format=quantile_legend_format,
                format=format,
            )
            self.add_range_query(
                average_expr,
                legend_format=average_legend_format,
                format=format,
            )
            return self

        self.add_query(
            quantile_expr,
            legend_format=quantile_legend_format,
            format=format,
        )
        self.add_query(
            average_expr,
            legend_format=average_legend_format,
            format=format,
        )
        return self

    def build(
        self,
        *,
        width: int | None = None,
        height: int | None = None,
        x: int | None = None,
    ):
        return build_graph_panel(
            title=self.title,
            targets=self._targets,
            key=self.title if self.key is None else self.key,
            description=self.description,
            unit=self.unit,
            min=self.min,
            max=self.max,
            decimals=self.decimals,
            width=12 if width is None else width,
            height=DEFAULT_PANEL_HEIGHT if height is None else height,
            x=x,
        )


@dataclass(slots=True)
class HeatmapPanelBuilder(BasePanelBuilder):
    """Use only for histogram bucket distributions over time."""

    unit: str = "short"

    def _default_format(self) -> str | None:
        return "heatmap"

    def add_query(
        self,
        expr: str | Expr,
        *,
        legend: str | None = None,
        legend_format: str | None = None,
        ref: str | None = None,
        hide: bool = False,
        format: str | None | object = _UNSET,
        instant: bool | None | object = _UNSET,
    ) -> Self:
        if legend is None and legend_format is None:
            legend_format = "{{le}}"
        BasePanelBuilder.add_query(
            self,
            expr,
            legend=legend,
            legend_format=legend_format,
            ref=ref,
            hide=hide,
            format=format,
            instant=instant,
        )
        return self

    def build(
        self,
        *,
        width: int | None = None,
        height: int | None = None,
        x: int | None = None,
    ):
        return build_heatmap_panel(
            title=self.title,
            targets=self._targets,
            key=self.title if self.key is None else self.key,
            description=self.description,
            unit=self.unit,
            width=12 if width is None else width,
            height=DEFAULT_PANEL_HEIGHT if height is None else height,
            x=x,
        )


@dataclass(slots=True)
class TablePanelBuilder(BasePanelBuilder):
    """Use when labels or detail rows are more readable than a graph."""

    transformations: list[TransformationSpec] = field(default_factory=list)

    def _default_format(self) -> str | None:
        return "time_series"

    def _default_instant(self) -> bool | None:
        return True

    def add_transformation(self, transformation_spec: TransformationSpec) -> Self:
        self.transformations.append(transformation_spec)
        return self

    def add_label_query(
        self,
        expr: str | Expr,
        *,
        columns: Sequence[str],
        rename: Mapping[str, str] | None = None,
        legend: str | None = None,
        legend_format: str | None = None,
        ref: str | None = None,
        hide: bool = False,
    ) -> Self:
        """Turn metric labels into visible columns using the standard transforms."""

        self.add_query(
            expr,
            legend=legend,
            legend_format=legend_format,
            ref=ref,
            hide=hide,
        )
        self.transformations.extend(
            _label_table_transformations(columns, rename=rename),
        )
        return self

    def build(
        self,
        *,
        width: int | None = None,
        height: int | None = None,
        x: int | None = None,
    ):
        return build_table_panel(
            title=self.title,
            targets=self._targets,
            key=self.title if self.key is None else self.key,
            description=self.description,
            width=12 if width is None else width,
            height=DEFAULT_PANEL_HEIGHT if height is None else height,
            x=x,
            transformations=self.transformations,
        )


type PanelInput = GraphPanelBuilder | HeatmapPanelBuilder | TablePanelBuilder | PanelSpecLike


@dataclass(frozen=True, slots=True)
class _LineSpec:
    panels: tuple[PanelInput, ...]
    layout: LineLayout


def _slot_offsets(widths: tuple[int, ...]) -> tuple[int, ...]:
    offsets: list[int] = []
    x = 0
    for width in widths:
        offsets.append(x)
        x += width
    return tuple(offsets)


def _build_panel(
    panel: PanelInput,
    *,
    width: int,
    height: int | None,
    x: int,
) -> PanelSpecLike:
    if isinstance(panel, BasePanelBuilder):
        return panel.build(width=width, height=height, x=x)
    return replace(
        panel,
        span=width,
        height=panel.height if height is None else height,
        x=x,
    )


@dataclass(slots=True)
class RowBuilder:
    """Collect one Grafana row as one or more visual lines of panels.

    Most rows should only need:

    - `add_panel(panel)` for one full-width panel
    - `add_panels(left, right)` for two half-width panels on one line
    - `add_panels(a, b, c)` for three equal-width panels on one line
    """

    title: str
    key: str | None = None
    height: int | None = None
    collapsed: bool = True
    repeat: str | None = None
    _lines: list[_LineSpec] = field(default_factory=list)

    def add_panel(
        self,
        panel: PanelInput,
        *,
        layout: LineLayout | None = None,
    ) -> Self:
        """Add one panel line, or reserve a specific slot layout when needed."""

        return self.add_panels(panel, layout=layout)

    def add_half_panel(
        self,
        panel: PanelInput,
    ) -> Self:
        return self.add_panel(panel, layout=LineLayouts.HALVES)

    def add_panels(
        self,
        *panels: PanelInput,
        layout: LineLayout | None = None,
    ) -> Self:
        """Add 1-3 panels on the same visual line inside this row."""

        if not panels:
            raise ValueError("row line must contain at least one panel")
        resolved_layout = _default_layout_for_panel_count(len(panels)) if layout is None else layout
        if len(panels) > len(resolved_layout.widths):
            raise ValueError("line layout does not have enough slots for the panels")
        self._lines.append(
            _LineSpec(
                panels=tuple(panels),
                layout=resolved_layout,
            )
        )
        return self

    def build(self) -> RowSpec:
        panels: list[PanelSpecLike] = []
        for line in self._lines:
            slot_offsets = _slot_offsets(line.layout.widths)
            start_index = 0
            if line.layout.align == "right":
                start_index = len(line.layout.widths) - len(line.panels)
            for index, panel in enumerate(line.panels):
                slot_index = start_index + index
                panels.append(
                    _build_panel(
                        panel,
                        width=line.layout.widths[slot_index],
                        height=self.height,
                        x=slot_offsets[slot_index],
                    )
                )
        return build_row_spec(
            self.title,
            panels,
            key=self.key if self.key is not None else self.title,
            collapsed=self.collapsed,
            repeat=self.repeat,
        )


def dashboard(
    *,
    title: str,
    uid: str,
    variables: list[VariableSpecLike],
    version: int = 1,
    annotations: list[Annotation] | None = None,
    refresh: str = "10s",
) -> DashboardBuilder:
    """Create the top-level dashboard builder.

    Normal row authors should not call this directly; `metrics/dashboard.py` owns
    dashboard assembly.
    """

    return DashboardBuilder(
        title=title,
        uid=uid,
        variables=list(variables),
        version=version,
        annotations=[] if annotations is None else list(annotations),
        refresh=refresh,
    )


def row(
    title: str,
    *,
    key: str | None = None,
    height: int | None = None,
    collapsed: bool = True,
    repeat: str | None = None,
) -> RowBuilder:
    """Create one row builder.

    Set `height` only when every panel in the row should share a non-default
    height.
    """

    return RowBuilder(
        title=title,
        key=key if key is not None else _infer_row_key(),
        height=height,
        collapsed=collapsed,
        repeat=repeat,
    )


def graph(
    title: str,
    *,
    key: str | None = None,
    description: str | None = None,
    unit: str = "short",
    min: str | int | float | None = None,
    max: str | int | float | None = None,
    decimals: int | None = None,
) -> GraphPanelBuilder:
    """Create the default panel type for most counter/gauge/histogram summaries."""

    return GraphPanelBuilder(
        title=title,
        key=key if key is not None else _infer_panel_key(),
        description=description,
        unit=unit,
        min=min,
        max=max,
        decimals=decimals,
    )


def heatmap(
    title: str,
    *,
    key: str | None = None,
    description: str | None = None,
    unit: str = "short",
) -> HeatmapPanelBuilder:
    """Create a heatmap panel for histogram bucket density over time."""

    return HeatmapPanelBuilder(
        title=title,
        key=key if key is not None else _infer_panel_key(),
        description=description,
        unit=unit,
    )


def table(
    title: str,
    *,
    key: str | None = None,
    description: str | None = None,
) -> TablePanelBuilder:
    """Create a table panel for detail output such as errors or label listings."""

    return TablePanelBuilder(
        title=title,
        key=key if key is not None else _infer_panel_key(),
        description=description,
    )


__all__ = [
    "DashboardBuilder",
    "GraphPanelBuilder",
    "HeatmapPanelBuilder",
    "LineLayout",
    "LineLayouts",
    "RowBuilder",
    "TablePanelBuilder",
    "dashboard",
    "graph",
    "heatmap",
    "row",
    "table",
]
