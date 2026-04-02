# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import IntEnum
from typing import Generic, Literal, Self, TypeVar, cast

from metrics.dsl.api import dashboard as build_dashboard_spec
from metrics.dsl.api import row as build_row_spec
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
from metrics.queries import graph_panel as build_graph_panel
from metrics.queries import heatmap_panel as build_heatmap_panel
from metrics.queries import table_panel as build_table_panel
from metrics.queries import target as build_target


class RowHeights(IntEnum):
    NORMAL = 7


DEFAULT_PANEL_HEIGHT = RowHeights.NORMAL
LineAlign = Literal["left", "right"]


class _UnsetType:
    pass


_UNSET = _UnsetType()
_LABEL_TABLE_HIDDEN_COLUMNS = ("Metric", "Time", "Value", "__name__")


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

    def add_instant_query(
        self,
        expr: str | Expr,
        *,
        legend: str | None = None,
        legend_format: str | None = None,
        ref: str | None = None,
        hide: bool = False,
        format: str | None | object = _UNSET,
    ) -> Self:
        return self.add_query(
            expr,
            legend=legend,
            legend_format=legend_format,
            ref=ref,
            hide=hide,
            format=format,
            instant=True,
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
    title: str
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
        return self.add_panels(panel, layout=layout)

    def add_graph(self, panel: PanelInput) -> Self:
        return self.add_panel(panel)

    def add_heatmap(self, panel: PanelInput) -> Self:
        return self.add_panel(panel)

    def add_table(self, panel: PanelInput) -> Self:
        return self.add_panel(panel)

    def add_half_panel(
        self,
        panel: PanelInput,
    ) -> Self:
        return self.add_panel(panel, layout=LineLayouts.HALVES)

    def add_right_half_panel(
        self,
        panel: PanelInput,
    ) -> Self:
        return self.add_panel(panel, layout=LineLayouts.HALVES_RIGHT)

    def add_panels(
        self,
        *panels: PanelInput,
        layout: LineLayout | None = None,
    ) -> Self:
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
    height: int | None = None,
    collapsed: bool = True,
    repeat: str | None = None,
) -> RowBuilder:
    return RowBuilder(
        title=title,
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
    return GraphPanelBuilder(
        title=title,
        key=key,
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
    return HeatmapPanelBuilder(
        title=title,
        key=key,
        description=description,
        unit=unit,
    )


def table(
    title: str,
    *,
    key: str | None = None,
    description: str | None = None,
) -> TablePanelBuilder:
    return TablePanelBuilder(
        title=title,
        key=key,
        description=description,
    )


__all__ = [
    "DashboardBuilder",
    "GraphPanelBuilder",
    "HeatmapPanelBuilder",
    "LineLayout",
    "LineLayouts",
    "RowHeights",
    "RowBuilder",
    "TablePanelBuilder",
    "dashboard",
    "graph",
    "heatmap",
    "row",
    "table",
]
