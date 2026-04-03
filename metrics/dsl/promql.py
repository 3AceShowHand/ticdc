# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Thin, author-friendly helpers for PromQL strings.

This layer is intentionally optional. Row authors can stay on the higher-level
`metrics.builders` + `metrics.queries` workflow when that reads more clearly.
"""

from __future__ import annotations

import textwrap
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LabelMatcher:
    """One Prometheus label matcher."""

    name: str
    op: str
    value: str

    def render(self) -> str:
        return f'{self.name}{self.op}"{self.value}"'


def promql(expr: str) -> str:
    """Normalize a multiline PromQL string for readable authoring."""

    return textwrap.dedent(expr).strip()


def eq(name: str, value: str) -> LabelMatcher:
    return LabelMatcher(name=name, op="=", value=value)


def regex(name: str, value: str) -> LabelMatcher:
    return LabelMatcher(name=name, op="=~", value=value)


def neq(name: str, value: str) -> LabelMatcher:
    return LabelMatcher(name=name, op="!=", value=value)


def not_regex(name: str, value: str) -> LabelMatcher:
    return LabelMatcher(name=name, op="!~", value=value)


def selector(metric: str, *matchers: LabelMatcher) -> str:
    """Render `metric{...}` using normalized matcher formatting."""

    if not matchers:
        return metric
    rendered = ", ".join(matcher.render() for matcher in matchers)
    return f"{metric}{{{rendered}}}"


def legend_for(
    *labels: str,
    prefix: str | None = None,
    suffix: str | None = None,
    separator: str = "-",
) -> str:
    """Build a Grafana legend from grouped labels."""

    parts: list[str] = []
    if prefix:
        parts.append(prefix)
    parts.extend(f"{{{{{label}}}}}" for label in labels)
    if suffix:
        parts.append(suffix)
    return separator.join(parts)


def _by_clause(labels: Sequence[str]) -> str:
    if not labels:
        return ""
    return f" by ({', '.join(labels)})"


def aggregate(op: str, expr: str, *labels: str) -> str:
    return f"{op}({expr}){_by_clause(labels)}"


def sum_by(expr: str, *labels: str) -> str:
    return aggregate("sum", expr, *labels)


def rate(expr: str, window: str) -> str:
    return f"rate({expr}[{window}])"


def delta(expr: str, window: str) -> str:
    return f"delta({expr}[{window}])"


def increase(expr: str, window: str) -> str:
    return f"increase({expr}[{window}])"


def sum_rate(
    metric: str,
    *,
    matchers: Sequence[LabelMatcher] = (),
    by: Sequence[str] = (),
    window: str = "1m",
) -> str:
    return sum_by(rate(selector(metric, *matchers), window), *by)
