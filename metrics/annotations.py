# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dashboard_identity import DATASOURCE


def _dashboard_annotation() -> dict[str, object]:
    return {
        "builtIn": 1,
        "datasource": "-- Grafana --",
        "enable": False,
        "expr": "",
        "hide": True,
        "iconColor": "#F2495C",
        "limit": 100,
        "name": "",
        "showIn": 0,
        "tagKeys": "",
        "textFormat": "",
        "titleFormat": "",
        "type": "dashboard",
        "useValueForTime": False,
    }


def _latency_spike_annotation() -> dict[str, object]:
    return {
        "datasource": DATASOURCE,
        "enable": True,
        "expr": (
            "max(ticdc_owner_checkpoint_ts_lag) by (changefeed, instance) > BOOL $spike_threshold"
        ),
        "hide": True,
        "iconColor": "#F2495C",
        "limit": 100,
        "name": "Latency spike",
        "showIn": 0,
        "tagKeys": "changefeed",
        "tags": [],
        "titleFormat": "Latency spike",
        "type": "tags",
        "useValueForTime": False,
    }


def _server_down_annotation() -> dict[str, object]:
    return {
        "datasource": DATASOURCE,
        "enable": False,
        "expr": (
            'delta(up{k8s_cluster="$k8s_cluster", '
            'tidb_cluster="$tidb_cluster", job=~"tikv|ticdc|tidb|pd"}[30s])'
            " < BOOL 0"
        ),
        "hide": False,
        "iconColor": "#FF9830",
        "limit": 100,
        "name": "Server down",
        "showIn": 0,
        "step": "15s",
        "tagKeys": "instance,job",
        "tags": [],
        "textFormat": "",
        "titleFormat": "Down",
        "type": "tags",
    }


def _all_ticdc_alerts_annotation() -> dict[str, object]:
    return {
        "datasource": DATASOURCE,
        "enable": False,
        "expr": (
            'sum(ALERTS{k8s_cluster="$k8s_cluster", '
            'tidb_cluster="$tidb_cluster", alertstate="firing", '
            'alertname=~"ticdc.*"}) by (alertname) > BOOL 0'
        ),
        "hide": False,
        "iconColor": "#B877D9",
        "limit": 100,
        "name": "All TiCDC alerts",
        "showIn": 0,
        "tagKeys": "alertname",
        "tags": [],
        "titleFormat": "Alert Name",
        "type": "tags",
    }


def _resolved_region_drop_annotation() -> dict[str, object]:
    return {
        "datasource": DATASOURCE,
        "enable": False,
        "expr": ('delta(tikv_cdc_region_resolve_status{status="resolved"}[30s]) < BOOL -800'),
        "hide": False,
        "iconColor": "rgba(255, 96, 96, 1)",
        "limit": 100,
        "name": "Resolved region drop",
        "showIn": 0,
        "step": "15s",
        "tagKeys": "instance",
        "tags": [],
        "titleFormat": "Resolved region drop",
        "type": "tags",
    }


def build_annotations() -> list[dict[str, object]]:
    return [
        _dashboard_annotation(),
        _latency_spike_annotation(),
        _server_down_annotation(),
        _all_ticdc_alerts_annotation(),
        _resolved_region_drop_annotation(),
    ]
