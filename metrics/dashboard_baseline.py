# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from typing import Any, Final

import metrics.dashboard_identity as dashboard_identity

EXPECTED_TEMPLATE_NAMES: Final[list[str]] = [
    "k8s_cluster",
    "tidb_cluster",
    "namespace",
    "changefeed",
    "ticdc_instance",
    "tikv_instance",
    "spike_threshold",
    "runtime_instance",
]
EXPECTED_ANNOTATION_NAMES: Final[list[str]] = [
    "",
    "Latency spike",
    "Server down",
    "All TiCDC alerts",
    "Resolved region drop",
]
EXPECTED_ROW_TITLES: Final[list[str]] = [
    "Summary",
    "Lag Summary",
    "Dataflow",
    "Server",
    "Changefeed",
    "Lag analyze",
    "Coordinator",
    "Maintainer",
    "Log Puller",
    "Event Store",
    "Schema Store",
    "Event Service",
    "Message Center",
    "Dispatcher",
    "Dynamic Stream",
    "Sink - General",
    "Sink - Transaction Sink",
    "Sink - MQ Sink",
    "Sink - Cloud Storage Sink",
    "Scheduler",
    "TiKV",
    "Active Active",
    "Redo",
    "Runtime $runtime_instance",
    "DDL",
    "Pulsar Sink",
]
EXPECTED_TEMPLATING: Final[list[dict[str, object]]] = [
    {
        "allValue": None,
        "current": {
            "isNone": True,
            "selected": False,
            "text": "None",
            "value": "",
        },
        "datasource": "${DS_TEST-CLUSTER}",
        "definition": "",
        "description": None,
        "error": None,
        "hide": 0,
        "includeAll": False,
        "label": "K8s-cluster",
        "multi": False,
        "name": "k8s_cluster",
        "options": [],
        "query": {
            "query": "label_values(go_goroutines, k8s_cluster)",
            "refId": "local-k8s_cluster-Variable-Query",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 1,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False,
    },
    {
        "allValue": None,
        "current": {
            "isNone": True,
            "selected": False,
            "text": "None",
            "value": "",
        },
        "datasource": "${DS_TEST-CLUSTER}",
        "definition": "",
        "description": None,
        "error": None,
        "hide": 0,
        "includeAll": False,
        "label": "tidb_cluster",
        "multi": False,
        "name": "tidb_cluster",
        "options": [],
        "query": {
            "query": ('label_values(go_goroutines{k8s_cluster="$k8s_cluster"}, tidb_cluster)'),
            "refId": "local-tidb_cluster-Variable-Query",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 1,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False,
    },
    {
        "allValue": ".*",
        "current": {"selected": False, "text": "All", "value": "$__all"},
        "datasource": "${DS_TEST-CLUSTER}",
        "definition": (
            'label_values(ticdc_owner_checkpoint_ts_lag{k8s_cluster="$k8s_cluster", '
            'tidb_cluster="$tidb_cluster"}, namespace)'
        ),
        "description": None,
        "error": None,
        "hide": 0,
        "includeAll": True,
        "label": "Namespace",
        "multi": True,
        "name": "namespace",
        "options": [],
        "query": {
            "query": (
                "label_values(ticdc_owner_checkpoint_ts_lag{"
                'k8s_cluster="$k8s_cluster", '
                'tidb_cluster="$tidb_cluster"}, namespace)'
            ),
            "refId": "StandardVariableQuery",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False,
    },
    {
        "allValue": ".*",
        "current": {"selected": False, "text": "All", "value": "$__all"},
        "datasource": "${DS_TEST-CLUSTER}",
        "definition": (
            'label_values(ticdc_owner_checkpoint_ts_lag{k8s_cluster="$k8s_cluster", '
            'tidb_cluster="$tidb_cluster"}, changefeed)'
        ),
        "description": None,
        "error": None,
        "hide": 0,
        "includeAll": True,
        "label": "Changefeed",
        "multi": True,
        "name": "changefeed",
        "options": [],
        "query": {
            "query": (
                "label_values(ticdc_owner_checkpoint_ts_lag{"
                'k8s_cluster="$k8s_cluster", '
                'tidb_cluster="$tidb_cluster"}, changefeed)'
            ),
            "refId": "StandardVariableQuery",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False,
    },
    {
        "allValue": ".*",
        "current": {"selected": False, "text": "All", "value": "$__all"},
        "datasource": "${DS_TEST-CLUSTER}",
        "definition": (
            'label_values(process_start_time_seconds{k8s_cluster="$k8s_cluster", '
            'tidb_cluster="$tidb_cluster", job=~".*ticdc.*"}, instance)'
        ),
        "description": None,
        "error": None,
        "hide": 0,
        "includeAll": True,
        "label": "TiCDC",
        "multi": True,
        "name": "ticdc_instance",
        "options": [],
        "query": {
            "query": (
                'label_values(process_start_time_seconds{k8s_cluster="$k8s_cluster", '
                'tidb_cluster="$tidb_cluster", job=~".*ticdc.*"}, instance)'
            ),
            "refId": "StandardVariableQuery",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False,
    },
    {
        "allValue": ".*",
        "current": {"selected": False, "text": "All", "value": "$__all"},
        "datasource": "${DS_TEST-CLUSTER}",
        "definition": (
            'label_values(tikv_engine_size_bytes{k8s_cluster="$k8s_cluster", '
            'tidb_cluster="$tidb_cluster"}, instance)'
        ),
        "description": None,
        "error": None,
        "hide": 0,
        "includeAll": True,
        "label": "TiKV",
        "multi": False,
        "name": "tikv_instance",
        "options": [],
        "query": {
            "query": (
                'label_values(tikv_engine_size_bytes{k8s_cluster="$k8s_cluster", '
                'tidb_cluster="$tidb_cluster"}, instance)'
            ),
            "refId": "local-tikv_instance-Variable-Query",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 1,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False,
    },
    {
        "allValue": "9999999999",
        "current": {"selected": True, "text": "All", "value": "$__all"},
        "description": None,
        "error": None,
        "hide": 0,
        "includeAll": True,
        "label": "Latency spike (s) >",
        "multi": False,
        "name": "spike_threshold",
        "options": [
            {"selected": True, "text": "All", "value": "$__all"},
            {"selected": False, "text": "1", "value": "1"},
            {"selected": False, "text": "3", "value": "3"},
            {"selected": False, "text": "5", "value": "5"},
            {"selected": False, "text": "10", "value": "10"},
            {"selected": False, "text": "60", "value": "60"},
            {"selected": False, "text": "300", "value": "300"},
        ],
        "query": "1, 3, 5, 10, 60, 300",
        "queryValue": "",
        "skipUrlSync": False,
        "type": "custom",
    },
    {
        "allValue": "",
        "current": {"selected": False, "text": "All", "value": "$__all"},
        "datasource": "${DS_TEST-CLUSTER}",
        "definition": (
            'label_values(process_start_time_seconds{k8s_cluster="$k8s_cluster", '
            'tidb_cluster="$tidb_cluster", job=~".*ticdc.*"}, instance)'
        ),
        "description": None,
        "error": None,
        "hide": 0,
        "includeAll": True,
        "label": "Runtime metrics",
        "multi": False,
        "name": "runtime_instance",
        "options": [],
        "query": {
            "query": (
                'label_values(process_start_time_seconds{k8s_cluster="$k8s_cluster", '
                'tidb_cluster="$tidb_cluster", job=~".*ticdc.*"}, instance)'
            ),
            "refId": "local-runtime_instance-Variable-Query",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False,
    },
]


def _annotation_names(dashboard: dict[str, Any]) -> list[str]:
    return [annotation.get("name", "") for annotation in dashboard["annotations"]["list"]]


def validate_dashboard_identity(
    dashboard: dict[str, Any],
    *,
    expected_row_titles: list[str] | tuple[str, ...] = EXPECTED_ROW_TITLES,
) -> None:
    assert dashboard["title"] == dashboard_identity.BASE_DASHBOARD_TITLE
    assert dashboard["uid"] == dashboard_identity.BASE_DASHBOARD_UID
    assert dashboard["version"] == dashboard_identity.DASHBOARD_VERSION
    assert dashboard["__inputs"] == [dashboard_identity.DATASOURCE_INPUT]
    assert [item["name"] for item in dashboard["templating"]["list"]] == EXPECTED_TEMPLATE_NAMES
    assert [panel["title"] for panel in dashboard["panels"]] == list(expected_row_titles)


def validate_dashboard_compatibility(
    dashboard: dict[str, Any],
    *,
    expected_row_titles: list[str] | tuple[str, ...] = EXPECTED_ROW_TITLES,
) -> None:
    validate_dashboard_identity(
        dashboard,
        expected_row_titles=expected_row_titles,
    )
    assert dashboard["templating"]["list"] == EXPECTED_TEMPLATING
    assert _annotation_names(dashboard) == EXPECTED_ANNOTATION_NAMES
