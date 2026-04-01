# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.api import custom_var, query_var
from metrics.dsl.specs import VariableSpecLike


def build_templating() -> list[VariableSpecLike]:
    return [
        query_var("k8s_cluster", query="label_values(go_goroutines, k8s_cluster)"),
        query_var(
            "tidb_cluster",
            query='label_values(go_goroutines{k8s_cluster="$k8s_cluster"}, tidb_cluster)',
        ),
        query_var(
            "namespace",
            query=(
                'label_values('
                'ticdc_owner_checkpoint_ts_lag'
                '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, '
                "namespace)"
            ),
            multi=True,
            include_all=True,
            all_value=".*",
        ),
        query_var(
            "changefeed",
            query=(
                'label_values('
                'ticdc_owner_checkpoint_ts_lag'
                '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, '
                "changefeed)"
            ),
            multi=True,
            include_all=True,
            all_value=".*",
        ),
        query_var(
            "ticdc_instance",
            query=(
                'label_values('
                'process_start_time_seconds'
                '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*ticdc.*"}, '
                "instance)"
            ),
            multi=True,
            include_all=True,
            all_value=".*",
        ),
        query_var(
            "tikv_instance",
            query=(
                'label_values('
                'tikv_engine_size_bytes'
                '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, '
                "instance)"
            ),
            include_all=True,
            all_value=".*",
        ),
        custom_var(
            "spike_threshold",
            options=["1", "3", "5", "10", "60", "300"],
            include_all=True,
            all_value="9999999999",
        ),
        query_var(
            "runtime_instance",
            query=(
                'label_values('
                'process_start_time_seconds'
                '{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*ticdc.*"}, '
                "instance)"
            ),
        ),
    ]
