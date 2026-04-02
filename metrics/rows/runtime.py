# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec


def build_runtime_row() -> RowSpec:
    row_builder = row("Runtime $runtime_instance")

    memory_usage = (
        graph(
            "Memory Usage",
            description="TiCDC process rss memory usage. TiCDC heap memory size in use ",
            unit="bytes",
            min="0",
        )
        .add_query(
            'process_resident_memory_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"}',
            legend="alloc-from-os",
        )
        .add_query(
            'go_memstats_next_gc_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} / (1 + ticdc_server_go_gc{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} / 100)',
            legend="estimate-inuse",
            ref="H",
        )
        .add_query(
            'go_memstats_heap_alloc_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} - go_memstats_next_gc_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} / (1 + ticdc_server_go_gc{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} / 100)',
            legend="estimate-garbage",
            ref="C",
        )
        .add_query(
            'go_memstats_heap_idle_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} - go_memstats_heap_released_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} + go_memstats_heap_inuse_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} - go_memstats_heap_alloc_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"}',
            legend="reserved-by-go",
        )
        .add_query(
            'go_memstats_stack_sys_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} + go_memstats_mspan_sys_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} + go_memstats_mcache_sys_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} + go_memstats_buck_hash_sys_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} + go_memstats_gc_sys_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} + go_memstats_other_sys_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"}',
            legend="used-by-go",
        )
        .add_query(
            'go_memstats_next_gc_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"}',
            legend="gc-threshold",
        )
        .add_query(
            '(clamp_max(idelta(go_memstats_last_gc_time_seconds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"}[1m]), 1) * go_memstats_next_gc_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"}) > 0',
            legend="gc",
        )
    )

    estimated_live_objects = graph(
        "Estimated Live Objects",
        description="Count of live objects.",
        min="0",
    ).add_query(
        'go_memstats_heap_objects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"}',
        legend="objects",
    )

    gc_stw_duration_last_256_gc_cycles = (
        graph(
            "GC STW Duration (last 256 GC cycles)",
            description="TiCDC process Go garbage collection STW pause duration",
            unit="s",
            min="0",
        )
        .add_range_query(
            'go_gc_duration_seconds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance", quantile="0"}',
            legend="min",
        )
        .add_range_query(
            'go_gc_duration_seconds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance", quantile!~"0|1"}',
            legend="{{quantile}}",
        )
        .add_range_query(
            'go_gc_duration_seconds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance", quantile="1"}',
            legend="max",
        )
    )

    allocator_throughput = (
        graph(
            "Allocator Throughput",
            description="The throughput of Go's memory allocator.",
            unit="Bps",
        )
        .add_query(
            'irate(go_memstats_alloc_bytes_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"}[30s])',
            legend="alloc",
        )
        .add_query(
            'irate((go_memstats_alloc_bytes_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"} - go_memstats_heap_alloc_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"})[30s:])',
            legend="sweep",
        )
        .add_query(
            'irate(go_memstats_mallocs_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"}[30s])',
            legend="alloc-ops",
        )
        .add_query(
            'irate(go_memstats_frees_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$runtime_instance"}[30s])',
            legend="swepp-ops",
        )
    )

    row_builder.add_panels(
        memory_usage,
        estimated_live_objects,
    )

    row_builder.add_panels(
        gc_stw_duration_last_256_gc_cycles,
        allocator_throughput,
    )

    return row_builder.build()
