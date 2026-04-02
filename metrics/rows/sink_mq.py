# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.builders import graph, row
from metrics.dsl.specs import RowSpec


def build_sink_mq_row() -> RowSpec:
    row_builder = row("Sink - MQ Sink")

    worker_send_message_duration_percentile = (
        graph(
            "Worker Send Message Duration Percentile",
            description="MQ worker send messages to Kafka. This metric records the time cost of sending each message.",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(0.999, sum(rate(ticdc_sink_mq_worker_send_message_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed", instance=~"$ticdc_instance"}[1m])) by (le, namespace, changefeed, instance))',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-P999",
        )
        .add_auto_query(
            'sum(rate(ticdc_sink_mq_worker_send_message_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed", instance=~"$ticdc_instance"}[1m])) by (namespace, changefeed, instance) / sum(rate(ticdc_sink_mq_worker_send_message_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", namespace=~"$namespace", changefeed=~"$changefeed", instance=~"$ticdc_instance"}[1m])) by (namespace, changefeed, instance)',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-avg",
        )
    )

    kafka_outgoing_bytes = graph(
        "Kafka Outgoing Bytes",
        description="Bytes / second written off all brokers.\nvalue = one-minute moving average rate of bytes per second",
        unit="bytes",
        min="0",
    ).add_query(
        'sum(ticdc_sink_kafka_producer_outgoing_byte_rate{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, instance, broker)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}-{{broker}}",
    )

    kafka_inflight_requests = graph(
        "Kafka Inflight Requests",
        description="The current number of in-flight requests awaiting a response for all brokers.",
        unit="none",
        min="0",
    ).add_query(
        'sum(ticdc_sink_kafka_producer_in_flight_requests{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, instance, broker)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}-{{broker}}",
    )

    kafka_request_latency = graph(
        "Kafka Request Latency",
        description="The request latency in ms for all brokers.\n\nvalue = request latency histogram's mean",
        unit="s",
        min="0",
    ).add_query(
        'sum(ticdc_sink_kafka_producer_request_latency{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, instance, broker, type)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}-{{broker}}-{{type}}",
    )

    kafka_request_rate = graph(
        "Kafka Request Rate",
        description="Requests / second sent to all brokers.\nvalue = one-minute moving average rate of events per second",
        unit="none",
        min="0",
    ).add_query(
        'sum(ticdc_sink_kafka_producer_request_rate{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, instance, broker)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}-{{broker}}",
    )

    kafka_records_per_request = graph(
        "Kafka Records Per Request",
        description="Records count per request sent to Kafka\nvalue = one-minute moving average of response receive rate",
        unit="none",
        min="0",
    ).add_query(
        'sum(ticdc_sink_kafka_producer_records_per_request{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, instance, type)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}-{{type}}",
    )

    kafka_producer_compression_ratio = graph(
        "Kafka Producer Compression Ratio",
        description="The compression ratio times 100 of record batches for all topics. Compression ratio = size of original data / size of compressed data * 100",
        unit="percent",
        min="0",
    ).add_query(
        'sum(ticdc_sink_kafka_producer_compression_ratio{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, instance, type)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}-{{type}}",
    )

    encoder_group_input_channel_size = graph(
        "Encoder Group Input Channel Size",
        description="",
        unit="none",
        min="0",
    ).add_auto_query(
        'ticdc_sink_encoder_group_input_chan_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}',
        legend="{{namespace}}-{{changefeed}}-{{instance}}-{{index}}",
    )

    encoder_group_output_channel_size = graph(
        "Encoder Group Output Channel Size",
        description="",
        unit="none",
        min="0",
    ).add_auto_query(
        'ticdc_sink_encoder_group_output_chan_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}',
        legend="{{namespace}}-{{changefeed}}-{{instance}}",
    )

    worker_batch_duration_percentile = (
        graph(
            "Worker Batch Duration Percentile",
            description="MQ worker batches multiple messages into one when using the batched encode protocol. This metric records batch latency.",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(0.999, sum(rate(ticdc_sink_mq_worker_batch_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (le, namespace, changefeed, instance))',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-P999",
        )
        .add_auto_query(
            'sum(rate(ticdc_sink_mq_worker_batch_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance) / sum(rate(ticdc_sink_mq_worker_batch_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance)',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-avg",
        )
    )

    worker_batch_size_percentile = (
        graph(
            "Worker Batch Size Percentile",
            description="MQ worker batches multiple messages into one when using the batched encode protocol. This metric tracks each batch size.",
            unit="none",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(0.999, sum(rate(ticdc_sink_mq_worker_batch_size_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (le, namespace, changefeed, instance))',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-P999",
        )
        .add_auto_query(
            'sum(rate(ticdc_sink_mq_worker_batch_size_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance) / sum(rate(ticdc_sink_mq_worker_batch_size_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance)',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-avg",
        )
    )

    claim_check_send_message_count = graph(
        "Claim Check Send Message Count",
        description="MQ worker sends large messages to external storage. This metric records the message count.",
        min="0",
    ).add_auto_query(
        'sum(rate(ticdc_sink_mq_claim_check_send_message_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}",
        ref="B",
    )

    claim_check_send_message_duration_percentile = (
        graph(
            "Claim Check Send Message Duration Percentile",
            description="MQ worker sends large messages to external storage. This metric records the time cost of sending each message.",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(0.999, sum(rate(ticdc_sink_mq_claim_check_send_message_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (le, namespace, changefeed, instance))',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-P999",
        )
        .add_auto_query(
            'sum(rate(ticdc_sink_mq_claim_check_send_message_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[30s])) by (namespace, changefeed, instance) / sum(rate(ticdc_sink_mq_claim_check_send_message_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[30s])) by (namespace, changefeed, instance)',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-avg",
        )
    )

    worker_send_checkpointts_message_count = graph(
        "Worker Send CheckpointTs Message Count",
        description="the number of message count of checkpointTs message",
        min="0",
    ).add_auto_query(
        'max(ticdc_sink_mq_checkpoint_ts_message_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}) by (namespace, changefeed, instance)',
        legend="{{namespace}}-{{changefeed}}-{{instance}}-P999",
    )

    worker_encode_and_send_checkpoint_message_duration = (
        graph(
            "Worker Encode and Send Checkpoint Message Duration",
            description="This metric records the time cost of encoding and sending checkpointTs messages downstream.",
            unit="s",
            min="0",
        )
        .add_auto_query(
            'histogram_quantile(0.999, sum(rate(ticdc_sink_mq_checkpoint_ts_message_duration_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (le, namespace, changefeed, instance))',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-P999",
        )
        .add_auto_query(
            'sum(rate(ticdc_sink_mq_checkpoint_ts_message_duration_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance) / sum(rate(ticdc_sink_mq_checkpoint_ts_message_duration_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$ticdc_instance", namespace=~"$namespace", changefeed=~"$changefeed"}[1m])) by (namespace, changefeed, instance)',
            legend="{{namespace}}-{{changefeed}}-{{instance}}-avg",
        )
    )

    row_builder.add_panels(
        worker_send_message_duration_percentile,
        kafka_outgoing_bytes,
    )

    row_builder.add_panels(
        kafka_inflight_requests,
        kafka_request_latency,
    )

    row_builder.add_panels(
        kafka_request_rate,
        kafka_records_per_request,
    )

    row_builder.add_panels(
        kafka_producer_compression_ratio,
        encoder_group_input_channel_size,
    )

    row_builder.add_panels(
        encoder_group_output_channel_size,
        worker_batch_duration_percentile,
    )

    row_builder.add_panels(
        worker_batch_size_percentile,
        claim_check_send_message_count,
    )

    row_builder.add_panels(
        claim_check_send_message_duration_percentile,
        worker_send_checkpointts_message_count,
    )

    row_builder.add_half_panel(worker_encode_and_send_checkpoint_message_duration)

    return row_builder.build()
