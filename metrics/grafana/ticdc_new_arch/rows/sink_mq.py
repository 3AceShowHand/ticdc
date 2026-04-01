# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from metrics.dsl.specs import RowSpec
from metrics.grafana.common import (
    eq,
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_max,
    expr_simple,
    expr_sum,
    expr_sum_rate,
    histogram_quantile_graph_panel as histogram_quantile_graph,
    regex,
)
from metrics.grafana.ticdc_new_arch.builders import graph, row

def build_sink_mq_row() -> RowSpec:
    row_builder = row('Sink - MQ Sink', default_height=7, default_span=12)

    worker_send_message_duration_percentile = histogram_quantile_graph(
        'Worker Send Message Duration Percentile',
        metric='ticdc_sink_mq_worker_send_message_duration',
        matchers=[
            eq('k8s_cluster', '$k8s_cluster'),
            eq('tidb_cluster', '$tidb_cluster'),
            regex('namespace', '$namespace'),
            regex('changefeed', '$changefeed'),
            regex('instance', '$ticdc_instance'),
        ],
        by=['namespace', 'changefeed', 'instance'],
        description='MQ worker send messages to Kafka, this metric record the time cost on send every message.',
        unit='s',
        min='0',
        width=12,
        height=7,
        quantile=0.999,
        quantile_legend='{{namespace}}-{{changefeed}}-{{instance}}-P999',
        average_legend='{{namespace}}-{{changefeed}}-{{instance}}-avg',
    )

    row_builder.add_graph(worker_send_message_duration_percentile)

    kafka_outgoing_bytes = graph(
        'Kafka Outgoing Bytes',
        description='Bytes / second written off all brokers.\nvalue = one-minute moving average rate of Bytes per second',
        unit='bytes',
        min='0',
    )

    kafka_outgoing_bytes.add_query(
        expr_sum(
            'ticdc_sink_kafka_producer_outgoing_byte_rate',
            by_labels=['namespace', 'changefeed', 'instance', 'broker'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{broker}}',
    )

    row_builder.add_graph(kafka_outgoing_bytes)

    kafka_inflight_requests = graph(
        'Kafka Inflight Requests',
        description='The current number of in-flight requests awaiting a response for all brokers.',
        unit='none',
        min='0',
    )

    kafka_inflight_requests.add_query(
        expr_sum(
            'ticdc_sink_kafka_producer_in_flight_requests',
            by_labels=['namespace', 'changefeed', 'instance', 'broker'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{broker}}',
    )

    row_builder.add_graph(kafka_inflight_requests)

    kafka_request_latency = graph(
        'Kafka Request Latency',
        description="The request latency in ms for all brokers.\n\nvalue = request latency histogram's mean",
        unit='s',
        min='0',
    )

    kafka_request_latency.add_query(
        expr_sum(
            'ticdc_sink_kafka_producer_request_latency',
            by_labels=['namespace', 'changefeed', 'instance', 'broker', 'type'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{broker}}-{{type}}',
    )

    row_builder.add_graph(kafka_request_latency)

    kafka_request_rate = graph(
        'Kafka Request Rate',
        description='Requests / second sent to all brokers.\nvalue = one-minute moving average rate of events per second',
        unit='none',
        min='0',
    )

    kafka_request_rate.add_query(
        expr_sum(
            'ticdc_sink_kafka_producer_request_rate',
            by_labels=['namespace', 'changefeed', 'instance', 'broker'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{broker}}',
    )

    row_builder.add_graph(kafka_request_rate)

    kafka_records_per_request = graph(
        'Kafka Records Per Request',
        description='Records count per request send to the kafka\nvalue = one-minute moving average of response receive rate',
        unit='none',
        min='0',
    )

    kafka_records_per_request.add_query(
        expr_sum(
            'ticdc_sink_kafka_producer_records_per_request',
            by_labels=['namespace', 'changefeed', 'instance', 'type'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{type}}',
    )

    row_builder.add_graph(kafka_records_per_request)

    kafka_producer_compression_ratio = graph(
        'Kafka Producer Compression Ratio',
        description='The compression ratio times 100 of record batches for all topics. Compression ratio = Size of original data / Size of compressed data * 100',
        unit='percent',
        min='0',
    )

    kafka_producer_compression_ratio.add_query(
        expr_sum(
            'ticdc_sink_kafka_producer_compression_ratio',
            by_labels=['namespace', 'changefeed', 'instance', 'type'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{type}}',
    )

    row_builder.add_graph(kafka_producer_compression_ratio)

    encoder_group_input_channel_size = graph(
        'Encoder Group Input Channel Size',
        description='',
        unit='none',
        min='0',
    )

    encoder_group_input_channel_size.add_query(
        expr_simple(
            'ticdc_sink_encoder_group_input_chan_size',
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-{{index}}',
        format=None,
    )

    row_builder.add_graph(encoder_group_input_channel_size)

    encoder_group_output_channel_size = graph(
        'Encoder Group Output Channel Size',
        description='',
        unit='none',
        min='0',
    )

    encoder_group_output_channel_size.add_query(
        expr_simple(
            'ticdc_sink_encoder_group_output_chan_size',
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}',
        format=None,
    )

    row_builder.add_graph(encoder_group_output_channel_size)

    worker_batch_duration_percentile = graph(
        'Worker Batch Duration Percentile',
        description='MQ worker batch multiple messages into one when using batched encode protocol, this metric record the time cost on batch messages.',
        unit='s',
        min='0',
    )

    worker_batch_duration_percentile.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_sink_mq_worker_batch_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-P999',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_sink_mq_worker_batch_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-avg',
        format=None,
    )

    row_builder.add_graph(worker_batch_duration_percentile)

    worker_batch_size_percentile = graph(
        'Worker Batch Size Percentile',
        description="MQ worker batch multiple messages into one when using batched encode protocol, this metrics track each batch's size",
        unit='none',
        min='0',
    )

    worker_batch_size_percentile.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_sink_mq_worker_batch_size',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-P999',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_sink_mq_worker_batch_size',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-avg',
        format=None,
    )

    row_builder.add_graph(worker_batch_size_percentile)

    claim_check_send_message_count = graph(
        'Claim Check Send Message Count',
        description='MQ worker send large message to the external storage, this metrics record the message count',
        unit='short',
        min='0',
    )

    claim_check_send_message_count.add_query(
        expr_sum_rate(
            'ticdc_sink_mq_claim_check_send_message_count',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}',
        ref='B',
        format=None,
    )

    row_builder.add_graph(claim_check_send_message_count)

    claim_check_send_message_duration_percentile = graph(
        'Claim Check Send Message Duration Percentile',
        description='MQ worker send large message to the external storage, this metric record the time cost on send every message.',
        unit='s',
        min='0',
    )

    claim_check_send_message_duration_percentile.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_sink_mq_claim_check_send_message_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-P999',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_sink_mq_claim_check_send_message_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
            window='30s',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-avg',
        format=None,
    )

    row_builder.add_graph(claim_check_send_message_duration_percentile)

    worker_send_checkpointts_message_count = graph(
        'Worker Send CheckpointTs Message Count',
        description='the number of message count of checkpointTs message',
        unit='short',
        min='0',
    )

    worker_send_checkpointts_message_count.add_query(
        expr_max(
            'ticdc_sink_mq_checkpoint_ts_message_count',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-P999',
        format=None,
    )

    row_builder.add_graph(worker_send_checkpointts_message_count)

    worker_encode_and_send_checkpoint_message_duration = graph(
        'Worker Encode and Send Checkpoint Message Duration',
        description='this metric record the time cost of the MQ worker encode and send  checkpointTs messages to downstream ',
        unit='s',
        min='0',
    )

    worker_encode_and_send_checkpoint_message_duration.add_query(
        expr_histogram_quantile(
            0.999,
            'ticdc_sink_mq_checkpoint_ts_message_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-P999',
        format=None,
    ).add_query(
        expr_histogram_avg(
            'ticdc_sink_mq_checkpoint_ts_message_duration',
            by_labels=['namespace', 'changefeed', 'instance'],
            scope='changefeed',
        ),
        legend='{{namespace}}-{{changefeed}}-{{instance}}-avg',
        format=None,
    )

    row_builder.add_graph(
        worker_encode_and_send_checkpoint_message_duration,
    )

    return row_builder.build()
