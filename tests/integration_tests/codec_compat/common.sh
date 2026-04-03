#!/bin/bash

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare

WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
CHANGEFEED_ID="codec-compat"
PROTOCOL=${CODEC_COMPAT_PROTOCOL:-canal-json}
ENCODING_FORMAT=${CODEC_COMPAT_ENCODING_FORMAT:-}
SCHEMA_REGISTRY_URI=${CODEC_COMPAT_SCHEMA_REGISTRY_URI:-http://127.0.0.1:8088}

function codec_compat_prepare_workdir() {
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"
	codec_compat_maybe_start_schema_registry
}

function codec_compat_maybe_start_schema_registry() {
	if [ "$PROTOCOL" != "avro" ]; then
		return
	fi

	echo 'Starting schema registry...'
	./bin/bin/schema-registry-start -daemon ./bin/etc/schema-registry/schema-registry.properties
	for i in $(seq 1 10); do
		if curl -sf "${SCHEMA_REGISTRY_URI}/subjects" >/dev/null 2>&1; then
			break
		fi
		sleep 1
		if [ "$i" = "10" ]; then
			echo 'Failed to start schema registry'
			exit 1
		fi
	done

	curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
		--data '{"compatibility": "NONE"}' "${SCHEMA_REGISTRY_URI}/config"
}

function codec_compat_build_sink_uri() {
	local topic_name=$1

	case "$PROTOCOL" in
		canal-json)
			echo "kafka://127.0.0.1:9092/${topic_name}?protocol=canal-json&enable-tidb-extension=true"
			;;
		open-protocol)
			echo "kafka://127.0.0.1:9092/${topic_name}?protocol=open-protocol&enable-tidb-extension=true"
			;;
		debezium)
			echo "kafka://127.0.0.1:9092/${topic_name}?protocol=debezium&enable-tidb-extension=true"
			;;
		simple)
			if [ -n "$ENCODING_FORMAT" ]; then
				echo "kafka://127.0.0.1:9092/${topic_name}?protocol=simple&encoding-format=${ENCODING_FORMAT}&enable-tidb-extension=true"
			else
				echo "kafka://127.0.0.1:9092/${topic_name}?protocol=simple&enable-tidb-extension=true"
			fi
			;;
		avro)
			echo "kafka://127.0.0.1:9092/${topic_name}?protocol=avro&enable-tidb-extension=true&avro-enable-watermark=true"
			;;
		*)
			echo "unsupported protocol ${PROTOCOL}" >&2
			exit 1
			;;
	esac
}

function codec_compat_create_changefeed() {
	local start_ts=$1
	local sink_uri=$2

	if [ "$PROTOCOL" = "avro" ]; then
		cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$sink_uri" -c "$CHANGEFEED_ID" --schema-registry="$SCHEMA_REGISTRY_URI"
		return
	fi

	cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$sink_uri" -c "$CHANGEFEED_ID"
}

function codec_compat_run_binary() {
	local mode=$1
	local topic_name=$2
	local log_file=$3

	cd "$CUR"
	set -o pipefail
	GO111MODULE=on go run . \
		--mode "$mode" \
		--protocol "$PROTOCOL" \
		--encoding-format "$ENCODING_FORMAT" \
		--schema-registry-uri "$SCHEMA_REGISTRY_URI" \
		--sql-root "$CUR/sql" \
		--fixture-root "$CUR/fixtures" \
		--upstream-dsn "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?charset=utf8mb4&multiStatements=true&parseTime=true" \
		--kafka-addrs "127.0.0.1:9092" \
		--topic "$topic_name" \
		--cdc-api "http://${CDC_HOST}:${CDC_PORT}" \
		--changefeed-id "$CHANGEFEED_ID" \
		--keyspace "$KEYSPACE_NAME" 2>&1 | tee "$log_file"
}

function codec_compat_cleanup() {
	cleanup_process "$CDC_BINARY"
	stop_tidb_cluster
	export TICDC_NEWARCH=true
}
