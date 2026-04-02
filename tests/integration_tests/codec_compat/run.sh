#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
CHANGEFEED_ID="codec-compat"

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	export TICDC_NEWARCH=true
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	TOPIC_NAME="ticdc-codec-compat-$RANDOM"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"

	cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$SINK_URI" -c "$CHANGEFEED_ID"

	cd "$CUR"
	set -o pipefail
	GO111MODULE=on go run . \
		--mode=verify \
		--protocol=canal-json \
		--sql-root "$CUR/sql" \
		--fixture-root "$CUR/fixtures" \
		--upstream-dsn "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?charset=utf8mb4&multiStatements=true&parseTime=true" \
		--kafka-addrs "127.0.0.1:9092" \
		--topic "$TOPIC_NAME" \
		--cdc-api "http://${CDC_HOST}:${CDC_PORT}" \
		--changefeed-id "$CHANGEFEED_ID" \
		--keyspace "$KEYSPACE_NAME" 2>&1 | tee "$WORK_DIR/codec-compat.log"

	cleanup_process "$CDC_BINARY"
	stop_tidb_cluster
	export TICDC_NEWARCH=true
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
