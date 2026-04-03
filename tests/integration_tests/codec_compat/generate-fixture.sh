#!/bin/bash

set -eu

SINK_TYPE=${1:-kafka}
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	codec_compat_prepare_workdir
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	unset TICDC_NEWARCH
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	TOPIC_NAME="ticdc-codec-compat-fixture-$RANDOM"
	SINK_URI=$(codec_compat_build_sink_uri "$TOPIC_NAME")
	codec_compat_create_changefeed "$start_ts" "$SINK_URI"
	codec_compat_run_binary generate "$TOPIC_NAME" "$WORK_DIR/codec-compat-generate.log"
	codec_compat_cleanup
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< generate fixtures for $TEST_NAME success! >>>>>>"
