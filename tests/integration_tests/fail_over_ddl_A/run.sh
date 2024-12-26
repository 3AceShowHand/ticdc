#!/bin/bash
# This series of tests are used to test the fail-over with ddl events of TiCDC.
# we start two TiCDC servers, and use failpoint to block the block event ddl execution of different situations
# and do restart to test the fail-over.

# This is the case-A of fail-over with ddl events.
# when dispatchers are all meet the block event ddl, and report the status to maintainer, and maintainer ask table trigger to write ddl, table trigger write the ddl, but not response to maintainer, then the two node both restart. --> we expect the cluster will get the correct table count and continue to sync the following events successfully.
#     1 ddl is drop databases
#     1 ddl is drop table 
#     1 ddl is rename table //
#     1 ddl is recover table // not support yet
#     1 ddl is truncate table

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
check_time=60

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"
	
	TOPIC_NAME="ticdc-failover-ddl-test-two-node-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac	

    run_sql "drop database if exists fail_over_ddl_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "drop database if exists fail_over_ddl_test2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create database fail_over_ddl_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create table fail_over_ddl_test.test1 (id int primary key, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create table fail_over_ddl_test.test2 (id int primary key, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create table fail_over_ddl_test.test3 (id int primary key, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_table_exists fail_over_ddl_test.test3 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
}

# ddl is drop database
function failOverCaseA-1() {
	prepare
	ret=$?
	if [ "$ret" != 0 ]; then
		exit 1
	fi

	# restart cdc server to enable failpoint
	cdc_pid_1=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	kill_cdc_pid $cdc_pid_1
    export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockReportAfterWrite=pause'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-1" --addr "127.0.0.1:8300"
	cdc_pid_1=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-1" --addr "127.0.0.1:8301"

    run_sql "drop database fail_over_ddl_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    ## make ddl must reach the place and report to maintainer, and get the write status, and block in the place that report to maintainer	
	ensure 30 "run_sql 'show databases;' ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_not_contains 'fail_over_ddl_test'" 

	kill_cdc_pid $cdc_pid_1
	cdc_pid_2=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
    kill_cdc_pid $cdc_pid_2

    export GO_FAILPOINTS=''

    # restart cdc server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-2" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-2" --addr "127.0.0.1:8301"

    run_sql "show databases;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_not_contains "fail_over_test"

    ## continue to write ddl and dml to test the cdc server is working well
    run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY

	echo "failOverCaseA-1 passed successfully"
}

# ddl is drop table
function failOverCaseA-2() {
	prepare
	ret=$?
	if [ "$ret" != 0 ]; then
		exit 1
	fi

	# restart cdc server to enable failpoint
	cdc_pid_1=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	kill_cdc_pid $cdc_pid_1
    export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockReportAfterWrite=pause'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-1" --addr "127.0.0.1:8300"
	cdc_pid_1=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-1" --addr "127.0.0.1:8301"

    run_sql "drop table fail_over_ddl_test.test1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    ## make ddl must reach the place and report to maintainer, and get the write status, and block in the place that report to maintainer
	ensure 30 "run_sql 'use fail_over_ddl_test;show tables;' ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_not_contains 'test1'" 
	
	kill_cdc_pid $cdc_pid_1
	cdc_pid_2=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
    kill_cdc_pid $cdc_pid_2

    export GO_FAILPOINTS=''

    # restart cdc server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-2" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-2" --addr "127.0.0.1:8301"

    run_sql "use fail_over_ddl_test;show tables;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_not_contains "test1" &&
		check_contains "test2" && 
		check_contains "test3"
	
	ret=$?
	if [ "$ret" != 0 ]; then
		exit 1
	fi

    ## continue to write ddl and dml to test the cdc server is working well
    run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into fail_over_ddl_test.test2 values (1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into fail_over_ddl_test.test3 values (1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY

	echo "failOverCaseA-2 passed successfully"
}

# ddl is rename table
function failOverCaseA-3() {
	prepare
	ret=$?
	if [ "$ret" != 0 ]; then
		exit 1
	fi

	# restart cdc server to enable failpoint
	cdc_pid_1=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	kill_cdc_pid $cdc_pid_1
    export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockReportAfterWrite=pause'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-1" --addr "127.0.0.1:8300"
	cdc_pid_1=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-1" --addr "127.0.0.1:8301"

    run_sql "rename table fail_over_ddl_test.test1 to fail_over_ddl_test.test4;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    ## make ddl must reach the place and report to maintainer, and get the write status, and block in the place that report to maintainer
	ensure 30 "run_sql 'use fail_over_ddl_test;show tables;' ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_not_contains 'test1' && check_contains 'test4'" 
	
	kill_cdc_pid $cdc_pid_1
	cdc_pid_2=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
    kill_cdc_pid $cdc_pid_2

    export GO_FAILPOINTS=''

    # restart cdc server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-2" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-2" --addr "127.0.0.1:8301"

    run_sql "use fail_over_ddl_test;show tables;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_not_contains "test1" &&
		check_contains "test2" && 
		check_contains "test3" && 
		check_contains "test4"
	
	ret=$?
	if [ "$ret" != 0 ]; then
		exit 1
	fi

    ## continue to write ddl and dml to test the cdc server is working well
    run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into fail_over_ddl_test.test2 values (1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into fail_over_ddl_test.test3 values (1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into fail_over_ddl_test.test4 values (1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY

	echo "failOverCaseA-3 passed successfully"
}

# ddl is truncate table
function failOverCaseA-5() {
	prepare
	ret=$?
	if [ "$ret" != 0 ]; then
		exit 1
	fi

	# restart cdc server to enable failpoint
	cdc_pid_1=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	kill_cdc_pid $cdc_pid_1
    export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockReportAfterWrite=pause'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-1" --addr "127.0.0.1:8300"
	cdc_pid_1=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-1" --addr "127.0.0.1:8301"

	run_sql "insert into fail_over_ddl_test.test1 values (2, 2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 10 "run_sql 'select id from fail_over_ddl_test.test1;' ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains '2'" 

    run_sql "truncate table fail_over_ddl_test.test1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	
    ## make ddl must reach the place and report to maintainer, and get the write status, and block in the place that report to maintainer
	ensure 30 "run_sql 'select id from fail_over_ddl_test.test1;' ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_not_contains '2'" 
	
	kill_cdc_pid $cdc_pid_1
	cdc_pid_2=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
    kill_cdc_pid $cdc_pid_2

    export GO_FAILPOINTS=''

    # restart cdc server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-2" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-2" --addr "127.0.0.1:8301"

    run_sql "use fail_over_ddl_test;show tables;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_contains "test1" &&
		check_contains "test2" && 
		check_contains "test3"
	
	ret=$?
	if [ "$ret" != 0 ]; then
		exit 1
	fi

    ## continue to write ddl and dml to test the cdc server is working well
    run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into fail_over_ddl_test.test1 values (1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into fail_over_ddl_test.test2 values (1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into fail_over_ddl_test.test3 values (1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY

	echo "failOverCaseA-5 passed successfully"
}

trap stop_tidb_cluster EXIT
failOverCaseA-1
failOverCaseA-2
failOverCaseA-3
failOverCaseA-5
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"