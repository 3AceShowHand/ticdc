// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	modeGenerate = "generate"
	modeVerify   = "verify"
)

type config struct {
	mode                  string
	protocol              string
	sqlRoot               string
	fixtureRoot           string
	upstreamDSN           string
	kafkaAddrs            string
	topic                 string
	cdcAPI                string
	changefeedID          string
	keyspace              string
	statementTimeout      time.Duration
	checkpointTimeout     time.Duration
	checkpointPoll        time.Duration
	collectIdleTimeout    time.Duration
	kafkaSessionTimeout   time.Duration
}

func main() {
	cfg := parseFlags()
	if err := run(context.Background(), cfg); err != nil {
		log.Fatal("codec compat failed", zap.Error(err))
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.mode, "mode", modeVerify, "run mode: generate or verify")
	flag.StringVar(&cfg.protocol, "protocol", "canal-json", "codec protocol")
	flag.StringVar(&cfg.sqlRoot, "sql-root", "", "root directory of SQL corpus")
	flag.StringVar(&cfg.fixtureRoot, "fixture-root", "", "root directory of fixtures")
	flag.StringVar(&cfg.upstreamDSN, "upstream-dsn", "", "upstream TiDB DSN")
	flag.StringVar(&cfg.kafkaAddrs, "kafka-addrs", "", "comma separated kafka brokers")
	flag.StringVar(&cfg.topic, "topic", "", "kafka topic")
	flag.StringVar(&cfg.cdcAPI, "cdc-api", "http://127.0.0.1:8300", "TiCDC API endpoint")
	flag.StringVar(&cfg.changefeedID, "changefeed-id", "codec-compat", "changefeed identifier")
	flag.StringVar(&cfg.keyspace, "keyspace", "default", "changefeed keyspace")
	flag.DurationVar(&cfg.statementTimeout, "statement-timeout", 30*time.Second, "timeout per SQL statement")
	flag.DurationVar(&cfg.checkpointTimeout, "checkpoint-timeout", 2*time.Minute, "timeout waiting for checkpoint to advance")
	flag.DurationVar(&cfg.checkpointPoll, "checkpoint-poll-interval", time.Second, "checkpoint polling interval")
	flag.DurationVar(&cfg.collectIdleTimeout, "collect-idle-timeout", 2*time.Second, "idle time before draining a statement")
	flag.DurationVar(&cfg.kafkaSessionTimeout, "kafka-session-timeout", 10*time.Second, "kafka client request timeout")
	flag.Parse()
	return cfg
}

func run(ctx context.Context, cfg config) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	plan := DefaultPlan()
	if err := plan.Validate(cfg.sqlRoot); err != nil {
		return err
	}

	runner, err := NewRunner(cfg.upstreamDSN, cfg.statementTimeout)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := runner.Close(); closeErr != nil {
			log.Warn("close runner failed", zap.Error(closeErr))
		}
	}()

	collector, err := NewCollector(splitCSV(cfg.kafkaAddrs), cfg.topic, cfg.kafkaSessionTimeout)
	if err != nil {
		return err
	}
	defer collector.Close()

	changefeedClient := NewChangefeedClient(cfg.cdcAPI, cfg.changefeedID, cfg.keyspace)
	store := NewFixtureStore(cfg.fixtureRoot, cfg.protocol)

	for _, plannedFile := range plan.OrderedFiles() {
		if err := processFile(ctx, cfg, plannedFile, runner, collector, changefeedClient, store); err != nil {
			return err
		}
	}

	return nil
}

func processFile(
	ctx context.Context,
	cfg config,
	plannedFile PlannedFile,
	runner *Runner,
	collector *Collector,
	changefeedClient *ChangefeedClient,
	store *FixtureStore,
) error {
	statements, err := LoadStatements(cfg.sqlRoot, plannedFile)
	if err != nil {
		return err
	}

	log.Info("processing sql file",
		zap.String("mode", cfg.mode),
		zap.String("file", statements[0].SourceFile),
		zap.Int("statements", len(statements)))

	actual := FileFixture{
		Protocol:   cfg.protocol,
		SourceFile: statements[0].SourceFile,
		Statements: make([]StatementFixture, 0, len(statements)),
	}

	for _, stmt := range statements {
		log.Info("executing statement",
			zap.String("file", stmt.SourceFile),
			zap.Int("ordinal", stmt.Ordinal),
			zap.String("sql", stmt.SQL))

		barrierTS, err := runner.Execute(ctx, stmt)
		if err != nil {
			return err
		}

		waitCtx, cancel := context.WithTimeout(ctx, cfg.checkpointTimeout)
		err = changefeedClient.WaitCheckpoint(waitCtx, barrierTS, cfg.checkpointPoll)
		cancel()
		if err != nil {
			return errors.Annotatef(err, "wait checkpoint for %s stmt#%d", stmt.SourceFile, stmt.Ordinal)
		}

		messages, err := collector.Drain(ctx, cfg.collectIdleTimeout)
		if err != nil {
			return errors.Annotatef(err, "drain messages for %s stmt#%d", stmt.SourceFile, stmt.Ordinal)
		}

		normalized, err := NormalizeCapturedMessages(messages)
		if err != nil {
			return errors.Annotatef(err, "normalize messages for %s stmt#%d", stmt.SourceFile, stmt.Ordinal)
		}

		actual.Statements = append(actual.Statements, StatementFixture{
			SQL:              stmt.SQL,
			ExpectedMessages: normalized,
		})

		log.Info("statement collected",
			zap.String("file", stmt.SourceFile),
			zap.Int("ordinal", stmt.Ordinal),
			zap.Int("messages", len(normalized)))
	}

	switch cfg.mode {
	case modeGenerate:
		if err := store.Write(actual); err != nil {
			return err
		}
		log.Info("fixture written",
			zap.String("file", actual.SourceFile),
			zap.String("path", store.FixturePath(actual.SourceFile)))
	case modeVerify:
		expected, err := store.Read(actual.SourceFile)
		if err != nil {
			return err
		}
		if err := CompareFileFixtures(expected, actual); err != nil {
			return err
		}
		log.Info("fixture verified", zap.String("file", actual.SourceFile))
	default:
		return errors.Errorf("unsupported mode %s", cfg.mode)
	}

	return nil
}

func (c config) validate() error {
	if c.mode != modeGenerate && c.mode != modeVerify {
		return errors.Errorf("unsupported mode %s", c.mode)
	}
	if c.protocol != "canal-json" {
		return errors.Errorf("unsupported protocol %s", c.protocol)
	}
	if c.sqlRoot == "" {
		return errors.New("sql-root must be set")
	}
	if c.fixtureRoot == "" {
		return errors.New("fixture-root must be set")
	}
	if c.upstreamDSN == "" {
		return errors.New("upstream-dsn must be set")
	}
	if len(splitCSV(c.kafkaAddrs)) == 0 {
		return errors.New("kafka-addrs must be set")
	}
	if c.topic == "" {
		return errors.New("topic must be set")
	}
	if c.changefeedID == "" {
		return errors.New("changefeed-id must be set")
	}
	return nil
}

func splitCSV(in string) []string {
	items := strings.Split(in, ",")
	result := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
