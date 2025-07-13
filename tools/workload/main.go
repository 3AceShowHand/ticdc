// Copyright 2024 PingCAP, Inc.
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
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func main() {
	// create config object and parse command line arguments
	config := NewWorkloadConfig()
	if err := config.ParseFlags(); err != nil {
		log.Panic("Failed to parse flags", zap.Error(err))
	}
	log.Info("workload configured", zap.Any("config", config))

	// create application object
	app := NewWorkloadApp(config)

	// initialize application
	if err := app.Initialize(); err != nil {
		log.Panic("Failed to initialize application", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	if config.EnableProfiling {
		g.Go(func() error {
			addr := "localhost:6060"
			log.Info("enable the pprof, listening", zap.String("address", addr))
			return http.ListenAndServe(addr, nil)
		})
	}

	g.Go(func() error {
		// start metrics reporting
		app.StartMetricsReporting()
		return nil
	})

	g.Go(func() error {
		// execute workload
		err := app.Execute()
		if err != nil {
			log.Error("Error executing workload", zap.Error(err))
		}
		return err
	})

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Info("terminating: context cancelled")
	case <-sigterm:
		log.Info("terminating: via signal")
	}
	cancel()
	err := g.Wait()
	if err != nil {
		log.Error("workload exited with error", zap.Error(err))
	}
	log.Info("workload exited")
}
