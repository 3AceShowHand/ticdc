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

package maintainer

import (
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/tiflow/cdc/model"
)

type CombineScheduler struct {
	schedulers []Scheduler
}

func NewCombineScheduler(schedulers ...Scheduler) Scheduler {
	return &CombineScheduler{schedulers}
}

// Schedule generates schedule tasks based on the inputs.
func (c *CombineScheduler) Schedule(
	allInferiors []scheduler.InferiorID,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	stateMachines scheduler.Map[scheduler.InferiorID, *StateMachine],
) []*ScheduleTask {
	for _, scheduler := range c.schedulers {
		tasks := scheduler.Schedule(allInferiors, aliveCaptures, stateMachines)
		if len(tasks) != 0 {
			return tasks
		}
	}
	return nil
}

func (c *CombineScheduler) Name() string {
	return "combine-scheduler"
}