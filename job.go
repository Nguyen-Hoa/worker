package worker

import (
	"time"

	"github.com/docker/docker/api/types"
)

type BaseJob struct {
	StartTime    time.Time
	StopTime     time.Time
	TotalRunTime time.Duration
}

type DockerJob struct {
	BaseJob
	types.Container
}

func (j *BaseJob) UpdateTotalRunTime(time.Time) error {
	j.TotalRunTime += time.Since(j.StartTime)
	return nil
}
