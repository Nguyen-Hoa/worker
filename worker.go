package worker

import (
	"context"
	"errors"
	"log"
	"time"

	powerMeter "github.com/Nguyen-Hoa/wattsup"
	cpu "github.com/mackerelio/go-osstat/cpu"
	memory "github.com/mackerelio/go-osstat/memory"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

type Worker struct {
	// config
	Name         string
	Address      string
	CpuThresh    int
	PowerThresh  int
	Cores        int
	DynamicRange []int

	// status
	Available            bool
	LatestActualPower    int
	LatestPredictedPower int
	LatestCPU            int

	// parameters
	_powerMeter *powerMeter.Wattsup
	_docker     *client.Client
	runningJobs []types.Container
}

type WorkerConfig struct {
	Name         string                 `json:"name"`
	Address      string                 `json:"address"`
	CpuThresh    int                    `json:"cpuThresh"`
	PowerThresh  int                    `json:"powerThresh"`
	Cores        int                    `json:"cores"`
	DynamicRange []int                  `json:"dynamicRange"`
	Wattsup      powerMeter.WattsupArgs `json:"wattsup"`
}

func (w *Worker) Init(c WorkerConfig) error {

	// Intialize Variables
	w.Name = c.Name
	w.Address = c.Address
	w.CpuThresh = c.CpuThresh
	w.PowerThresh = c.PowerThresh
	w.Cores = c.Cores
	w.DynamicRange = c.DynamicRange

	w.Available = true
	w.LatestActualPower = 0
	w.LatestPredictedPower = 0
	w.LatestCPU = 0

	// Initialize Power Meter
	w._powerMeter = powerMeter.New(c.Wattsup)

	// Initialize Docker API
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	w._docker = cli
	containers, err := cli.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return err
	}
	w.runningJobs = containers

	return nil
}

func (w *Worker) StartMeter() error {

	// Check if another meter is running
	if w._powerMeter.Running() {
		return errors.New("meter already running")
	} else if err := w._powerMeter.Start(); err != nil {
		return err
	} else {
		return nil
	}
}

func (w *Worker) StopMeter() error {
	if err := w._powerMeter.Stop(); err != nil {
		return err
	} else {
		return nil
	}
}

func (w *Worker) getActualPower() int {
	return w.LatestActualPower
}

func (w *Worker) getCPU() int {
	return w.LatestCPU
}

func (w *Worker) RunningJobs() ([]types.Container, error) {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}
	return containers, nil
}

func (w *Worker) RunningJobsStats() (map[string]types.ContainerStats, error) {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}
	w.runningJobs = containers

	var containerStats map[string]types.ContainerStats = make(map[string]types.ContainerStats)
	for _, container := range containers {
		stats, err := w._docker.ContainerStats(context.Background(), container.ID, false)
		if err != nil {
			log.Println("Failed to get stats for {}", container.ID)
		}
		containerStats[container.ID] = stats
	}
	return containerStats, nil
}

func (w *Worker) Stats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	//cpu usage
	user, system, idle, err := cpuStats()
	if err != nil {
		log.Print("Error getting cpu usage...")
	} else {
		stats["cpu_user"] = user
		stats["cpu_system"] = system
		stats["cpu_idle"] = idle
	}

	// memory usage
	memory, err := memory.Get()
	if err != nil {
		log.Print("Error getting memory usage...")
		return nil, err
	} else {
		stats["mem_total"] = memory.Total
		stats["mem_used"] = memory.Used
		stats["mem_cached"] = memory.Cached
		stats["mem_free"] = memory.Free
	}

	return stats, nil
}

func cpuStats() (float64, float64, float64, error) {
	before, err := cpu.Get()
	if err != nil {
		return 0.0, 0.0, 0.0, err
	}
	time.Sleep(time.Duration(1) * time.Second)
	after, err := cpu.Get()
	if err != nil {
		return 0.0, 0.0, 0.0, err
	}
	total := float64(after.Total - before.Total)

	user := float64(after.User-before.User) / total * 100
	system := float64(after.System-before.System) / total * 100
	idle := float64(after.Idle-before.Idle) / total * 100

	return user, system, idle, nil
}
