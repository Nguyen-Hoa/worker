package worker

import (
	"context"
	"errors"

	powerMeter "github.com/Nguyen-Hoa/wattsup"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

type Worker struct {
	name        string
	address     string
	cpuThresh   int
	powerThresh int
	powerMeter  *powerMeter.Wattsup

	// parameters
	_latestPower int
	_latestCPU   int
	_docker      client.Client
	_runningJobs []types.Container
}

type WorkerConfig struct {
	Name        string                 `json:"name"`
	Address     string                 `json:"address"`
	CpuThresh   int                    `json:"cpuThresh"`
	PowerThresh int                    `json:"powerThresh"`
	Wattsup     powerMeter.WattsupArgs `json:"wattsup"`
}

func (w *Worker) Init(c WorkerConfig) error {

	// Intialize Variables
	w.name = c.Name
	w.address = c.Address
	w.cpuThresh = c.CpuThresh
	w.powerThresh = c.PowerThresh
	w.powerMeter = powerMeter.New(c.Wattsup)

	// Initialize Docker API
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		panic(err)
	}
	containers, err := cli.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		panic(err)
	}
	w._runningJobs = containers

	return nil
}

func (w *Worker) StartMeter() error {

	// Check if another meter is running
	if w.powerMeter.Running() {
		return errors.New("meter already running")
	} else if err := w.powerMeter.Start(); err != nil {
		return err
	} else {
		return nil
	}
}

func (w *Worker) StopMeter() error {
	if err := w.powerMeter.Stop(); err != nil {
		return err
	} else {
		return nil
	}
}

func (w *Worker) getPower() int {
	return w._latestPower
}

func (w *Worker) getCPU() int {
	return w._latestCPU
}

func (w *Worker) getRunningJobs() []types.Container {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		panic(err)
	}
	return containers
}
