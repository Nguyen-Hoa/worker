package worker

import (
	"errors"

	powerMeter "github.com/Nguyen-Hoa/wattsup"
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
}

type WorkerConfig struct {
	Name        string                 `json:"name"`
	Address     string                 `json:"address"`
	CpuThresh   int                    `json:"cpuThresh"`
	PowerThresh int                    `json:"powerThresh"`
	Wattsup     powerMeter.WattsupArgs `json:"wattsup"`
}

func (w *Worker) Init(c WorkerConfig) error {
	w.name = c.Name
	w.address = c.Address
	w.cpuThresh = c.CpuThresh
	w.powerThresh = c.PowerThresh
	w.powerMeter = powerMeter.New(c.Wattsup)
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
