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

func (w *Worker) Init(
	name string,
	address string,
	cpuThresh int,
	powerThresh int,
	powerMeterParams powerMeter.WattsupArgs,
) error {
	w.name = name
	w.address = address
	w.cpuThresh = cpuThresh
	w.powerThresh = powerThresh
	w.powerMeter = powerMeter.New(powerMeterParams)
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
