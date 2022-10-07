package worker

import (
	"context"
	"errors"
	"log"
	"time"

	job "github.com/Nguyen-Hoa/job"
	profile "github.com/Nguyen-Hoa/profile"
	powerMeter "github.com/Nguyen-Hoa/wattsup"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

func (w *ServerWorker) Init(config WorkerConfig) error {
	w.config = config

	// Intialize Variables
	w.Name = config.Name
	w.Address = config.Address
	w.CpuThresh = config.CpuThresh
	w.PowerThresh = config.PowerThresh
	w.Cores = config.Cores
	w.DynamicRange = config.DynamicRange
	w.ManagerView = config.ManagerView
	w.RPCServer = config.RPCServer
	w.RPCPort = config.RPCPort

	w.Available = false
	w.LatestActualPower = 0
	w.LatestPredictedPower = 0
	w.LatestCPU = 0

	w.RunningJobs = job.SharedDockerJobsMap{}
	w.jobsToKill = job.SharedDockerJobsMap{}

	if !w.ManagerView {
		// Initialize Power Meter
		w._powerMeter = powerMeter.New(config.Wattsup)

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
		w.updateGetRunningJobs(containers)
	}

	return nil
}

func (w *ServerWorker) StartMeter() error {
	// Check if another meter is running
	if w._powerMeter.Running() {
		return errors.New("meter already running")
	} else if err := w._powerMeter.Start(); err != nil {
		return err
	} else {
		return nil
	}
}

func (w *ServerWorker) StopMeter() error {
	if err := w._powerMeter.Stop(); err != nil {
		return err
	} else {
		w._powerMeter = powerMeter.New(w.config.Wattsup)
		return nil
	}
}

func (w *ServerWorker) verifyImage(ID string) bool {
	if _, _, err := w._docker.ImageInspectWithRaw(context.Background(), ID); err != nil {
		log.Println(err)
		log.Println("Attempting to pull image...")
		if _, err := w._docker.ImagePull(context.Background(), ID, types.ImagePullOptions{}); err != nil {
			log.Println(err)
			log.Println("Failed to pull image...")
		}
	}
	return true
}

func (w *ServerWorker) verifyContainer(ID string) bool {
	if _, exists := w.RunningJobs.Get(ID); exists {
		return true
	}
	return false
}

func (w *ServerWorker) StartJob(image string, cmd []string, duration int) error {
	// verify image exists
	if !w.verifyImage(image) {
		return errors.New("image does not exist")
	}

	// create image
	resp, err := w._docker.ContainerCreate(context.Background(), &container.Config{
		Image: image,
		Cmd:   cmd,
	}, nil, nil, nil, "")
	if err != nil {
		panic(err)
	}

	log.Print("starting job", duration)

	// start image
	if err := w._docker.ContainerStart(context.Background(), resp.ID, types.ContainerStartOptions{}); err != nil {
		panic(err)
	}

	// update list of running jobs
	newCtr := job.DockerJob{
		BaseJob: job.BaseJob{
			StartTime:    time.Now(),
			TotalRunTime: time.Duration(0),
			Duration:     time.Duration(duration) * time.Second,
		},
		Container: types.Container{ID: resp.ID},
	}
	w.RunningJobs.Update(resp.ID, newCtr)

	return nil
}

func (w *ServerWorker) StopJob(ID string) error {
	if w.verifyContainer(ID) {
		if err := w._docker.ContainerStop(context.Background(), ID, nil); err != nil {
			return err
		}
	} else {
		return errors.New("failed to stop: Job ID not found")
	}

	ctr, _ := w.RunningJobs.Get(ID)
	ctr.UpdateTotalRunTime(time.Now())

	log.Printf("Stopped %s, total run time: %s", ID, ctr.TotalRunTime)
	return nil
}

func (w *ServerWorker) updateGetRunningJobs(containers []types.Container) error {
	ids := make([]string, 0)
	for _, container := range containers {
		if w.verifyContainer(container.ID) {
			base, _ := w.RunningJobs.Get(container.ID)
			updatedCtr := job.DockerJob{
				BaseJob:   base.BaseJob,
				Container: container,
			}
			updatedCtr.UpdateTotalRunTime(time.Now())
			if updatedCtr.TotalRunTime >= updatedCtr.Duration {
				w.jobsToKill.Update(updatedCtr.ID, updatedCtr)
			}
		} else {
			log.Println("Found an orphan job")
			newCtr := job.DockerJob{
				BaseJob: job.BaseJob{
					StartTime:    time.Now(),
					TotalRunTime: time.Duration(0),
					Duration:     time.Duration(-1),
				},
				Container: types.Container{ID: container.ID},
			}
			w.jobsToKill.Update(newCtr.ID, newCtr)
			w.RunningJobs.Update(container.ID, newCtr)
		}
		ids = append(ids, container.ID)
	}
	w.RunningJobs.Refresh(ids)
	return nil
}

func (w *ServerWorker) GetRunningJobs() ([]types.Container, error) {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}
	w.updateGetRunningJobs(containers)
	return containers, nil
}

func (w *ServerWorker) GetRunningJobsStats() (map[string]types.ContainerStats, error) {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}
	w.updateGetRunningJobs(containers)

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

func (w *ServerWorker) Stats() (map[string]interface{}, error) {
	done := make(chan bool, 1)
	go func(done chan bool) {
		w.GetRunningJobs()
		w.killJobs()
		done <- true
	}(done)
	stats, err := profile.Get11Stats()
	if err != nil {
		return nil, err
	}

	<-done
	return stats, nil
}

func (w *ServerWorker) IsAvailable() bool {
	return w.Available
}

func (w *ServerWorker) killJobs() error {
	for _, id := range w.jobsToKill.Keys() {
		if err := w.StopJob(id); err != nil {
			log.Print(err)
		} else {
			w.jobsToKill.Delete(id)
			w.RunningJobs.Delete(id)
		}
	}
	return nil
}
