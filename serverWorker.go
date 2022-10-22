package worker

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
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
	w.Hostname, _ = os.Hostname()
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

	w.RunningJobStats = make(map[string]interface{})
	w.RunningJobs = job.SharedDockerJobsMap{}
	w.jobsToKill = job.SharedDockerJobsMap{}
	w.RunningJobs.Init()
	w.jobsToKill.Init()

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
	if w._powerMeter.Running() {
		if err := w._powerMeter.Stop(); err != nil {
			return err
		} else {
			w._powerMeter = powerMeter.New(w.config.Wattsup)
		}
	}
	if err := w._powerMeter.Start(); err != nil {
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

func (w *ServerWorker) StartJob(image string, cmd []string, duration int) (string, error) {
	// verify image exists
	if !w.verifyImage(image) {
		return "", errors.New("image does not exist")
	}

	// create image
	resp, err := w._docker.ContainerCreate(context.Background(), &container.Config{
		Image: image,
		Cmd:   cmd,
	}, nil, nil, nil, "")
	if err != nil {
		log.Print(err)
		return "", err
	}

	// start image
	if err := w._docker.ContainerStart(context.Background(), resp.ID, types.ContainerStartOptions{}); err != nil {
		log.Print(err)
		return "", err
	}

	log.Print("started job ", duration)

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

	return resp.ID, nil
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

func (w *ServerWorker) updateGetRunningJobs(containers []types.Container) (job.SharedDockerJobsMap, error) {
	ids := make([]string, 0)
	for _, container := range containers {

		// found existing job
		if w.verifyContainer(container.ID) && container.ID[:13] != w.Hostname {
			base, _ := w.RunningJobs.Get(container.ID)
			updatedCtr := job.DockerJob{
				BaseJob:   base.BaseJob,
				Container: container,
			}
			updatedCtr.UpdateTotalRunTime(time.Now())
			if updatedCtr.TotalRunTime >= updatedCtr.Duration {
				w.jobsToKill.Update(updatedCtr.ID, updatedCtr)
			}
		} else { // found orphan job
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

	// remove stale jobs
	w.RunningJobs.Refresh(ids)
	return w.RunningJobs, nil
}

func (w *ServerWorker) GetRunningJobs() (map[string]job.DockerJob, error) {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}
	RunningJobs, _ := w.updateGetRunningJobs(containers)
	w.killJobs()
	return RunningJobs.Snap(), nil
}

func (w *ServerWorker) GetRunningJobsStats() (map[string][]byte, error) {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}
	w.updateGetRunningJobs(containers)

	var containerStats map[string][]byte = make(map[string][]byte)
	for _, container := range containers {
		if container.ID[:13] != w.Hostname {
			stats, err := w._docker.ContainerStatsOneShot(context.Background(), container.ID)
			if err != nil {
				log.Println("Failed to get stats for {}", container.ID)
			}
			defer stats.Body.Close()
			raw_stats, err := io.ReadAll(stats.Body)
			if err != nil {
				log.Print(err)
			}
			containerStats[container.ID] = raw_stats
		}
	}
	return containerStats, nil
}

func (w *ServerWorker) Stats() (map[string]interface{}, error) {
	stats, err := profile.Get11Stats()
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (w *ServerWorker) ReducedStats() (map[string]interface{}, error) {
	stats, err := profile.GetCPUAndMemStats()
	if err != nil {
		return nil, err
	}
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
