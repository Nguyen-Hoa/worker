package worker

import (
	"context"
	"errors"
	"log"
	"time"

	profile "github.com/Nguyen-Hoa/profile"
	powerMeter "github.com/Nguyen-Hoa/wattsup"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

func (w *RPCServerWorker) Init(config WorkerConfig) error {
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

	w.RunningJobs = make(map[string]DockerJob)
	w.jobsToKill = make(map[string]DockerJob)

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
		w.updateRunningJobs(containers)
	}

	return nil
}

func (w *RPCServerWorker) StartMeter(_ string, reply *string) error {
	if w._powerMeter.Running() {
		*reply = "meter already running"
		return errors.New("meter already running")
	} else if err := w._powerMeter.Start(); err != nil {
		*reply = err.Error()
		return err
	} else {
		return nil
	}
}

func (w *RPCServerWorker) StopMeter(_ string, reply *string) error {
	if err := w._powerMeter.Stop(); err != nil {
		*reply = err.Error()
		return err
	} else {
		w._powerMeter = powerMeter.New(w.config.Wattsup)
		return nil
	}
}

func (w *RPCServerWorker) verifyImage(ID string) bool {
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

func (w *RPCServerWorker) verifyContainer(ID string) bool {
	if _, exists := w.RunningJobs[ID]; exists {
		return true
	}
	return false
}

func (w *RPCServerWorker) StartJob(job Job, reply *string) error {
	log.Print("recvd start job")

	// verify image exists
	if !w.verifyImage(job.Image) {
		*reply = "image does not exist"
		return errors.New("image does not exist")
	}

	// create image
	resp, err := w._docker.ContainerCreate(context.Background(), &container.Config{
		Image: job.Image,
		Cmd:   job.Cmd,
	}, nil, nil, nil, "")
	if err != nil {
		*reply = err.Error()
		return err
	}

	// start image
	if err := w._docker.ContainerStart(context.Background(), resp.ID, types.ContainerStartOptions{}); err != nil {
		*reply = err.Error()
		return err
	}

	// update list of running jobs
	newCtr := DockerJob{
		BaseJob: BaseJob{
			StartTime:    time.Now(),
			TotalRunTime: time.Duration(0),
			Duration:     time.Duration(job.Duration) * time.Second,
		},
		Container: types.Container{ID: resp.ID},
	}
	w.RunningJobs[resp.ID] = newCtr
	*reply = resp.ID

	return nil
}

func (w *RPCServerWorker) stopJob(ID string) error {
	if w.verifyContainer(ID) {
		if err := w._docker.ContainerStop(context.Background(), ID, nil); err != nil {
			return err
		}
	} else {
		return errors.New("failed to stop: Job ID not found")
	}
	ctr := w.RunningJobs[ID]
	ctr.UpdateTotalRunTime(time.Now())
	log.Printf("Stopped %s, total run time: %s", ID, ctr.TotalRunTime)
	return nil
}

func (w *RPCServerWorker) updateRunningJobs(containers []types.Container) (map[string]DockerJob, error) {
	updatedRunningJobs := make(map[string]DockerJob)
	for _, container := range containers {
		if w.verifyContainer(container.ID) {
			updatedCtr := DockerJob{
				BaseJob:   w.RunningJobs[container.ID].BaseJob,
				Container: container,
			}
			updatedCtr.UpdateTotalRunTime(time.Now())
			if updatedCtr.TotalRunTime >= updatedCtr.Duration {
				w.jobsToKill[updatedCtr.ID] = updatedCtr
			}
			updatedRunningJobs[container.ID] = updatedCtr
		} else {
			log.Println("Found an orphan job")
			newCtr := DockerJob{
				BaseJob: BaseJob{
					StartTime:    time.Now(),
					TotalRunTime: time.Duration(0),
					Duration:     time.Duration(-1),
				},
				Container: types.Container{ID: container.ID},
			}
			w.jobsToKill[newCtr.ID] = newCtr
			w.RunningJobs[container.ID] = newCtr
		}
	}
	w.RunningJobs = updatedRunningJobs
	return updatedRunningJobs, nil
}

func (w *RPCServerWorker) GetRunningJobs(_ string, reply *map[string]DockerJob) error {
	containers, err := w.getRunningJobs()
	if err != nil {
		return err
	}

	RunningJobs, _ := w.updateRunningJobs(containers)
	w.killJobs()
	*reply = RunningJobs
	return nil
}

func (w *RPCServerWorker) getRunningJobs() ([]types.Container, error) {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}
	w.updateRunningJobs(containers)
	return containers, nil
}

func (w *RPCServerWorker) GetRunningJobsStats(_ string, reply *map[string]types.ContainerStats) error {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return err
	}
	w.updateRunningJobs(containers)

	var containerStats map[string]types.ContainerStats = make(map[string]types.ContainerStats)
	for _, container := range containers {
		stats, err := w._docker.ContainerStats(context.Background(), container.ID, false)
		if err != nil {
			log.Println("Failed to get stats for {}", container.ID)
		}
		containerStats[container.ID] = stats
	}

	*reply = containerStats
	return nil
}

func (w *RPCServerWorker) Poll(_ string, reply *map[string]interface{}) error {

	if res, err := profile.Get11Stats(); err == nil {
		log.Print(res)
		*reply = res
	} else {
		log.Print(err)
		return err
	}

	return nil
}

func (w *RPCServerWorker) IsAvailable(_ string, reply *bool) error {
	*reply = w.Available
	return nil
}

func (w *RPCServerWorker) killJobs() error {
	for id := range w.jobsToKill {
		if err := w.stopJob(id); err != nil {
			log.Print(err)
		} else {
			delete(w.jobsToKill, id)
			delete(w.RunningJobs, id)
		}
	}
	return nil
}
