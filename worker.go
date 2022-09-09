package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	http "net/http"
	"time"

	powerMeter "github.com/Nguyen-Hoa/wattsup"
	cpu "github.com/mackerelio/go-osstat/cpu"
	memory "github.com/mackerelio/go-osstat/memory"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type BaseWorker struct {
	// config
	Name         string
	Address      string
	CpuThresh    int
	PowerThresh  int
	Cores        int
	DynamicRange []int
	ManagerView  bool

	// status
	Available            bool
	LatestActualPower    int
	LatestPredictedPower int
	LatestCPU            int
	stats                map[string]interface{}
	runningJobs          map[string]DockerJob
}

type ServerWorker struct {
	BaseWorker

	_powerMeter *powerMeter.Wattsup
	_docker     *client.Client
}

type WorkerConfig struct {
	Name         string                 `json:"name"`
	Address      string                 `json:"address"`
	CpuThresh    int                    `json:"cpuThresh"`
	PowerThresh  int                    `json:"powerThresh"`
	Cores        int                    `json:"cores"`
	DynamicRange []int                  `json:"dynamicRange"`
	ManagerView  bool                   `json:"managerView"`
	Wattsup      powerMeter.WattsupArgs `json:"wattsup"`
}

func (w *ServerWorker) Init(config WorkerConfig) error {

	// Intialize Variables
	w.Name = config.Name
	w.Address = config.Address
	w.CpuThresh = config.CpuThresh
	w.PowerThresh = config.PowerThresh
	w.Cores = config.Cores
	w.DynamicRange = config.DynamicRange
	w.ManagerView = config.ManagerView

	w.Available = false
	w.LatestActualPower = 0
	w.LatestPredictedPower = 0
	w.LatestCPU = 0

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

func New(config WorkerConfig) (*BaseWorker, error) {
	w := BaseWorker{}
	// Intialize Variables
	w.Name = config.Name
	w.Address = config.Address
	w.CpuThresh = config.CpuThresh
	w.PowerThresh = config.PowerThresh
	w.Cores = config.Cores
	w.DynamicRange = config.DynamicRange
	w.ManagerView = config.ManagerView

	w.Available = false
	w.LatestActualPower = 0
	w.LatestPredictedPower = 0
	w.LatestCPU = 0

	return &w, nil
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
		return nil
	}
}

func (w *ServerWorker) VerifyImage(ID string) bool {
	if _, _, err := w._docker.ImageInspectWithRaw(context.Background(), ID); err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (w *ServerWorker) VerifyContainer(ID string) bool {
	if _, exists := w.runningJobs[ID]; exists {
		return true
	}
	return false
}

func (w *ServerWorker) StartJob(image string, cmd []string) error {
	// verify image exists
	if w.VerifyImage(image) {
		return errors.New("Image does not exist")
	}

	// create image
	resp, err := w._docker.ContainerCreate(context.Background(), &container.Config{
		Image: image,
		Cmd:   cmd,
	}, nil, nil, nil, "")
	if err != nil {
		panic(err)
	}

	// start image
	if err := w._docker.ContainerStart(context.Background(), resp.ID, types.ContainerStartOptions{}); err != nil {
		panic(err)
	}

	// update list of running jobs
	newCtr := DockerJob{
		BaseJob: BaseJob{
			StartTime:    time.Now(),
			TotalRunTime: time.Duration(0),
		},
		Container: types.Container{ID: resp.ID},
	}
	w.runningJobs[resp.ID] = newCtr

	return nil
}

func (w *ServerWorker) StopJob(ID string) error {
	if w.VerifyContainer(ID) {
		if err := w._docker.ContainerStop(context.Background(), ID, nil); err != nil {
			return err
		}
	} else {
		return errors.New("Failed to stop: Job ID not found")
	}

	ctr := w.runningJobs[ID]
	ctr.UpdateTotalRunTime(time.Now())

	return nil
}

func (w *ServerWorker) updateRunningJobs(containers []types.Container) error {
	for _, container := range containers {
		if w.VerifyContainer(container.ID) {
			newContainer := DockerJob{
				BaseJob:   w.runningJobs[container.ID].BaseJob,
				Container: container,
			}
			newContainer.UpdateTotalRunTime(time.Now())
			w.runningJobs[container.ID] = newContainer
		} else {
			log.Println("Found an orphan job")
			newCtr := DockerJob{
				BaseJob: BaseJob{
					StartTime:    time.Now(),
					TotalRunTime: time.Duration(0),
				},
				Container: types.Container{ID: container.ID},
			}
			w.runningJobs[container.ID] = newCtr
		}
	}
	return nil
}

func (w *ServerWorker) RunningJobs() ([]types.Container, error) {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}
	w.updateRunningJobs(containers)
	return containers, nil
}

func (w *ServerWorker) RunningJobsStats() (map[string]types.ContainerStats, error) {
	containers, err := w._docker.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, err
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
	return containerStats, nil
}

func (w *ServerWorker) Stats() (map[string]interface{}, error) {
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

	w.stats = stats
	return stats, nil
}

func (w *BaseWorker) Stats() (map[string]interface{}, error) {
	resp, err := http.Get(w.Address + "/stats")
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	stats := make(map[string]interface{})
	io.Copy(buf, resp.Body)
	json.Unmarshal(buf.Bytes(), &stats)

	w.stats = stats
	return stats, nil
}

func (w *ServerWorker) IsAvailable() bool {
	return w.Available
}

func (w *BaseWorker) IsAvailable() bool {
	resp, err := http.Get(w.Address + "/available")
	if err != nil {
		log.Fatalln(err)
		return false
	}

	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		return true
	} else {
		return false
	}

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
