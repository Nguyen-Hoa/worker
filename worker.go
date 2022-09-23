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

	profile "github.com/Nguyen-Hoa/profile"
	powerMeter "github.com/Nguyen-Hoa/wattsup"

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

type Job struct {
	Image string   `json:"image"`
	Cmd   []string `json:"cmd"`
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

	w.runningJobs = make(map[string]DockerJob)

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

func (w *BaseWorker) StartMeter() error {
	if res, err := http.Post(w.Address+"/meter-start", "", nil); res.StatusCode != 200 {
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

func (w *BaseWorker) StopMeter() error {
	if res, err := http.Post(w.Address+"/meter-stop", "", nil); res.StatusCode != 200 {
		return err
	} else {
		return nil
	}
}

func (w *ServerWorker) VerifyImage(ID string) bool {
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

func (w *ServerWorker) VerifyContainer(ID string) bool {
	if _, exists := w.runningJobs[ID]; exists {
		return true
	}
	return false
}

func (w *ServerWorker) StartJob(image string, cmd []string) error {
	// verify image exists
	if !w.VerifyImage(image) {
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

func (w *BaseWorker) StartJob(image string, cmd []string) error {
	job, err := json.Marshal(Job{Image: image, Cmd: cmd})
	if err != nil {
		return err
	}

	body := bytes.NewBuffer(job)
	if _, err := http.Post(w.Address+"/execute", "application/json", body); err != nil {
		return err
	}
	return nil
}

func (w *ServerWorker) StopJob(ID string) error {
	if w.VerifyContainer(ID) {
		if err := w._docker.ContainerStop(context.Background(), ID, nil); err != nil {
			return err
		}
	} else {
		return errors.New("failed to stop: Job ID not found")
	}

	ctr := w.runningJobs[ID]
	ctr.UpdateTotalRunTime(time.Now())

	log.Printf("Stopped %s, total run time: %s", ID, ctr.TotalRunTime)
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
	stats, err := profile.Get11Stats()
	if err != nil {
		return nil, err
	}
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

func (w *BaseWorker) GetStats() map[string]interface{} {
	return w.stats
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
