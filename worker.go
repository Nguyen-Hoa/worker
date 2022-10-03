package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	http "net/http"
	"net/rpc"
	"time"

	profile "github.com/Nguyen-Hoa/profile"
	powerMeter "github.com/Nguyen-Hoa/wattsup"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type WorkerConfig struct {
	Name         string                 `json:"name"`
	Address      string                 `json:"address"`
	CpuThresh    int                    `json:"cpuThresh"`
	PowerThresh  int                    `json:"powerThresh"`
	Cores        int                    `json:"cores"`
	DynamicRange []int                  `json:"dynamicRange"`
	ManagerView  bool                   `json:"managerView"`
	RPCServer    bool                   `json:"rpcServer"`
	RPCPort      string                 `json:"rpcPort"`
	Wattsup      powerMeter.WattsupArgs `json:"wattsup"`
}

type Job struct {
	Image    string   `json:"image"`
	Cmd      []string `json:"cmd"`
	Duration int      `json:"duration"`
}

/* --------------------
Base Worker
----------------------*/
type BaseWorker struct {
	// config
	Name         string
	Address      string
	CpuThresh    int
	PowerThresh  int
	Cores        int
	DynamicRange []int
	ManagerView  bool
	RPCServer    bool
	RPCPort      string
	rpcClient    *rpc.Client
	config       WorkerConfig

	// status
	Available            bool
	LatestActualPower    int
	LatestPredictedPower int
	LatestCPU            int
	stats                map[string]interface{}
	runningJobs          map[string]DockerJob
	jobsToKill           []DockerJob
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
	w.RPCServer = config.RPCServer
	w.RPCPort = config.RPCPort
	if config.RPCServer {
		client, err := rpc.DialHTTP("tcp", config.Address+config.RPCPort)
		if err != nil {
			log.Print(err)
			return nil, err
		}
		w.rpcClient = client
	} else {
		w.Address = "http://" + w.Address + ":8080"
	}

	w.Available = false
	w.LatestActualPower = 0
	w.LatestPredictedPower = 0
	w.LatestCPU = 0

	return &w, nil
}

func (w *BaseWorker) StartMeter() error {
	if w.RPCServer {
		var reply string
		if err := w.rpcClient.Call("RPCServerWorker.StartMeter", "", &reply); err != nil {
			return err
		}
	} else {
		if res, err := http.Post(w.Address+"/meter-start", "application/json", bytes.NewBufferString("")); res.StatusCode != 200 {
			return err
		}
	}
	return nil
}

func (w *BaseWorker) StopMeter() error {
	if w.RPCServer {
		var reply string
		if err := w.rpcClient.Call("RPCServerWorker.StopMeter", "", &reply); err != nil {
			return err
		}
	} else {
		if res, err := http.Post(w.Address+"/meter-stop", "application/json", bytes.NewBufferString("")); res.StatusCode != 200 {
			return err
		}
	}
	return nil
}

func (w *BaseWorker) StartJob(image string, cmd []string, duration int) error {
	job, err := json.Marshal(Job{Image: image, Cmd: cmd, Duration: duration})
	if err != nil {
		return err
	}

	body := bytes.NewBuffer(job)
	if _, err := http.Post(w.Address+"/execute", "application/json", body); err != nil {
		return err
	}
	return nil
}

func (w *BaseWorker) Stats() (map[string]interface{}, error) {
	if w.RPCServer {
		var reply map[string]interface{}
		if err := w.rpcClient.Call("RPCServerWorker.Stats", "", &reply); err != nil {
			return nil, err
		}
		w.stats = reply
		return reply, nil
	} else {
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
}

func (w *BaseWorker) GetStats() map[string]interface{} {
	return w.stats
}

func (w *BaseWorker) IsAvailable() bool {
	if w.RPCServer {
		var available bool
		if err := w.rpcClient.Call("RPCServerWorker.IsAvailable", "", &available); err != nil {
			log.Fatalln(err)
			return false
		}
	} else {
		resp, err := http.Get(w.Address + "/available")
		if err != nil {
			log.Fatalln(err)
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return false
		}
	}
	return true
}

/* --------------------
Server Worker
----------------------*/
type ServerWorker struct {
	BaseWorker

	_powerMeter *powerMeter.Wattsup
	_docker     *client.Client
}

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

	w.runningJobs = make(map[string]DockerJob)
	w.jobsToKill = []DockerJob{}

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

func (w *ServerWorker) StartJob(image string, cmd []string, duration int) error {
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
			Duration:     time.Duration(duration),
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
			updatedCtr := DockerJob{
				BaseJob:   w.runningJobs[container.ID].BaseJob,
				Container: container,
			}
			updatedCtr.UpdateTotalRunTime(time.Now())
			if updatedCtr.TotalRunTime >= updatedCtr.Duration {
				w.jobsToKill = append(w.jobsToKill, updatedCtr)
			}
			w.runningJobs[container.ID] = updatedCtr
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
			w.jobsToKill = append(w.jobsToKill, newCtr)
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
	done := make(chan bool, 1)
	go func(done chan bool) {
		w.RunningJobs()
		log.Print(w.runningJobs)
		log.Print(w.jobsToKill)
		w.killJobs()
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
	for _, job := range w.jobsToKill {
		if err := w.StopJob(job.ID); err != nil {
			log.Print(err)
		}
	}
	return nil
}

/* --------------------
RPC Server Worker
----------------------*/
type RPCServerWorker struct {
	ServerWorker
}

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

func (w *RPCServerWorker) Stats(_ string, reply *map[string]interface{}) error {
	stats, err := profile.Get11Stats()
	if err != nil {
		return err
	}
	*reply = stats
	return nil
}

func (w *RPCServerWorker) IsAvailable(_ string, reply *bool) error {
	*reply = w.Available
	return nil
}
