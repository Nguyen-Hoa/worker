package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	http "net/http"
	"net/rpc"
	"sync"

	job "github.com/Nguyen-Hoa/job"
)

func New(config WorkerConfig) (*ManagerWorker, error) {
	w := ManagerWorker{}
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
	w.RunningJobs = job.SharedDockerJobsMap{}
	w.RunningJobs.Init()

	return &w, nil
}

func (w *ManagerWorker) StartMeter() error {
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

func (w *ManagerWorker) StopMeter() error {
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

func (w *ManagerWorker) StartJob(image string, cmd []string, duration int) error {
	j := job.Job{Image: image, Cmd: cmd, Duration: duration}
	if w.RPCServer {
		var reply string
		if err := w.rpcClient.Call("RPCServerWorker.StartJob", j, &reply); err != nil {
			log.Print(err, reply)
			return err
		}
	} else {
		j, err := json.Marshal(j)
		if err != nil {
			return err
		}
		body := bytes.NewBuffer(j)
		if _, err := http.Post(w.Address+"/execute", "application/json", body); err != nil {
			return err
		}
	}
	return nil
}

func (w *ManagerWorker) Stats() (map[string]interface{}, error) {
	if w.RPCServer {
		var pollWaitGroup sync.WaitGroup
		var errs = make([]string, 0)

		pollWaitGroup.Add(1)
		go func() {
			defer pollWaitGroup.Done()
			var reply map[string]interface{}
			if err := w.rpcClient.Call("RPCServerWorker.Poll", "", &reply); err != nil {
				log.Print(err)
				errs = append(errs, err.Error())
			}
			w.stats = reply
		}()

		pollWaitGroup.Add(1)
		go func() {
			defer pollWaitGroup.Done()
			var reply map[string]job.DockerJob
			if err := w.rpcClient.Call("RPCServerWorker.GetRunningJobs", "", &reply); err != nil {
				log.Print(err)
				errs = append(errs, err.Error())
			}

			w.RunningJobs.InitFromMap(reply)
		}()

		pollWaitGroup.Wait()

		if len(errs) > 0 {
			return nil, errors.New(errs[0])
		}
		return w.stats, nil
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

func (w *ManagerWorker) ContainerStats() (map[string]string, error) {
	if w.RPCServer {
		var reply map[string]string
		if err := w.rpcClient.Call("RPCServerWorker.GetRunningJobsStats", "", &reply); err != nil {
			log.Print(err)
			return nil, err
		} else {
			log.Print(w.Name)
			log.Print(reply)
			return reply, nil
		}
	}
	return nil, nil
}

func (w *ManagerWorker) GetStats() map[string]interface{} {
	return w.stats
}

func (w *ManagerWorker) IsAvailable() bool {
	if w.RPCServer {
		var available bool
		if err := w.rpcClient.Call("RPCServerWorker.IsAvailable", "", &available); err != nil {
			log.Print(err)
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
