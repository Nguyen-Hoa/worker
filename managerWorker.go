package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	http "net/http"
	"net/rpc"
	"strings"
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

	w.RPCServer = config.RPCServer
	w.RPCPort = config.RPCPort
	w.HTTPPort = config.HTTPPort
	if config.RPCServer && config.RPCPort != "" {
		client, err := rpc.DialHTTP("tcp", config.Address+config.RPCPort)
		if err != nil {
			log.Print(err)
			return nil, err
		}
		w.rpcClient = client
	} else if config.HTTPPort != "" {
		w.Address = "http://" + w.Address + w.HTTPPort
	} else {
		return nil, errors.New("no valid rpc or http configuration provided")
	}

	w.Available = w.IsAvailable()
	if !w.Available {
		return nil, errors.New("worker not available, check that worker is running")
	}

	w.HasPowerMeter = w.PowerMeterOn()
	if w.HasPowerMeter {
		if err := w.StartMeter(); err != nil {
			log.Println("Worker meter failure", w.Name)
			return nil, err
		}
	}

	w.LatestActualPower = 0
	w.LatestPredictedPower = 0
	w.LatestCPU = 0
	w.RunningJobStats = make(map[string]interface{})
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

func (w *ManagerWorker) StopMeter() (string, error) {
	var reply string
	if w.RPCServer {
		if err := w.rpcClient.Call("RPCServerWorker.StopMeter", "", &reply); err != nil {
			return "", err
		}
	} else {
		if res, err := http.Post(w.Address+"/meter-stop", "application/json", bytes.NewBufferString("")); res.StatusCode != 200 {
			return "", err
		} else {
			defer res.Body.Close()
			buf := new(bytes.Buffer)
			io.Copy(buf, res.Body)
			body := make(map[string]interface{})
			json.Unmarshal(buf.Bytes(), &body)
			reply = body["path"].(string)
		}
	}
	return reply, nil
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

func (w *ManagerWorker) Stats(reduced bool) (map[string]interface{}, error) {
	if w.RPCServer {
		var pollWaitGroup sync.WaitGroup
		var errs = make([]string, 0)

		pollWaitGroup.Add(1)
		go func() {
			defer pollWaitGroup.Done()
			var endpoint string = "RPCServerWorker.Poll"
			if reduced {
				endpoint = "RPCServerWorker.ReducedStats"
			}
			var reply map[string]interface{}
			if err := w.rpcClient.Call(endpoint, "", &reply); err != nil {
				log.Print(err)
				errs = append(errs, err.Error())
			}
			w.stats = reply
		}()

		pollWaitGroup.Add(1)
		go func() {
			defer pollWaitGroup.Done()
			var reply map[string][]byte
			if err := w.rpcClient.Call("RPCServerWorker.GetRunningJobsStats", "", &reply); err != nil {
				log.Print(err)
				errs = append(errs, err.Error())
			} else {
				for key := range reply {
					var stat map[string]interface{}
					json.Unmarshal(reply[key], &stat)
					w.RunningJobStats[key] = stat
				}
			}
		}()

		pollWaitGroup.Wait()

		if len(errs) > 0 {
			return nil, errors.New(errs[0])
		}
		return w.stats, nil
	} else {
		var pollWaitGroup sync.WaitGroup
		var errs = make([]string, 0)

		pollWaitGroup.Add(1)
		go func() {
			defer pollWaitGroup.Done()
			var endpoint string = "/stats"
			if reduced {
				endpoint = "/reduced-stats"
			}
			resp, err := http.Get(w.Address + endpoint)
			if err != nil {
				errs = append(errs, err.Error())
			}
			defer resp.Body.Close()
			buf := new(bytes.Buffer)
			stats := make(map[string]interface{})
			io.Copy(buf, resp.Body)
			json.Unmarshal(buf.Bytes(), &stats)
			w.stats = stats
		}()

		pollWaitGroup.Add(1)
		go func() {
			defer pollWaitGroup.Done()
			resp, err := http.Get(w.Address + "/running_jobs_stats")
			if err != nil {
				errs = append(errs, err.Error())
			}
			defer resp.Body.Close()
			buf := new(bytes.Buffer)
			io.Copy(buf, resp.Body)
			stats := make(map[string]interface{})
			json.Unmarshal(buf.Bytes(), &stats)
			w.RunningJobStats = stats
		}()
		pollWaitGroup.Wait()
		if len(errs) > 0 {
			return nil, errors.New(errs[0])
		}
		return w.stats, nil
	}
}

func (w *ManagerWorker) ContainerStats() (map[string][]byte, error) {
	if w.RPCServer {
		var reply map[string][]byte
		if err := w.rpcClient.Call("RPCServerWorker.GetRunningJobsStats", "", &reply); err != nil {
			log.Print(err)
			return nil, err
		} else {
			for key := range reply {
				var stat map[string]interface{}
				json.Unmarshal(reply[key], &stat)
			}
			return reply, nil
		}
	} else {
		resp, err := http.Get(w.Address + "/running_jobs_stats")
		if err != nil {
			log.Print(err)
			return nil, err
		}
		defer resp.Body.Close()
		buf := new(bytes.Buffer)
		io.Copy(buf, resp.Body)

		stats := make(map[string][]byte)
		json.Unmarshal(buf.Bytes(), &stats)
		for key := range stats {
			var stat map[string]interface{}
			json.Unmarshal(stats[key], &stat)
			w.RunningJobStats[key] = stat
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

func (w *ManagerWorker) PowerMeterOn() bool {
	if w.RPCServer {
		var available bool
		if err := w.rpcClient.Call("RPCServerWorker.PowerMeterOn", "", &available); err != nil {
			log.Print(err)
			return false
		}
		return available
	} else {
		resp, err := http.Get(w.Address + "/has-power-meter")
		if err != nil {
			log.Fatalln(err)
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return false
		}
		buf := new(bytes.Buffer)
		io.Copy(buf, resp.Body)
		if strings.Contains(buf.String(), "not running") {
			return false
		} else {
			return true
		}
	}
}
