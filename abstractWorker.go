package worker

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	http "net/http"
	"net/rpc"
)

func New(config WorkerConfig) (*AbstractWorker, error) {
	w := AbstractWorker{}
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

func (w *AbstractWorker) StartMeter() error {
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

func (w *AbstractWorker) StopMeter() error {
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

func (w *AbstractWorker) StartJob(image string, cmd []string, duration int) error {
	job := Job{Image: image, Cmd: cmd, Duration: duration}
	if w.RPCServer {
		var reply string
		if err := w.rpcClient.Call("RPCServerWorker.StartJob", job, &reply); err != nil {
			log.Print(err, reply)
			return err
		}
	} else {
		job, err := json.Marshal(job)
		if err != nil {
			return err
		}
		body := bytes.NewBuffer(job)
		if _, err := http.Post(w.Address+"/execute", "application/json", body); err != nil {
			return err
		}
	}
	return nil
}

func (w *AbstractWorker) Stats() (map[string]interface{}, error) {
	if w.RPCServer {
		var reply map[string]interface{}
		if err := w.rpcClient.Call("RPCServerWorker.Poll", "", &reply); err != nil {
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

func (w *AbstractWorker) GetStats() map[string]interface{} {
	return w.stats
}

func (w *AbstractWorker) IsAvailable() bool {
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
