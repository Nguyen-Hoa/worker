package worker

import (
	"net/rpc"

	job "github.com/Nguyen-Hoa/job"
	powerMeter "github.com/Nguyen-Hoa/wattsup"

	"github.com/docker/docker/client"
)

type WorkerConfig struct {
	Name         string                 `json:"name"`
	Address      string                 `json:"address"`
	CpuThresh    float32                `json:"cpuThresh"`
	PowerThresh  float32                `json:"powerThresh"`
	Cores        int                    `json:"cores"`
	DynamicRange []float32              `json:"dynamicRange"`
	ManagerView  bool                   `json:"managerView"`
	RPCServer    bool                   `json:"rpcServer"`
	RPCPort      string                 `json:"rpcPort"`
	HTTPPort     string                 `json:"httpPort"`
	Wattsup      powerMeter.WattsupArgs `json:"wattsup"`
}

/* --------------------
Base Worker
----------------------*/
type worker struct {
	// config
	Name          string
	Address       string
	Hostname      string
	CpuThresh     float32
	PowerThresh   float32
	Cores         int
	DynamicRange  []float32
	ManagerView   bool
	RPCServer     bool
	RPCPort       string
	HTTPPort      string
	rpcClient     *rpc.Client
	config        WorkerConfig
	HasPowerMeter bool

	// status
	Available            bool
	LatestActualPower    float32
	LatestPredictedPower float32
	LatestCPU            float32
	stats                map[string]interface{}
	RunningJobStats      map[string]interface{}
	RunningJobs          job.SharedDockerJobsMap
	jobsToKill           job.SharedDockerJobsMap
}

/* --------------------
Worker (Abstract)
----------------------*/
// type Worker interface {
// 	Init()
// 	StartMeter()
// 	StopMeter()
// 	StartJob()
// 	StopJob()
// 	Poll()
// 	IsAvailable()
// }

/* --------------------
Manager Worker
----------------------*/
type ManagerWorker struct {
	worker
}

/* --------------------
HTTP Server Worker
----------------------*/
type ServerWorker struct {
	worker

	_powerMeter *powerMeter.Wattsup
	_docker     *client.Client
}

/* --------------------
RPC Server Worker
----------------------*/
type RPCServerWorker struct {
	worker

	_powerMeter *powerMeter.Wattsup
	_docker     *client.Client
}
