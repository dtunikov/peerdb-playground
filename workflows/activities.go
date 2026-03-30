package workflows

import (
	"peerdb-playground/config"
	"peerdb-playground/services/flows"
	"peerdb-playground/services/peers"
)

type Activities struct {
	flowsSvc *flows.Service
	peersSvc *peers.Service
	cdcCfg   config.CdcConfig
}

var activities *Activities

func Init(a *Activities, flowsSvc *flows.Service, peersSvc *peers.Service, cdcCfg config.CdcConfig) {
	a.flowsSvc = flowsSvc
	a.peersSvc = peersSvc
	a.cdcCfg = cdcCfg
	activities = a
}
