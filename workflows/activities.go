package workflows

import (
	"peerdb-playground/services/flows"
	"peerdb-playground/services/peers"
)

type Activities struct {
	flowsSvc *flows.Service
	peersSvc *peers.Service
}

var activities *Activities

func Init(a *Activities, flowsSvc *flows.Service, peersSvc *peers.Service) {
	a.flowsSvc = flowsSvc
	a.peersSvc = peersSvc
	activities = a
}
