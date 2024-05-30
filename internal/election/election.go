package election

import (
	"zookeeper-test/internal/zookeeper"
)

type Election struct {
	zk *zookeeper.Zookeeper
}

func NewElection(zk *zookeeper.Zookeeper) *Election {
	return &Election{
		zk: zk,
	}
}

func (election *Election) StartElection() error {
	if err := election.zk.NominateSelf(); err != nil {
		return err
	}

	if err := election.zk.ReElectLeader(); err != nil {
		return err
	}
	return nil
}
