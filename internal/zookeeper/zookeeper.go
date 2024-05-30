package zookeeper

import (
	"log"
	"sort"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	// session is considered valid for this time after losing connection so ephemeral
	// nodes are retained.
	ZK_SESSION_TIMEOUT = 500 * time.Millisecond

	// Election namespace in zookeeper.
	ELECTION_NS = "/election"

	// Zookeeper address.
	ZOOKEEPER_ADDRESS = "localhost:2181"
)

type Zookeeper struct {
	// Connection to zookeeper server.
	zkClient *zk.Conn

	// Channel to receive events from keeper.
	eventChan <-chan zk.Event

	// Name of the node corresponding to this instance.
	znodeName string

	// Current leader name.
	leaderName string

	// If current instance is a leader this is set to true.
	isLeader bool

	// Notify caller on this channel when zookeeper disconnects.
	wait chan string
}

func NewZookeeper(wait chan string) (*Zookeeper, error) {
	zkpr := &Zookeeper{}
	zkpr.wait = wait
	return zkpr, zkpr.connect()
}

func (zkpr *Zookeeper) connect() error {
	// 1. Connect to zookeeper.
	var err error

	options := zk.WithLogInfo(false)
	zkpr.zkClient, zkpr.eventChan, err = zk.Connect([]string{ZOOKEEPER_ADDRESS},
		ZK_SESSION_TIMEOUT, options)
	if err != nil {
		log.Fatalf("error while creating zookeeper connection, %s", err)
	}

	go zkpr.eventHandler()
	return nil
}

// Creates an ephemeral znode and save the name of the znode created.
func (zkpr *Zookeeper) NominateSelf() error {
	znodePrefix := ELECTION_NS + "/candidate_"
	znodePath, err := zkpr.zkClient.Create(znodePrefix, []byte{},
		zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Println("error while creating znode", err)
		return err
	}

	zkpr.znodeName = znodePath[strings.Index(znodePath, "candidate_"):]
	log.Println("I am", zkpr.znodeName)
	return nil
}

func (zkpr *Zookeeper) ReElectLeader() error {
	var exists bool = false
	for !exists {
		childrens, _, err := zkpr.zkClient.Children(ELECTION_NS)
		if err != nil {
			log.Println("error while fetching election namespace children", err)
			return err
		}

		sort.Strings(childrens)
		zkpr.leaderName = childrens[0]

		if zkpr.leaderName == zkpr.znodeName {
			log.Printf("Hurray! I won, I '%s', am the leader now.", zkpr.leaderName)
			zkpr.isLeader = true
			return nil
		} else {
			zkpr.isLeader = false
			predecessorIndex := sort.SearchStrings(childrens, zkpr.znodeName)
			predecessor := childrens[predecessorIndex-1]
			log.Println("Uhh! I am not the leader. Watching predecessor", predecessor)
			exists, _, _, err = zkpr.zkClient.ExistsW(ELECTION_NS + "/" + predecessor)
			if err != nil {
				exists = false
				continue
			}
		}
	}

	return nil
}

func (zkpr *Zookeeper) eventHandler() {
	for {
		select {
		case event := <-zkpr.eventChan:
			zkpr.handleEvent(event)
		}
	}
}

func (zkpr *Zookeeper) handleEvent(event zk.Event) {
	switch event.Type {
	case zk.EventSession:
		switch event.State {
		case zk.StateConnected:
			log.Println("Connected to zookeeper")

		case zk.StateDisconnected:
			log.Println("Disconnected from zookeeper")
			zkpr.wait <- "Disconnected from zookeeper"
			return
		}
	case zk.EventNodeDeleted:
		zkpr.ReElectLeader()
	}
}
