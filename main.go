package main

import (
	"fmt"
	"log"

	"zookeeper-test/internal/election"
	"zookeeper-test/internal/zookeeper"
)

func main() {
	var wait chan string
	zk, err := zookeeper.NewZookeeper(wait)
	if err != nil {
		log.Fatal("unable to connect to zookeeper ", err)
	}
	electionTerm := election.NewElection(zk)
	if err := electionTerm.StartElection(); err != nil {
		log.Fatal("error during election ", err)
	}

	fmt.Println(<-wait)
}
