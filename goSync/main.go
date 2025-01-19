package main

import (
	"log"

	"github.com/QuantDevops/SyncHub/goSync/cmd"
	"github.com/QuantDevops/SyncHub/goSync/config"
	"github.com/QuantDevops/SyncHub/goSync/db"
)

func main() {

	log.Println("🚀 Up and Running goSync data processor...")

	config.LoadEnv()

	db.InitRedis()
	db.InitTimescaleDB()

	cmd.ConsumeKafka()

}
