package main

import (
	"goSync/cmd"
	"goSync/config"
	"goSync/db"
	"log"
)

func main() {

	log.Println("🚀 Up and Running goSync data processor...")

	config.LoadEnv()

	db.InitRedis()
	db.InitTimescaleDB()

	cmd.ConsumeKafka()
}
