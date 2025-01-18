package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

var TimescaleDB *sql.DB

func InitTimescaleDB() {
	var err error
	TimescaleDB, err = sql.Open("postgres",
		fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
			os.Getenv("PG_HOST"), os.Getenv("PG_PORT"), os.Getenv("PG_DB"), os.Getenv("PG_USER"), os.Getenv("PG_PASSWORD")))

	if err != nil {
		log.Fatalf("❌ can't open TimescaleDB connection: %v", err)
	}
	
	err = TimescaleDB.Ping()
	if err != nil {
		log.Fatalf("❌ TimescaleDB is not responding: %v", err)
	}

	log.Println("✅ Successfully connected to TimescaleDB")
}
