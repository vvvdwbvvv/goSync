package config

import (
	"github.com/joho/godotenv"
	"log"
)

func LoadEnv() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("❌ can't load .env")
	}
	log.Println("✅ Environment variables loaded.")
}
