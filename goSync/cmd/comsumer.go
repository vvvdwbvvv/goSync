package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"goSync/db"
	"goSync/models"

	"github.com/segmentio/kafka-go"
)

func processBinanceTrade(bt models.BinanceTrade) {
	// trade time to seconds
	tradeSec := float64(bt.TradeTime) / 1000.0

	// write to Redis
	ctx := context.Background()
	db.RedisClient.HSet(ctx, fmt.Sprintf("market:%s", bt.Symbol),
		"price", bt.Price,
		"qty", bt.Qty,
	)
	log.Printf("✅ Saved to Redis: %s, price=%.2f, qty=%.2f\n",
		bt.Symbol, bt.Price, bt.Qty)

	// write to TimescaleDB
	_, err := db.TimescaleDB.Exec(`
        INSERT INTO binance_trade
            (event_type, event_time, symbol, trade_id, price, quantity, trade_ts, is_maker, ignore_flag)
        VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7), $8, $9)
    `,
		bt.E,           // event_type, e.g. "trade"
		bt.EvtTime,     // event_time
		bt.Symbol,      // symbol
		bt.TradeID,     // trade_id
		bt.Price,       // float64
		bt.Qty,         // float64
		tradeSec,       // sec
		bt.MarketMaker, // bool
		bt.Ignore,      // bool
	)
	if err != nil {
		log.Printf("❌ Can't save binance_trade to TimescaleDB: %v\n", err)
	} else {
		log.Printf("✅ Saved to TimescaleDB: symbol=%s, price=%.2f, qty=%.2f\n",
			bt.Symbol, bt.Price, bt.Qty)
	}
}

// parse message from Kafka
func handleMessage(msg []byte) {
	var bt models.BinanceTrade
	if err := json.Unmarshal(msg, &bt); err != nil {
		log.Printf("❌ JSON parse failed: %v\n", err)
		return
	}

	if bt.E != "trade" {
		log.Printf("❌ Not a trade event, skip: e=%s\n", bt.E)
		return
	}

	processBinanceTrade(bt)
}

// Kafka consumer
func ConsumeKafka() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_BROKER")},
		Topic:    os.Getenv("KAFKA_TOPIC"),
		GroupID:  "binance-consumer",
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("📡 Kafka Consumer started...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("❌ Loading Kafka message failed: %v. Retrying in 3s...\n", err)
			time.Sleep(3 * time.Second)
			continue
		}

		handleMessage(msg.Value)

		if err := reader.CommitMessages(context.Background(), msg); err != nil {
			log.Printf("❌ Kafka commit offset failed: %v\n", err)
		}
	}
}
