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

// handling message from Kafka
func handleMessage(message []byte) {
	var raw map[string]interface{}
	err := json.Unmarshal(message, &raw)
	if err != nil {
		log.Printf("❌ JSON parse failed: %v\n", err)
		return
	}

	messageType, ok := raw["type"].(string)
	if !ok {
		log.Println("❌ Cannot parse message type")
		return
	}

	switch messageType {
	case "trade":
		var trade models.Trade
		err = json.Unmarshal(message, &trade)
		if err != nil {
			log.Printf("❌ Trade data parse failed: %v\n", err)
			return
		}
		processTrade(trade)

	case "orderbook_update":
		var orderBook models.OrderBookUpdate
		err = json.Unmarshal(message, &orderBook)
		if err != nil {
			log.Printf("❌ Orderbook parse failed: %v\n", err)
			return
		}
		processOrderBookUpdate(orderBook)
	}
}

// handling trade
func processTrade(trade models.Trade) {
	ctx := context.Background()
	// save to Redis
	db.RedisClient.HSet(ctx, fmt.Sprintf("market:%s", trade.Symbol), "price", trade.Price, "volume", trade.Volume)
	log.Printf("✅ Saved to Redis: %s - Price: %.2f, Vol: %.2f\n", trade.Symbol, trade.Price, trade.Volume)

	// save to TimescaleDB
	_, err := db.TimescaleDB.Exec("INSERT INTO trade (symbol, price, volume, timestamp) VALUES ($1, $2, $3, to_timestamp($4))",
		trade.Symbol, trade.Price, trade.Volume, float64(trade.Timestamp)/1000.0)

	if err != nil {
		log.Printf("❌ Can't save trade to TimescaleDB: %v\n", err)
	} else {
		log.Printf("✅ Saved to TimescaleDB: %s - Price: %.2f, Vol: %.2f\n", trade.Symbol, trade.Price, trade.Volume)
	}
}

// handling orderbook update
func processOrderBookUpdate(orderBook models.OrderBookUpdate) {
	ctx := context.Background()

	// transform to JSON
	bidsJSON, _ := json.Marshal(orderBook.Bids)
	asksJSON, _ := json.Marshal(orderBook.Asks)

	// save to Redis
	db.RedisClient.HSet(ctx, fmt.Sprintf("orderbook:%s", orderBook.Symbol),
		"bids", string(bidsJSON), "asks", string(asksJSON))
	log.Printf("✅ Saved to Redis: %s orderbook update", orderBook.Symbol)

	// save to TimescaleDB
	_, err := db.TimescaleDB.Exec("INSERT INTO order_book (symbol, bids, asks, timestamp) VALUES ($1, $2, $3, to_timestamp($4))",
		orderBook.Symbol, bidsJSON, asksJSON, float64(orderBook.Timestamp)/1000.0)

	if err != nil {
		log.Printf("❌ Can't save orderbook to TimescaleDB: %v\n", err)
	} else {
		log.Printf("✅ Saved to TimescaleDB: %s orderbook update", orderBook.Symbol)
	}
}

// kafka consumer
func ConsumeKafka() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_BROKER")},
		Topic:    os.Getenv("KAFKA_TOPIC"),
		GroupID:  "market-consumer",
		MaxBytes: 10e6,
	})

	defer reader.Close()

	log.Println("📡 Kafka Consumer started...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("❌ Loading Kafka message failed: %v. Retrying in 3 seconds...\n", err)
			time.Sleep(3 * time.Second)
			continue
		}

		handleMessage(msg.Value)

		// commit offset
		err = reader.CommitMessages(context.Background(), msg)
		if err != nil {
			log.Printf("❌ Kafka commit offset failed: %v\n", err)
		}
	}
}
