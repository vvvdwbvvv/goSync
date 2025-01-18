import json
import time
import random
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "market-data"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

SYMBOLS = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]


def generate_trade(symbol):
    return {
        "symbol": symbol,
        "type": "trade",
        "price": round(random.uniform(27000, 28000), 2),
        "volume": round(random.uniform(0.1, 5), 2),
        "timestamp": int(time.time() * 1000)
    }

def generate_orderbook_update(symbol):
    return {
        "symbol": symbol,
        "type": "orderbook_update",
        "bids": [[round(random.uniform(27000, 27500), 2), round(random.uniform(0.1, 1), 2)]],
        "asks": [[round(random.uniform(27500, 28000), 2), round(random.uniform(0.1, 1), 2)]],
        "timestamp": int(time.time() * 1000)
    }

def produce_mock_data():
    while True:
        symbol = random.choice(SYMBOLS)

        if random.random() < 0.5:
            data = generate_trade(symbol)
        else:
            data = generate_orderbook_update(symbol)

        producer.send(TOPIC, value=data, key=symbol.encode("utf-8"))
        print(f"Sent to Kafka: {data}")

        time.sleep(1)

if __name__ == "__main__":
    produce_mock_data()
