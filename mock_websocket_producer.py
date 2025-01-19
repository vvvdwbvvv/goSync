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

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def generate_binance_trade(symbol):
    """
    參考官方文件:
    {
      "e": "trade",     // 事件種類
      "E": 123456789,   // 事件時間 (ms)
      "s": "BTCUSDT",   // 交易對
      "t": 12345,       // 成交ID
      "p": "0.001",     // 成交價 (字串)
      "q": "100",       // 成交量 (字串)
      "T": 123456785,   // 成交時間 (ms)
      "m": true,        // 是否為 Maker
      "M": true         // 忽略欄位
    }
    """
    return {
        "e": "trade",
        "E": int(time.time() * 1000),
        "s": symbol,
        "t": random.randint(10000, 99999),
        "p": f"{round(random.uniform(27000, 28000), 2)}",
        "q": f"{round(random.uniform(0.01, 5), 2)}",
        "T": int(time.time() * 1000),
        "m": random.choice([True, False]),
        "M": random.choice([True, False])
    }


def produce_mock_data():
    while True:
        symbol = random.choice(SYMBOLS)
        data = generate_binance_trade(symbol)

        producer.send(TOPIC, value=data, key=symbol.encode("utf-8"))
        print(f"Sent to Kafka: {data}")

        time.sleep(0.1)


if __name__ == "__main__":
    produce_mock_data()
