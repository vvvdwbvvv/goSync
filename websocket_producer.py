import asyncio
import websockets
import json
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"

class WebSocketUtils:
    def __init__(self, uri):
        self.uri = uri
        self.connection = None

    async def connect(self):
        if self.connection is None:
            try:
                self.connection = await websockets.connect(self.uri)
                print(f"Connected to {self.uri}")
            except Exception as e:
                print(f"Failed to connect to {self.uri}: {e}")
                exit(1)
        return self.connection

    async def close_connection(self):
        if self.connection is not None:
            await self.connection.close()
            self.connection = None
            print("WebSocket connection closed")

class PublicStream(WebSocketUtils):
    def __init__(self, base_uri, symbols):
        self.symbols = symbols
        self.topics = ["trade", "depth10@100ms"]  # Subscription messages
        streams = "/".join([f"{symbol}@{topic}" for symbol in symbols for topic in self.topics])
        uri = f"{base_uri}{streams}"
        super().__init__(uri)

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    async def listen(self):
        websocket = await self.connect()

        while True:
            try:
                message = await websocket.recv()
                payload = json.loads(message)
                # print(payload)

                '''
                    Sample payload for a trade message:
                    {
                        "stream": "btcusdt@trade",
                        "data": {
                            "e": "trade",
                            "E": 1626070800000,
                            "s": "BTCUSDT",
                            "t": 12345,
                            "p": "30000.00",
                            "q": "0.001",
                            "b": 123456,
                            "a": 654321,
                            "T": 1626070800001,
                            "m": true,
                            "M": true
                        }
                    }

                    Sample payload for a orderbook message:
                    {
                        "stream": "btcusdt@depth10@100ms",
                        "data": {
                            "lastUpdateId": 1234,
                            "bids": [
                                ["30000.00", "0.001"],
                                ["29999.00", "0.002"],
                                ["29998.00", "0.003"],
                                ["29997.00", "0.004"],
                                ["29996.00", "0.005"],
                                ["29995.00", "0.006"],
                                ["29994.00", "0.007"],
                                ["29993.00", "0.008"],
                                ["29992.00", "0.009"],
                                ["29991.00", "0.010"]
                            ],
                            "asks": [
                                ["30000.00", "0.001"],
                                ["30001.00", "0.002"],
                                ["30002.00", "0.003"],
                                ["30003.00", "0.004"],
                                ["30004.00", "0.005"],
                                ["30005.00", "0.006"],
                                ["30006.00", "0.007"],
                                ["30007.00", "0.008"],
                                ["30008.00", "0.009"],
                                ["30009.00", "0.010"]
                            ]
                        }
                    }
                '''

                # Extract relevant data from the payload
                stream = payload.get("stream")
                symbol = stream.split("@")[0]
                topic = stream.split("@")[1]
                data = payload.get("data")
                if data:
                    # Send data to Kafka
                    self.producer.send(topic, value=data)

            except websockets.ConnectionClosed as e:
                print(f"Connection closed: {e}")
                print("Reconnecting...")
                await asyncio.sleep(5)  # Wait before reconnecting
                await self.listen()
                break
            except Exception as e:
                print(f"Error receiving data: {e}")
                break

def main():
    base_uri = "wss://stream.binance.com:9443/stream?streams="
    symbols = ["btcusdt", "ethusdt"]
    public_stream = PublicStream(base_uri, symbols)

    try:
        asyncio.run(public_stream.listen())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
    finally:
        asyncio.run(public_stream.close_connection())

if __name__ == "__main__":
    main()
