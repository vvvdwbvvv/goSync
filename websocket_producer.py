import asyncio
import websockets
import json
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "market-data"

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
        self.topics = ["trade"]  # Subscription messages
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

                # Extract relevant data from the payload
                data = payload.get("data")
                if data:
                    print(f"Received data: {data}")

                    # Send data to Kafka
                    self.producer.send(KAFKA_TOPIC, value=data)

            except websockets.ConnectionClosed as e:
                print(f"Connection closed: {e}")
                print("Reconnecting...")
                await asyncio.sleep(5)  # Wait before reconnecting
                await self.listen()
                break
            except Exception as e:
                print(f"Error receiving data: {e}")
                break

    async def respond_pong(self, websocket):
        pong_message = {"event": "pong", "ts": int(asyncio.get_event_loop().time() * 1000)}
        await websocket.send(json.dumps(pong_message))


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
