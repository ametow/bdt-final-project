"""
Binance WebSocket -> Kafka producer.

Subscribes to public trade streams for selected symbols and forwards each
trade event as a JSON message into a Kafka topic.
"""

import asyncio
import json
import os
import signal
import sys

import websockets
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "trades")
SYMBOLS = [s.strip().lower() for s in os.environ.get(
    "SYMBOLS", "btcusdt,ethusdt,solusdt,bnbusdt"
).split(",") if s.strip()]

BINANCE_HOST = os.environ.get("BINANCE_HOST", "stream.binance.us:9443")
BINANCE_URL = (
    f"wss://{BINANCE_HOST}/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 50,
        "compression.type": "lz4",
    })


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed: {err}", file=sys.stderr)


async def stream_trades(producer: Producer):
    backoff = 1
    while True:
        try:
            print(f"[producer] connecting to {BINANCE_URL}")
            async with websockets.connect(BINANCE_URL, ping_interval=20) as ws:
                backoff = 1
                async for raw in ws:
                    msg = json.loads(raw)
                    data = msg.get("data") or msg
                    if data.get("e") != "trade":
                        continue
                    event = {
                        "symbol": data["s"],
                        "price": float(data["p"]),
                        "qty": float(data["q"]),
                        "trade_id": int(data["t"]),
                        "trade_time": int(data["T"]),  # ms epoch
                    }
                    producer.produce(
                        KAFKA_TOPIC,
                        key=event["symbol"],
                        value=json.dumps(event),
                        on_delivery=delivery_report,
                    )
                    producer.poll(0)
        except Exception as exc:
            print(f"[producer] WS error: {exc}; reconnecting in {backoff}s", file=sys.stderr)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


def main():
    producer = make_producer()
    loop = asyncio.new_event_loop()

    def shutdown(*_):
        print("[producer] shutting down")
        producer.flush(5)
        loop.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        loop.run_until_complete(stream_trades(producer))
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
