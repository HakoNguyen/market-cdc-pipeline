import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import json
import asyncio
import websockets
from kafka import KafkaProducer

import os

KAFKA_BOOTSTRAP_SERVERS = [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")]
KAFKA_TOPIC = "ws.kline_1m"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"

async def stream_binance_trade(max_messages=None):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    async with websockets.connect(BINANCE_WS_URL) as ws:
        print(f"Connected to {BINANCE_WS_URL}")
        count = 0
        while True:
            try: 
                msg = await ws.recv()
                data = json.loads(msg)
                kline = data.get('k', {})
                payload = {
                    'symbol': kline.get('s'),
                    'start_time': kline.get('t'),
                    'close_time': kline.get('T'),
                    'open': float(kline.get('o', 0)),
                    'high': float(kline.get('h', 0)),
                    'low': float(kline.get('l', 0)),
                    'close': float(kline.get('c', 0)),
                    'volume': float(kline.get('v', 0)),
                    'trades_count': float(kline.get('n', 0)),
                    'is_close': kline.get('x', False)
                }

                producer.send(KAFKA_TOPIC, value=payload)
                count += 1
                print(f"Sent trade #{count}: {payload}")

                if max_messages and count >= max_messages:
                    print(f"Successfully streamed {count} trade messages.")
                    break
            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(stream_binance_trade())
    except KeyboardInterrupt:
        print("Stream stopped by user")