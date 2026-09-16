import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import json
import asyncio
import websockets
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9094"]
KAFKA_TOPIC = "ws.raw_trades"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

async def stream_binance_trade():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    async with websockets.connect(BINANCE_WS_URL) as ws:
        print(f"Connected to {BINANCE_WS_URL}")
        while True:
            try: 
                msg = await ws.recv()
                trade_data = json.loads(msg)
                payload = {
                    'trade_id': trade_data.get('t'),
                    'symbol': trade_data.get('s'),
                    'price': float(trade_data.get('p', 0)),
                    'qty': float(trade_data.get('q', 0)),
                    'tradeTime': trade_data.get('T'),
                    'is_buyer_maker': trade_data.get('m')
                }

                producer.send(KAFKA_TOPIC, value=payload)
                print(f"Sent trade: {payload}")
            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(stream_binance_trade())
    except KeyboardInterrupt:
        print("Stream stopped by user")