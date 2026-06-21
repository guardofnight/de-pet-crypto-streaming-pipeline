import json
import time
from kafka import KafkaProducer
from websocket import WebSocketApp

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "crypto_trades"

print("Подключение к Kafka...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
print("Успешно подключено к Kafka!")


def on_message(ws, message):
    data = json.loads(message)

    if "topic" in data and "data" in data:
        trades = data["data"]

        for trade in trades:
            payload = {
                "symbol": trade["s"],
                "price": float(trade["p"]),
                "side": trade["S"],
                "size": float(trade["v"]),
                "trade_time": int(trade["T"])
            }

            producer.send(TOPIC_NAME, value=payload)

            print(f"[{payload['symbol']}] {payload['side']} {payload['size']} @ {payload['price']}")


def on_error(ws, error):
    print("Ошибка вебсокета:", error)


def on_close(ws, close_status_code, close_msg):
    print("Соединение закрыто")


def on_open(ws):
    print("Соединение с Bybit установлено. Отправка подписки...")
    subscribe_message = {
        "op": "subscribe",
        "args": ["publicTrade.BTCUSDT", "publicTrade.ETHUSDT"]
    }
    ws.send(json.dumps(subscribe_message))


if __name__ == "__main__":
    bybit_url = "wss://stream.bybit.com/v5/public/linear"

    ws = WebSocketApp(
        bybit_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()