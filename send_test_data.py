"""
Скрипт для отправки тестовых данных в топики Kafka.
Запускайте после старта docker-compose.

Требования:
    pip install confluent-kafka

Использование:
    python tests/send_test_data.py
"""

import json
import time
from confluent_kafka import Producer

# Подключаемся к любому из трёх брокеров кластера
KAFKA_BROKER = "localhost:9094,localhost:9095,localhost:9096"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Ошибка доставки: {err}")
    else:
        print(f"[SENT]  topic={msg.topic()} | {msg.value().decode()}")


def send(topic: str, value: dict):
    producer.produce(
        topic,
        value=json.dumps(value, ensure_ascii=False).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)


# ---------------------------------------------------------------------------
# Шаг 1. Блокировка пользователей
# ---------------------------------------------------------------------------
print("\n=== Шаг 1: Блокируем пользователей ===")

send("blocked_users", {"user_id": "alice", "blocked_id": "charlie"})
send("blocked_users", {"user_id": "bob",   "blocked_id": "dave"})

producer.flush()
print("Ждём обработки агентом handle_block_events...")
time.sleep(3)

# ---------------------------------------------------------------------------
# Шаг 2. Добавление запрещённых слов через HTTP-эндпоинт
# ---------------------------------------------------------------------------
print("\n=== Шаг 2: Добавьте запрещённые слова через HTTP ===")
print("  curl -X POST http://localhost:6066/censored/add/спам")
print("  curl -X POST http://localhost:6066/censored/add/реклама")
print("  curl http://localhost:6066/censored/list")
time.sleep(1)

# ---------------------------------------------------------------------------
# Шаг 3. Тестовые сообщения
# ---------------------------------------------------------------------------
print("\n=== Шаг 3: Отправляем тестовые сообщения ===")

test_messages = [
    # Обычное сообщение — должно пройти
    {
        "user_id": "bob",
        "recipient_id": "alice",
        "text": "Привет, Alice! Как дела?",
    },
    # От заблокированного charlie → alice — должно быть отброшено
    {
        "user_id": "charlie",
        "recipient_id": "alice",
        "text": "Это сообщение не должно дойти до Alice.",
    },
    # Содержит запрещённые слова — слова заменяются на ***
    {
        "user_id": "eve",
        "recipient_id": "alice",
        "text": "Это спам и реклама, игнорируй!",
    },
    # От заблокированного dave → bob — должно быть отброшено
    {
        "user_id": "dave",
        "recipient_id": "bob",
        "text": "Привет Bob, это dave!",
    },
    # Обычное сообщение — должно пройти
    {
        "user_id": "alice",
        "recipient_id": "bob",
        "text": "Привет Bob, всё хорошо!",
    },
]

for msg in test_messages:
    send("messages", msg)

producer.flush()

print("\n=== Тестовые данные отправлены ===")
print("Логи приложения: docker compose logs -f faust-app")
print("Kafka UI:        http://localhost:8080")
print("Faust API:       http://localhost:6066/censored/list")
