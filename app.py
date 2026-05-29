"""
Сервис обмена сообщениями с блокировкой пользователей и цензурой.
"""

import os
import faust

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "kafka://localhost:9094,localhost:9095,localhost:9096",
)

app = faust.App(
    "kafkablock2",
    broker=KAFKA_BROKER,
    store="rocksdb://",
    value_serializer="json",
    topic_replication_factor=3,   # все внутренние топики Faust создаются с replication-factor=3
)

# ---------------------------------------------------------------------------
# Модели данных
# ---------------------------------------------------------------------------

class Message(faust.Record, serializer="json"):
    user_id: str
    recipient_id: str
    text: str


class BlockEvent(faust.Record, serializer="json"):
    user_id: str
    blocked_id: str


class CensorCommand(faust.Record, serializer="json"):
    action: str   # "add" или "remove"
    word: str


# ---------------------------------------------------------------------------
# Топики
#
# Важно: число партиций топика-источника и changelog-топика таблицы
# должны совпадать. Faust создаёт changelog автоматически с числом
# партиций равным app.conf.topic_partitions (по умолчанию = числу
# партиций топика-источника агента).
#
# blocked_users  → 3 партиции → blocked_list-changelog  → 3 партиции
# censored_commands → 1 партиция → censored_words-changelog → 1 партиция
# ---------------------------------------------------------------------------

messages_topic          = app.topic("messages",          value_type=Message,       partitions=3)
filtered_messages_topic = app.topic("filtered_messages", value_type=Message,       partitions=3)
blocked_users_topic     = app.topic("blocked_users",     value_type=BlockEvent,    partitions=3)
censored_commands_topic = app.topic("censored_commands", value_type=CensorCommand, partitions=1)

# ---------------------------------------------------------------------------
# Таблицы
#
# changelog партиций = партициям топика-источника агента:
# - blocked_list  читается из blocked_users (3 партиции)  → changelog: 3
# - censored_words читается из censored_commands (1 партиция) → changelog: 1
# ---------------------------------------------------------------------------

blocked_list: faust.Table = app.Table(
    "blocked_list",
    partitions=3,
    default=set,
    help="Список заблокированных отправителей для каждого получателя.",
)

censored_words: faust.Table = app.Table(
    "censored_words",
    partitions=1,
    default=set,
    help="Глобальный список запрещённых слов.",
)

# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _get_blocked(recipient_id: str) -> set:
    value = blocked_list[recipient_id]
    return set(value) if value else set()


def _get_censored() -> set:
    value = censored_words["global"]
    return set(value) if value else set()


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------

def apply_censorship(msg: Message) -> Message:
    words = _get_censored()
    if not words:
        return msg
    tokens = []
    for token in msg.text.split():
        clean_token = token.lower().strip(".,!?;:\"'")
        tokens.append("***" if clean_token in words else token)
    censored_text = " ".join(tokens)
    if censored_text != msg.text:
        print(f"[CENSOR] '{msg.text}' → '{censored_text}'")
    return Message(
        user_id=msg.user_id,
        recipient_id=msg.recipient_id,
        text=censored_text,
    )


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------

def log_delivered(msg: Message) -> None:
    print(f"[DELIVERED] {msg.user_id} → {msg.recipient_id}: '{msg.text}'")


# ---------------------------------------------------------------------------
# Агент 1: управление цензурой
# ---------------------------------------------------------------------------

@app.agent(censored_commands_topic)
async def handle_censor_commands(commands):
    async for cmd in commands:
        words = _get_censored()
        if cmd.action == "add":
            words.add(cmd.word.lower())
            print(f"[CENSOR_ADD] Добавлено: '{cmd.word}'")
        elif cmd.action == "remove":
            words.discard(cmd.word.lower())
            print(f"[CENSOR_REMOVE] Удалено: '{cmd.word}'")
        censored_words["global"] = words
        print(f"[CENSOR_LIST] Текущий список: {words}")


# ---------------------------------------------------------------------------
# Агент 2: блокировка пользователей
# ---------------------------------------------------------------------------

@app.agent(blocked_users_topic)
async def handle_block_events(events):
    async for event in events:
        current = _get_blocked(event.user_id)
        current.add(event.blocked_id)
        blocked_list[event.user_id] = current
        print(f"[BLOCK] {event.user_id} заблокировал {event.blocked_id}. Список: {current}")


# ---------------------------------------------------------------------------
# Агент 3: фильтрация сообщений
# ---------------------------------------------------------------------------

@app.agent(messages_topic, sink=[log_delivered])
async def process_messages(stream):
    # group_by(recipient_id) гарантирует что все сообщения одного получателя
    # попадают на одну партицию — ту же, где хранится его blocked_list.
    # Без этого сообщения без ключа распределяются по случайным партициям
    # и фильтрация молча не применяется.
    async for msg in stream.group_by(Message.recipient_id).filter(
        lambda m: m.user_id not in _get_blocked(m.recipient_id)
    ):
        clean_msg = apply_censorship(msg)
        await filtered_messages_topic.send(value=clean_msg)
        print(f"[OK] {clean_msg.user_id} → {clean_msg.recipient_id}: '{clean_msg.text}'")
        yield clean_msg


# ---------------------------------------------------------------------------
# HTTP-эндпоинты
# ---------------------------------------------------------------------------

@app.page("/censored/add/{word}")
async def add_censored_word(web, request, word: str):
    await censored_commands_topic.send(
        value=CensorCommand(action="add", word=word)
    )
    return web.json({"status": "queued", "action": "add", "word": word})


@app.page("/censored/remove/{word}")
async def remove_censored_word(web, request, word: str):
    await censored_commands_topic.send(
        value=CensorCommand(action="remove", word=word)
    )
    return web.json({"status": "queued", "action": "remove", "word": word})


@app.page("/censored/list")
async def list_censored_words(web, request):
    return web.json({"censored_words": list(_get_censored())})


@app.page("/blocked/list/{user_id}")
async def list_blocked(web, request, user_id: str):
    return web.json(
        {"user_id": user_id, "blocked": list(_get_blocked(user_id))}
    )


if __name__ == "__main__":
    app.main()
