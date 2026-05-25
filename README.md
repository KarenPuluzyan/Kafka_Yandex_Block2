# KafkaBlock2 — Сервис обмена сообщениями с блокировкой и цензурой

Практическая работа 3, Задание 1.  
Язык: **Python** | Библиотека: **faust-streaming 0.11.3** | Кластер: **Kafka 7.5.0 + Zookeeper (3 брокера)**

---

## Структура проекта

```
~/KafkaBlock2/                        ← корень проекта и виртуальное окружение Python
├── bin/                              ← исполняемые файлы venv (python, pip, faust, ...)
├── lib/                              ← установленные пакеты venv
├── include/                          ← заголовки venv
├── pyvenv.cfg                        ← конфигурация виртуального окружения
│
├── app.py                            ← Faust-приложение (агенты, таблицы, HTTP API)
├── docker-compose.yml                ← Zookeeper + 3 брокера Kafka + Kafka UI
├── Dockerfile                        ← образ для faust-app контейнера
├── requirements.txt                  ← зависимости Python с версиями
│
└── tests/
    └── send_test_data.py             ← скрипт отправки тестовых данных
```

Виртуальное окружение создано прямо в каталоге проекта:
```bash
python3 -m venv ~/KafkaBlock2
cd ~/KafkaBlock2
source bin/activate
pip install faust-streaming==0.11.3 aiokafka==0.10.0 confluent-kafka==2.3.0
```

---

## Версии компонентов

| Компонент | Образ / Пакет | Версия |
|---|---|---|
| Zookeeper | `confluentinc/cp-zookeeper` | 7.5.0 |
| Kafka брокеры | `confluentinc/cp-kafka` | 7.5.0 |
| Kafka UI | `provectuslabs/kafka-ui` | v0.7.0 |
| faust-streaming | PyPI | 0.11.3 |
| aiokafka | PyPI | 0.10.0 |
| confluent-kafka | PyPI | 2.3.0 |

---

## Описание архитектуры

### Kafka-кластер

Кластер построен на базе **Zookeeper + три брокера Confluentinc**:

```
┌─────────────────────────────────────────────┐
│                  Zookeeper                  │
│          confluentinc/cp-zookeeper:7.5.0    │
│               порт 2181                     │
└──────────┬──────────────┬──────────────┬────┘
           │              │              │
    ┌──────▼──────┐ ┌─────▼──────┐ ┌────▼──────┐
    │   kafka-0   │ │  kafka-1   │ │  kafka-2  │
    │  broker_id=0│ │ broker_id=1│ │broker_id=2│
    │  внутр:9092 │ │ внутр:9092 │ │внутр:9092 │
    │  внешн:9094 │ │ внешн:9095 │ │внешн:9096 │
    └─────────────┘ └────────────┘ └───────────┘
```

Каждый брокер имеет два listener-а:
- `PLAINTEXT` — для внутреннего общения между контейнерами
- `EXTERNAL` — для подключения с хоста (localhost:9094/9095/9096)

Настройки:
- `KAFKA_DEFAULT_REPLICATION_FACTOR: 3` — каждый топик реплицируется на все три брокера
- `KAFKA_MIN_INSYNC_REPLICAS: 1` — запись подтверждается одной репликой (для разработки)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: false` — топики создаются только явно через kafka-init

### Порядок запуска (depends_on)

```
zookeeper (healthy)
    ↓
kafka-0, kafka-1, kafka-2 (healthy)
    ↓
kafka-init (service_completed_successfully) ← топики созданы
    ↓
faust-app, kafka-ui
```

### Топики Kafka

| Топик | Партиции | Replication | Назначение |
|---|---|---|---|
| `messages` | 3 | 3 | Входящие сообщения от пользователей |
| `filtered_messages` | 3 | 3 | Доставленные сообщения после фильтрации |
| `blocked_users` | 3 | 3 | События блокировки пользователей |
| `censored_commands` | 1 | 3 | Команды управления цензурой (add/remove) |

### Модели данных (faust.Record)

**Message** — входящее сообщение:
```json
{ "user_id": "alice", "recipient_id": "bob", "text": "Привет!" }
```

**BlockEvent** — событие блокировки:
```json
{ "user_id": "alice", "blocked_id": "charlie" }
```

**CensorCommand** — команда цензуры:
```json
{ "action": "add", "word": "спам" }
```

### Компоненты Faust

| Компонент | Реализация | Описание |
|---|---|---|
| **Agent** | `handle_censor_commands` | Читает команды из censored_commands, обновляет таблицу censored_words |
| **Agent** | `handle_block_events` | Stateful: обновляет таблицу blocked_list |
| **Agent** | `process_messages` | Фильтрация + цензура + публикация в filtered_messages |
| **Stream** | `stream.filter(...)` | Stateless-фильтрация заблокированных отправителей |
| **Processor** | `apply_censorship()` | Трансформация текста: замена запрещённых слов на `***` |
| **Sink** | `log_delivered()` | Логирование доставленных сообщений (вызывается после `yield`) |
| **Table** | `blocked_list` | 3 партиции, хранит заблокированных user_id для каждого получателя |
| **Table** | `censored_words` | 1 партиция, хранит глобальный список запрещённых слов |

### Согласование партиций топиков и таблиц

Faust требует чтобы число партиций changelog-топика таблицы совпадало
с числом партиций топика-источника агента, который в эту таблицу пишет:

| Топик-источник | Партиции | Таблица | Changelog партиции |
|---|---|---|---|
| `blocked_users` | 3 | `blocked_list` | 3 |
| `censored_commands` | 1 | `censored_words` | 1 |

### Логика управления цензурой

Faust запрещает изменять Table вне цикла `async for event in stream`.
Поэтому HTTP-эндпоинт отправляет команду в Kafka, агент применяет изменение:

```
GET /censored/add/спам
       │
       ▼
censored_commands (топик Kafka, 1 партиция)
       │
       ▼
handle_censor_commands (агент, внутри stream-цикла)
       │
       ▼
censored_words (Table, 1 партиция) ← изменение разрешено
```

### Логика фильтрации сообщений

```
Входящее сообщение (user_id → recipient_id)
        │
        ▼
 [stream.filter()]  — stateless
 user_id ∈ blocked_list[recipient_id]?
        │
    Да  │  Нет
        │   │
   Отброс   ▼
         [Processor: apply_censorship()]
         Замена запрещённых слов на ***
                 │
                 ▼
         filtered_messages (топик)
                 │
                 ▼
         [Sink: log_delivered()]
         Логирование доставки
```

---

## Установка и запуск

### Требования
- Ubuntu/Debian
- Docker + Docker Compose plugin: `sudo apt install docker-compose-plugin`
- Python 3.11+

### 1. Создайте виртуальное окружение и установите зависимости

```bash
python3 -m venv ~/KafkaBlock2
cd ~/KafkaBlock2
source bin/activate
pip install faust-streaming==0.11.3 aiokafka==0.10.0 confluent-kafka==2.3.0
```

### 2. Скопируйте файлы проекта в каталог

Поместите `app.py`, `docker-compose.yml`, `Dockerfile`, `requirements.txt`
и папку `tests/` в `~/KafkaBlock2/`.

### 3. Запустите кластер

```bash
cd ~/KafkaBlock2
docker compose up -d
```

Дождитесь готовности (~30–60 секунд):

```bash
docker compose ps
docker compose logs -f faust-app
```

Признак готовности в логах:
```
[^Worker]: Ready
```

### 4. Откройте Kafka UI

Перейдите в браузере: **http://localhost:8080**

---

## Тестирование

### Шаг 1. Активируйте виртуальное окружение

```bash
cd ~/KafkaBlock2
source bin/activate
```

### Шаг 2. Добавьте запрещённые слова

```bash
curl http://localhost:6066/censored/add/спам
curl http://localhost:6066/censored/add/реклама
sleep 3
curl http://localhost:6066/censored/list
# {"censored_words": ["спам", "реклама"]}
```

### Шаг 3. Запустите тестовый скрипт

```bash
python tests/send_test_data.py
```

### Шаг 4. Проверьте логи

```bash
docker compose logs -f faust-app
```

Ожидаемый вывод:
```
[BLOCK]      alice заблокировал charlie. Список: {'charlie'}
[BLOCK]      bob заблокировал dave. Список: {'dave'}
[CENSOR_ADD] Добавлено: 'спам'
[CENSOR_ADD] Добавлено: 'реклама'
[CENSOR]     'Это спам и реклама, игнорируй!' → 'Это *** и *** игнорируй!'
[OK]         bob → alice: 'Привет, Alice! Как дела?'
[OK]         eve → alice: 'Это *** и *** игнорируй!'
[OK]         alice → bob: 'Привет Bob, всё хорошо!'
[DELIVERED]  bob → alice: 'Привет, Alice! Как дела?'
[DELIVERED]  eve → alice: 'Это *** и *** игнорируй!'
[DELIVERED]  alice → bob: 'Привет Bob, всё хорошо!'
```

### Шаг 5. Проверить заблокированных пользователей

```bash
curl http://localhost:6066/blocked/list/alice
# {"user_id": "alice", "blocked": ["charlie"]}
```

### Шаг 6. Удалить слово из цензуры

```bash
curl http://localhost:6066/censored/remove/спам
sleep 3
curl http://localhost:6066/censored/list
```

### Шаг 7. Просмотр доставленных сообщений в Kafka UI

Перейдите: **http://localhost:8080** → Topics → `filtered_messages` → Messages

---

## Тестовые данные

### Блокировки (топик `blocked_users`)

```json
{"user_id": "alice", "blocked_id": "charlie"}
{"user_id": "bob",   "blocked_id": "dave"}
```

### Сообщения (топик `messages`)

```json
{"user_id": "bob",     "recipient_id": "alice", "text": "Привет, Alice! Как дела?"}
{"user_id": "charlie", "recipient_id": "alice", "text": "Это сообщение не должно дойти до Alice."}
{"user_id": "eve",     "recipient_id": "alice", "text": "Это спам и реклама, игнорируй!"}
{"user_id": "dave",    "recipient_id": "bob",   "text": "Привет Bob, это dave!"}
{"user_id": "alice",   "recipient_id": "bob",   "text": "Привет Bob, всё хорошо!"}
```

### Ожидаемые результаты

| Сообщение | Результат | Причина |
|---|---|---|
| bob → alice | ✅ Доставлено | Не заблокирован |
| charlie → alice | ❌ Отброшено | charlie в blocked_list[alice] |
| eve → alice | ✅ Доставлено с цензурой | «спам», «реклама» → `***` |
| dave → bob | ❌ Отброшено | dave в blocked_list[bob] |
| alice → bob | ✅ Доставлено | Не заблокирована |

---

## HTTP API

| Метод | URL | Описание |
|---|---|---|
| GET | `/censored/add/{word}` | Добавить запрещённое слово (асинхронно через Kafka) |
| GET | `/censored/remove/{word}` | Удалить запрещённое слово (асинхронно через Kafka) |
| GET | `/censored/list` | Список запрещённых слов |
| GET | `/blocked/list/{user_id}` | Список заблокированных пользователей |

Faust-приложение доступно на порту **6066**.

---

## Остановка

```bash
# Остановить контейнеры
docker compose down

# Остановить и удалить все данные (топики, changelog)
docker compose down -v
```
