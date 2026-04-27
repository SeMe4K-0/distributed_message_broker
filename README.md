# Distributed Message Broker

Kafka-подобный брокер сообщений, написанный с нуля на чистом Elixir/OTP.  
Продюсеры пишут сообщения в топики, консьюмеры читают их по оффсетам.  
Никаких внешних баз данных — только BEAM, `:gen_tcp` и файловая система.

## Архитектура

```
Broker.Application (one_for_one)
├── Registry (unique)                   — именование партиций
├── Topic.TopicSupervisor               — DynamicSupervisor топиков
│     └── Topic.PartitionSupervisor     — one_for_one на партицию
│           └── Topic.Partition         — GenServer, сериализует записи
├── Network.Listener                    — TCP accept loop
└── ConnectionSupervisor                — DynamicSupervisor соединений
      └── Network.Connection            — GenServer на клиента
```

## Текущий статус: Фаза 2

### Что реализовано

#### Фаза 1 — TCP-брокер, WAL, бинарный протокол

**Бинарный протокол** (`lib/broker/protocol/`)

Собственный wire-формат поверх TCP. Каждый фрейм:

```
+--[1] magic (0x42) --+--[1] ver (0x01) --+--[1] type --+--[1] flags --+--[4] payload_len --+
+--[N] payload --+
```

Поддерживаемые типы сообщений:

| Код    | Тип             | Направление      |
|--------|-----------------|------------------|
| `0x01` | PRODUCE         | client → broker  |
| `0x02` | PRODUCE_ACK     | broker → client  |
| `0x03` | FETCH           | client → broker  |
| `0x04` | FETCH_RESPONSE  | broker → client  |
| `0xFF` | ERROR           | broker → client  |

Парсинг через Elixir binary pattern matching — без сторонних библиотек.

**WAL** (`lib/broker/log/wal.ex`) — базовый формат записей на диск:

```
entry_len::32 | offset::64 | timestamp::64 | crc32::32 | key_len::32 | key | value_len::32 | value
```

CRC32 считается над `offset + timestamp + key + value`. Используется сегментами как формат `.log`-файлов.

**TCP сервер** (`lib/broker/network/`)

- `Listener` — GenServer, accept loop через `send(self(), :accept)`
- `Connection` — GenServer на каждого клиента, буферизует байты, диспетчеризует фреймы

---

#### Фаза 2 — Сегментный лог и индекс

**Сегментный лог** (`lib/broker/log/`)

WAL разбит на файлы фиксированного размера. Каждый сегмент — это пара файлов:

```
data/{topic}/{partition}/
├── 00000000000000000000.log    — данные
├── 00000000000000000000.index  — индекс
├── 00000000000000000036.log
├── 00000000000000000036.index
└── ...
```

Имена файлов — 20-значный base offset первой записи в сегменте.

**Индексный файл** (`.index`) — фиксированные 16-байтовые записи:

```
relative_offset::64 | file_position::64
```

`relative_offset = offset - base_offset`. Файл отсортирован, поиск за O(log n) — бинарный поиск по массиву.

**Lookup по оффсету:**
1. Найти сегмент с наибольшим `base_offset <= target`
2. Бинарный поиск в `.index` → `file_position`
3. `:file.pread(fd, file_position, 4)` → `entry_len`
4. `:file.pread(fd, file_position + 4, entry_len)` → запись

**SegmentManager** (`lib/broker/log/segment_manager.ex`)

GenServer, управляющий списком сегментов партиции:
- Держит список `sealed` (завершённых) сегментов и один `active`
- При превышении `max_bytes` автоматически ротирует active → sealed, открывает новый
- При старте восстанавливает все сегменты с диска
- Зарегистрирован в `Registry` с ключом `{:mgr, topic, partition}`

**Compactor** (`lib/broker/log/compactor.ex`)

GenServer, удаляющий `.log` и `.index` файлы старше `retention_ms` (default: 7 дней). Активный сегмент никогда не удаляется.

---

### Файловая структура

```
lib/broker/
├── application.ex
├── protocol/
│   ├── frame.ex                — константы типов и кодов ошибок
│   └── codec.ex                — encode/decode фреймов и payload
├── log/
│   ├── wal.ex                  — чистые функции: encode/decode/append
│   ├── segment.ex              — GenServer: один .log + .index файл
│   ├── segment_index.ex        — чистые функции: binary search
│   ├── segment_manager.ex      — GenServer: список сегментов партиции
│   └── compactor.ex            — GenServer: удаление старых сегментов
├── network/
│   ├── listener.ex             — TCP accept loop
│   └── connection.ex           — обработка одного TCP-клиента
└── topic/
    ├── partition.ex            — GenServer: делегирует в SegmentManager
    ├── partition_supervisor.ex — запускает SegmentManager + Compactor + Partition
    └── topic_supervisor.ex     — DynamicSupervisor
```

### Тесты

```
test/
├── broker/
│   ├── protocol/codec_test.exs       — round-trip encode/decode, граничные случаи
│   ├── log/wal_test.exs              — CRC, partial read, recovery
│   ├── log/segment_index_test.exs    — binary search: exact, floor, not_found, 100 entries
│   ├── log/segment_test.exs          — append, read_at, full?, disk recovery
│   └── topic/partition_test.exs      — offsets, fetch, перезапуск
├── integration/
│   ├── produce_consume_test.exs      — end-to-end через TCP
│   └── segment_rotation_test.exs     — ротация, cross-segment fetch, файлы на диске
└── support/
    └── tcp_client.ex
```

```
mix test   # 44 теста, 0 ошибок
```

## Запуск

```bash
mix deps.get
mix run --no-halt
```

Брокер слушает на порту `9092` по умолчанию. Порт и папка данных настраиваются:

```elixir
# config/config.exs
config :broker, port: 9092
config :broker, data_dir: "data"
```

## Пример — produce и fetch через TCP

```elixir
# Подключение
{:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", 9092, [:binary, packet: :raw, active: false])

# PRODUCE: записать "hello" с ключом "k1" в топик "events", партиция 0
topic = "events"
produce_payload =
  <<byte_size(topic)::16, topic::binary,
    0::32,          # partition
    1::64,          # correlation_id
    1::32,          # num_records
    byte_size("k1")::32, "k1"::binary,
    byte_size("hello")::32, "hello"::binary,
    0::32>>         # headers_count

frame = <<0x42, 0x01, 0x01, 0x00, byte_size(produce_payload)::32, produce_payload::binary>>
:gen_tcp.send(socket, frame)

# Получаем PRODUCE_ACK
{:ok, ack} = :gen_tcp.recv(socket, 0, 5000)
# <<0x42, 0x01, 0x02, 0x00, _len::32, _cid::64, base_offset::64, 0>> = ack

# FETCH: читать с оффсета 0
fetch_payload =
  <<byte_size(topic)::16, topic::binary,
    0::32,          # partition
    0::64,          # fetch_offset
    1_048_576::32,  # max_bytes
    2::64>>         # correlation_id

frame2 = <<0x42, 0x01, 0x03, 0x00, byte_size(fetch_payload)::32, fetch_payload::binary>>
:gen_tcp.send(socket, frame2)

{:ok, response} = :gen_tcp.recv(socket, 0, 5000)
```

## Roadmap

| Фаза | Описание | Статус |
|------|----------|--------|
| 1 | Одиночный брокер — TCP, WAL, produce/fetch | ✅ Готово |
| 2 | Сегментный лог + индекс, O(log n) lookup | ✅ Готово |
| 3 | GenStage — backpressure, demand-driven flow | ⬜ |
| 4 | Кластер — consistent hashing, партиции по нодам | ⬜ |
| 5 | Raft — leader election, log replication | ⬜ |
| 6 | Consumer groups — координатор, rebalance | ⬜ |
| 7 | Observability — Telemetry, MetricsLib, Benchee | ⬜ |
