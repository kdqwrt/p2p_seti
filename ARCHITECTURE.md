# Архитектура защищённой оверлейной P2P-сети

Документ описывает архитектуру программного узла, границы компонентов,
потоки данных и основные инженерные решения проекта
**«Защищённая оверлейная сеть передачи данных на основе пиринговых протоколов»**.

**Уровень реализации:** продвинутый  
**Язык:** Python  
**Транспорт:** TCP  
**Асинхронная модель:** `asyncio`  
**Текущий этап:** Этап 1 — транспорт и базовый RPC

---

## Содержание

- [Назначение](#назначение)
- [Основные архитектурные принципы](#основные-архитектурные-принципы)
- [Общая архитектура](#общая-архитектура)
- [Структура компонентов](#структура-компонентов)
- [Node](#node)
- [Transport](#transport)
- [Protocol](#protocol)
- [RPC](#rpc)
- [Входящий поток данных](#входящий-поток-данных)
- [Исходящий RPC](#исходящий-rpc)
- [Модель TCP-соединений](#модель-tcp-соединений)
- [StreamDecoder](#streamdecoder)
- [Contact](#contact)
- [Конфигурация](#конфигурация)
- [Обработка ошибок](#обработка-ошибок)
- [Логирование](#логирование)
- [Тестируемость](#тестируемость)

---

## Назначение

Архитектура Этапа 1 описывает реализованную транспортно-протокольную основу P2P-узла. На первом этапе решаются четыре основные задачи:

```text
TCP transport
      │
      ▼
Binary framing
      │
      ▼
Payload serialization
      │
      ▼
RPC processing
```

---

## Основные архитектурные принципы

В проекте используются следующие принципы:

1. **Разделение ответственности.**  
   TCP-транспорт, бинарный протокол, RPC и логика узла реализуются
   отдельными компонентами.

2. **Независимость от границ TCP-read.**  
   Один вызов `read()` не считается одним сообщением.

3. **Явное кадрирование.**  
   Каждое сетевое сообщение содержит собственный 24-байтовый заголовок.

4. **Ограничение входных данных.**  
   Размер payload проверяется до дальнейшей обработки сообщения.

5. **Корреляция RPC.**  
   Запрос и ответ связываются через случайный `request_id`.

6. **Отказоустойчивость узла.**  
   Ошибка отдельного соединения не должна завершать весь P2P-узел.

7. **Конфигурируемость.**  
   Сетевые параметры и тайм-ауты изменяются без правки исходного кода.

---

## Общая архитектура

Текущая архитектура состоит из четырёх основных уровней:

```text
┌───────────────────────────────────────────────┐
│                    NODE                       │
│                                               │
│ lifecycle / bootstrap / orchestration         │
├───────────────────────────────────────────────┤
│                     RPC                       │
│                                               │
│ Dispatcher / PING / PONG / ERROR              │
├───────────────────────────────────────────────┤
│                  PROTOCOL                     │
│                                               │
│ Frame / Messages / Contact / MessagePack      │
├───────────────────────────────────────────────┤
│                 TRANSPORT                     │
│                                               │
│ TCPServer / Connection / StreamDecoder        │
└───────────────────────────────────────────────┘
                        │
                        ▼
                       TCP
```

Каждый уровень работает с собственным представлением данных.

```text
Transport       bytes
                   │
                   ▼
Protocol         Frame
                   │
                   ▼
RPC             Message
                   │
                   ▼
Node        application logic
```

---

## Структура компонентов

Основные программные компоненты расположены следующим образом:

```text
src/p2p/
│
├── config.py
│
├── node/
│   └── node.py
│
├── protocol/
│   ├── codec.py
│   ├── constants.py
│   ├── contact.py
│   ├── frame.py
│   ├── messages.py
│   └── payload_codec.py
│
├── rpc/
│   └── dispatcher.py
│
├── transport/
│   ├── connection.py
│   ├── server.py
│   └── stream_decoder.py
│

```

Связи между компонентами:

```text
                  ┌────────────┐
                  │    Node    │
                  └─────┬──────┘
                        │
             ┌──────────┴──────────┐
             ▼                     ▼
       ┌────────────┐        ┌────────────┐
       │ TCP Server │        │ Connection │
       └──────┬─────┘        └─────┬──────┘
              │                    │
              ▼                    │
       ┌──────────────┐            │
       │StreamDecoder │            │
       └──────┬───────┘            │
              │                    │
              ▼                    │
          ┌───────┐                │
          │ Frame │◄───────────────┘
          └───┬───┘
              │
              ▼
       ┌────────────┐
       │ Dispatcher │
       └──────┬─────┘
              │
              ▼
         RPC handlers
```

---

## Node

`Node` является верхним координирующим компонентом.

Он отвечает за:

- запуск узла;
- запуск TCP-сервера;
- остановку;
- работу с конфигурацией;
- bootstrap;
- инициирование RPC;
- взаимодействие с транспортным и RPC-слоем.

`Node` не должен самостоятельно разбирать байты TCP или вручную
декодировать бинарный заголовок.

Общая схема:

```text
Node.start()
    │
    ├── create local state
    │
    ├── start TCPServer
    │
    └── process bootstrap peers
```

При наличии bootstrap peer:

```text
Node
 │
 ├── connect(peer)
 │
 ├── create PING
 │
 ├── send request
 │
 ├── receive PONG
 │
 ├── validate request_id
 │
 └── calculate RTT
```

---

## Transport

Транспортный слой отвечает только за передачу байтов.

Основные компоненты:

```text
transport/
├── connection.py
├── server.py
└── stream_decoder.py
```

### TCPServer

`TCPServer`:

- принимает входящие соединения;
- читает данные;
- передаёт их `StreamDecoder`;
- получает готовые Frame;
- передаёт Frame в `Dispatcher`;
- отправляет ответ;
- корректно закрывает соединение.

```text
Remote peer
     │
     ▼
 TCP socket
     │
     ▼
 TCPServer
     │
     ▼
StreamDecoder
```

### Connection

Исходящее соединение используется для выполнения RPC.

```text
Node
 │
 ▼
Connection
 │
 ├── connect
 ├── send Frame
 ├── wait response
 └── close
```

Для соединений применяются тайм-ауты, задаваемые конфигурацией.

---

## Protocol

Протокольный слой определяет формат сетевого обмена.

Он отвечает за:

- структуру Frame;
- кодирование Frame;
- декодирование Frame;
- типы сообщений;
- MessagePack payload;
- структуру `Contact`;
- проверку полей сообщений.

```text
Python message
      │
      ▼
MessagePack
      │
      ▼
Frame.payload
      │
      ▼
Frame
      │
      ▼
binary encoding
      │
      ▼
TCP
```

При получении выполняется обратный процесс:

```text
TCP
 │
 ▼
bytes
 │
 ▼
Frame
 │
 ▼
MessagePack
 │
 ▼
validation
 │
 ▼
message
```

Точная спецификация находится в `PROTOCOL.md`.

---

## RPC

RPC-слой находится между протоколом и логикой узла.

Основным компонентом является:

```text
Dispatcher
```

Его задача — определить обработчик по `message_type`.

```text
                    Frame
                      │
                      ▼
                 Dispatcher
                      │
        ┌─────────────┼──────────────┐
        │
        ▼
      PING
        │
        ▼
   PING handler
        │
        ▼
      PONG
```

На этапе 1 полностью используется обработка:

```text
PING → PONG
```

Также поддерживается формирование `ERROR`.

---

## Входящий поток данных

Полный путь входящего запроса:

```text
Удалённый узел
      │
      ▼
TCP connection
      │
      ▼
TCPServer
      │
      ▼
reader.read()
      │
      ▼
StreamDecoder.feed()
      │
      ▼
Frame
      │
      ▼
Dispatcher.dispatch()
      │
      ▼
decode payload
      │
      ▼
validate message
      │
      ▼
RPC handler
      │
      ▼
Response Frame
      │
      ▼
encode_frame()
      │
      ▼
writer.write()
      │
      ▼
Удалённый узел
```

Таким образом, RPC-handler не работает непосредственно с TCP-потоком.

---

## Исходящий RPC

При исходящем `PING` последовательность обратная:

```text
Node
 │
 ├── generate request_id
 │
 ├── create PING payload
 │
 ├── MessagePack encode
 │
 ├── create Frame
 │
 ▼
Connection
 │
 ├── TCP connect
 │
 ├── send Frame
 │
 ├── receive response
 │
 ▼
Frame
 │
 ├── check type
 │
 ├── check request_id
 │
 ├── decode PONG
 │
 └── validate timestamp
 │
 ▼
RPC success
```

---

## Модель TCP-соединений

На текущем этапе используется простая модель:

```text
1 RPC = 1 TCP connection
```

Последовательность:

```text
connect
   │
   ▼
send request
   │
   ▼
receive response
   │
   ▼
close
```

Преимущества текущего решения:

- простое управление состоянием;
- отсутствие сложной маршрутизации ответов внутри одного соединения;
- простая обработка timeout;
- простое освобождение ресурсов;
- удобство тестирования первого этапа.

Ограничение:

- для каждого RPC создаётся новое TCP-соединение.

Это осознанное решение Этапа 1, упрощающее корреляцию RPC, обработку timeout и освобождение ресурсов.

---

## StreamDecoder

`StreamDecoder` необходим из-за потоковой природы TCP.

Нельзя предполагать:

```text
writer.write(frame) == reader.read(frame)
```

Один Frame может быть разделён на несколько TCP-чтений:

```text
FRAME

┌────────────── HEADER ─────────────┬──────── PAYLOAD ────────┐
└───────────────────────────────────┴──────────────────────────┘

                     ↓

read #1        read #2            read #3
┌───────┐    ┌──────────────┐   ┌───────────────────────┐
│header │    │header+payload│   │ remaining payload     │
│ part  │    │     part     │   │                       │
└───────┘    └──────────────┘   └───────────────────────┘
```

Также несколько Frame могут прийти одним чтением:

```text
read()

┌──── Frame 1 ────┬──── Frame 2 ────┬── Frame 3 part ──┐
```

Поэтому алгоритм декодера выглядит следующим образом:

```text
new bytes
   │
   ▼
append to internal buffer
   │
   ▼
enough for header?
   │
   ├── no ──► wait
   │
   ▼ yes
decode header
   │
   ▼
validate payload_length
   │
   ▼
full payload available?
   │
   ├── no ──► wait
   │
   ▼ yes
create Frame
   │
   ▼
remove consumed bytes
   │
   └──► try next Frame
```

Проверка `payload_length` выполняется после получения заголовка и до
ожидания полного payload.

---

## Contact

`Contact` является сетевым описанием узла.

Текущая структура:

```text
Contact
│
├── node_id
├── identity_algorithm
├── identity_public_key
├── host
└── port
```

На этапе 1 криптографическая identity ещё не создана.

Поэтому используется временное представление:

```text
node_id             = empty
identity_algorithm  = "stage1-none"
identity_public_key = empty
host                = local host
port                = local port
```

Это временное архитектурное решение.


---

## Конфигурация

Настройки вынесены из исходного кода.

Пример разделения:

```text
             Config
                │
       ┌────────┴────────┐
       │                 │
       ▼                 ▼
   Transport          Protocol
```

Используемые параметры:

| Параметр | Назначение |
|---|---|
| `listen_host` | локальный адрес |
| `listen_port` | TCP-порт |
| `bootstrap_peers` | начальные узлы |
| `node_state_dir` | состояние узла |
| `node_id_bits` | размер NodeID |
| `k_bucket_size` | размер k-bucket |
| `alpha` | параллелизм lookup |
| `connect_timeout_ms` | подключение |
| `read_timeout_ms` | чтение |
| `ping_timeout_ms` | PING |
| `max_frame_payload` | ограничение payload |
| `protocol_version` | версия протокола |
| `log_level` | уровень логирования |
| `log_file` | файл журнала |

Для локальной демонстрации используются:

```text
configs/node1.json
configs/node2.json
```

---

## Обработка ошибок

Ошибки удалённого узла должны быть локализованы внутри соответствующего
соединения.

```text
                   Connection
                       │
                       ▼
                Decode / Validate
                       │
              ┌────────┴────────┐
              │                 │
              ▼                 ▼
             OK               ERROR
              │                 │
              ▼                 ▼
         Dispatcher       close connection
                                │
                                ▼
                        TCPServer continues
```

К протокольным ошибкам относятся:

- неизвестная версия;
- неизвестный тип;
- превышение допустимого payload;
- повреждённый payload;
- несоответствие схеме;
- неправильное направление сообщения;
- разрыв внутри Frame;
- нарушение корреляции RPC.

Если Frame уже можно безопасно идентифицировать, сервер может вернуть
сообщение `ERROR` перед закрытием соединения.

Ошибки не должны приводить к завершению процесса всего узла.

---

## Логирование

Логи используются не только для диагностики, но и для подтверждения
корректной работы на защите.

Для каждого узла используется отдельный файл:

```text
logs/
├── node1.log
└── node2.log
```

Журналируются:

- запуск и остановка узла;
- запуск TCP-сервера;
- входящее соединение;
- исходящее соединение;
- направление сообщения;
- тип сообщения;
- `request_id`;
- размер payload;
- timeout;
- протокольная ошибка;
- закрытие соединения.

Закрытые ключи и другие секретные значения журналироваться не должны.


---

## Тестируемость

Архитектура разделена таким образом, чтобы отдельные компоненты можно
было проверять независимо.

```text
Frame
  │
  └── unit tests

StreamDecoder
  │
  └── fragmentation tests

Payload
  │
  └── validation tests

Dispatcher
  │
  └── RPC tests

TCPServer + Connection
  │
  └── integration tests

Malformed input
  │
  └── negative tests
```

Критически важный сценарий:

```text
100 Frames
     │
     ▼
random fragmentation
     │
     ▼
StreamDecoder
     │
     ▼
100 Frames
```

Он подтверждает отсутствие зависимости от границ TCP-чтения.

