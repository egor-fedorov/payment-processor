# Payment Processor

Асинхронный сервис процессинга платежей на `FastAPI + PostgreSQL + RabbitMQ`.

## Что реализовано

- `POST /api/v1/payments` c `Idempotency-Key`
- `GET /api/v1/payments/{payment_id}`
- статический API key через заголовок `X-API-Key`
- outbox pattern для гарантированной публикации событий
- асинхронный consumer на `FastStream`
- эмуляция платежного шлюза: задержка 2-5 секунд, успех 90%, ошибка 10%
- retry доставки webhook до 3 попыток с экспоненциальной задержкой `1s -> 2s`
- DLQ `payments.dlq` для окончательно упавших сообщений
- Alembic миграции
- Docker Compose окружение

## Архитектура

1. API создаёт запись в `payments` и событие в `outbox` в одной транзакции.
2. Outbox dispatcher в процессе API читает неопубликованные записи и публикует сообщения в очередь `payments.new`.
3. Consumer читает событие, выполняет эмуляцию платежного шлюза и обновляет статус платежа.
4. После обработки consumer отправляет webhook.
5. Если webhook не доставлен, сообщение публикуется в retry-очередь `payments.retry` с TTL.
6. После истечения TTL RabbitMQ возвращает сообщение обратно в `payments.new`.
7. После 3 неудачных попыток сообщение отправляется в `payments.dlq`.

## Запуск

```bash
cp .env.example .env
make up
```

Остановить стек:

```bash
make down
```

Полезные команды:

```bash
make help
make logs
make ps
make migrate
make clean
```

Сервисы после запуска:

- API: `http://localhost:8000`
- PostgreSQL: `localhost:5433`
- Swagger UI: `http://localhost:8000/docs`
- RabbitMQ UI: `http://localhost:15672` (`guest` / `guest`)

## Примеры запросов

Создание платежа:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: super-secret-key" \
  -H "Idempotency-Key: order-1001" \
  -d '{
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Оплата заказа #1001",
    "metadata": {
      "order_id": "1001",
      "customer_id": "cust-1"
    },
    "webhook_url": "https://httpbin.org/post"
  }'
```

Получение статуса платежа:

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: super-secret-key"
```

## Переменные окружения

- `API_KEY` - ключ для всех HTTP эндпоинтов
- `DATABASE_URL` - async SQLAlchemy DSN
- `RABBITMQ_URL` - DSN для RabbitMQ
- `OUTBOX_POLL_INTERVAL_SECONDS` - интервал опроса outbox
- `OUTBOX_BATCH_SIZE` - размер пачки публикации
- `PAYMENT_SUCCESS_RATE` - вероятность успешного платежа
- `PAYMENT_DELAY_MIN_SECONDS` / `PAYMENT_DELAY_MAX_SECONDS` - диапазон задержки шлюза
- `RETRY_MAX_ATTEMPTS` - максимум попыток доставки webhook
- `RETRY_BASE_DELAY_SECONDS` - базовая задержка для backoff
- `WEBHOOK_TIMEOUT_SECONDS` - timeout HTTP webhook

## Замечания

- Идемпотентность реализована по `Idempotency-Key` на уровне БД.
- Публикация из outbox даёт семантику at-least-once, поэтому consumer сделан повторно вызываемым.
- В compose добавлен отдельный сервис `migrate`, чтобы API и consumer стартовали только после применения схемы.
