import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List

import aio_pika
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

PORT = int(os.getenv("PORT", 8004))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", 8500))

PAYMENT_LIMIT = float(os.getenv("PAYMENT_LIMIT", 1500))
SERVICE_NAME = "payment-service"
SERVICE_ID = "payment-service"
SERVICE_HOST = os.getenv("INSTANCE_HOST", "payment-service")
CONSUL_URL = f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service"

payments_db: dict[int, dict] = {}
rabbit_connection = None
rabbit_channel = None
saga_exchange = None


async def register_in_consul():
    payload = {
        "ID": SERVICE_ID,
        "Name": SERVICE_NAME,
        "Address": SERVICE_HOST,
        "Port": PORT,
        "Check": {
            "HTTP": f"http://{SERVICE_HOST}:{PORT}/health",
            "Interval": "10s",
            "Timeout": "2s",
            "DeregisterCriticalServiceAfter": "30s",
        },
    }
    async with httpx.AsyncClient(timeout=3.0) as client:
        for attempt in range(1, 31):
            try:
                resp = await client.put(f"{CONSUL_URL}/register", json=payload)
                resp.raise_for_status()
                print(f"[Consul] Зарегистрирован {SERVICE_ID}")
                return
            except Exception as e:
                print(f"[Consul] Ошибка регистрации payment (попытка {attempt}): {e}")
                await asyncio.sleep(2)


def process_payment(order_id: int, user_id: str, amount: float) -> dict:
    if amount > PAYMENT_LIMIT:
        status_, reason = "FAILED", f"Лимит средств превышен (лимит: {PAYMENT_LIMIT})"
    else:
        status_, reason = "SUCCESS", None
    return {
        "order_id": order_id,
        "user_id": user_id,
        "amount": amount,
        "status": status_,
        "reason": reason,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


async def handle_order_created(message: aio_pika.IncomingMessage):
    try:
        data = json.loads(message.body.decode())
        order_id = int(data["order_id"])
        user_id = str(data["user_id"])
        amount = float(data["amount"])
    except (ValueError, KeyError, TypeError):
        return

    if order_id in payments_db:
        return

    payment = process_payment(order_id, user_id, amount)
    routing_key = "payment.success" if payment["status"] == "SUCCESS" else "payment.failed"
    event = {
        "order_id": order_id,
        "user_id": user_id,
        "amount": amount,
        "reason": payment["reason"],
    }
    await saga_exchange.publish(
        aio_pika.Message(
            body=json.dumps(event).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key,
    )
    payments_db[order_id] = payment
    print(f"[Saga: Payment] Заказ #{order_id}: {payment['status']} -> {routing_key}")


async def consume_order_events():
    queue = await rabbit_channel.declare_queue("payment_service_queue", durable=True)
    await queue.bind(saga_exchange, routing_key="order.created")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                async with message.process(requeue=True):
                    await handle_order_created(message)
            except Exception as e:
                print(f"[Payment] Ошибка обработки, requeue: {e}")
                await asyncio.sleep(1)


async def run_consumer_forever():
    while True:
        try:
            await consume_order_events()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Payment] Потребитель упал: {e!r}. Перезапуск...")
            await asyncio.sleep(3)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global rabbit_connection, rabbit_channel, saga_exchange

    while True:
        try:
            rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
            rabbit_channel = await rabbit_connection.channel()
            saga_exchange = await rabbit_channel.declare_exchange(
                "saga_exchange", aio_pika.ExchangeType.TOPIC, durable=True
            )
            print("[RabbitMQ] Payment подключен к брокеру")
            break
        except Exception:
            await asyncio.sleep(2)

    consumer_task = asyncio.create_task(run_consumer_forever())
    await register_in_consul()

    yield

    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    if rabbit_connection:
        await rabbit_connection.close()

    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            await client.put(f"{CONSUL_URL}/deregister/{SERVICE_ID}")
        except Exception:
            pass


app = FastAPI(title="Payment Service", lifespan=lifespan)


class Payment(BaseModel):
    order_id: int
    user_id: str
    amount: float
    status: str
    reason: str | None = None
    processed_at: str


@app.get("/health")
def health():
    return {"status": "UP", "service": SERVICE_NAME}


@app.get("/payments", response_model=List[Payment])
def list_payments():
    return list(payments_db.values())


@app.get("/payments/{order_id}", response_model=Payment)
def get_payment(order_id: int):
    payment = payments_db.get(order_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    return payment