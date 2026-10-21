import asyncio
import json
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Deque, List, Optional

import aio_pika
import httpx
import jwt
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

PORT = int(os.getenv("PORT", 8005))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", 8500))

JWT_SECRET = os.getenv("JWT_SECRET", "super-secret-jwt-key-minimum-32-chars-long")
JWT_ALGORITHM = "HS256"

SERVICE_NAME = "notification-service"
SERVICE_ID = "notification-service"
SERVICE_HOST = os.getenv("INSTANCE_HOST", "notification-service")
CONSUL_URL = f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service"

notifications_db: Deque[dict] = deque(maxlen=1000)
_next_id = 1

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
                print(f"[Consul] Ошибка регистрации notification (попытка {attempt}): {e}")
                await asyncio.sleep(2)


def send_notification(user_id: str, kind: str, order_id: int, text: str):
    global _next_id
    note = {
        "id": _next_id,
        "user_id": user_id,
        "type": kind,
        "order_id": order_id,
        "message": text,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _next_id += 1
    notifications_db.append(note)
    print(f"[Notify -> {user_id}] {text}")
    print("Отправлено уведомление клиенту")


def handle_event(routing_key: str, data: dict):
    order_id = int(data["order_id"])
    user_id = str(data["user_id"])

    if routing_key == "payment.success":
        send_notification(user_id, "PAYMENT_COMPLETED", order_id, f"Заказ #{order_id} успешно оплачен")
    elif routing_key == "order.cancelled":
        send_notification(
            user_id, "ORDER_CANCELLED", order_id,
            f"Заказ #{order_id} отменён: {data.get('reason')}",
        )


async def consume_events():
    queue = await rabbit_channel.declare_queue("notification_service_queue", durable=True)
    # Подписка на PaymentCompleted и OrderCancelled
    for key in ("payment.success", "order.cancelled"):
        await queue.bind(saga_exchange, routing_key=key)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process(requeue=False):
                try:
                    print(f"[Notify] Получено событие {message.routing_key}")
                    handle_event(message.routing_key, json.loads(message.body.decode()))
                except Exception as e:
                    print(f"[Notify] Ошибка: {e}")


async def run_consumer_forever():
    while True:
        try:
            await consume_events()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Notify] Потребитель упал: {e!r}. Перезапуск через 3с")
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
            print("[RabbitMQ] Notification подключен к брокеру")
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


app = FastAPI(title="Notification Service", lifespan=lifespan)


class Notification(BaseModel):
    id: int
    user_id: str
    type: str
    order_id: int
    message: str
    created_at: str


def current_user(authorization: Optional[str]) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Expected 'Authorization: Bearer <token>'")
    try:
        payload = jwt.decode(authorization.split(" ", 1)[1], JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    if not payload.get("sub"):
        raise HTTPException(status_code=401, detail="Token has no subject")
    return payload["sub"]


@app.get("/health")
def health():
    return {"status": "UP", "service": SERVICE_NAME}


@app.get("/me", response_model=List[Notification])
def my_notifications(authorization: Optional[str] = Header(None)):
    user_id = current_user(authorization)
    return [n for n in reversed(notifications_db) if n["user_id"] == user_id]