import asyncio
import json
import os
import random
from contextlib import asynccontextmanager
from typing import List, Optional

import aio_pika
import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

PORT = int(os.getenv("PORT", 8003))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", 8500))

SERVICE_NAME = "order-service"
SERVICE_ID = "order-service"
SERVICE_HOST = os.getenv("INSTANCE_HOST", "order-service")
CONSUL_URL = f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service"
CONSUL_HEALTH_URL = f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/health/service"

orders_db: dict[int, dict] = {
    1: {
        "id": 1,
        "user_id": "Alice",
        "amount": 1225.0,
        "status": "CONFIRMED",
        "items": [{"product_id": 1, "quantity": 1}, {"product_id": 2, "quantity": 1}],
    }
}

rabbit_connection = None
rabbit_channel = None
saga_exchange = None


async def get_service_url(service_name: str, fallback_url: str) -> str:
    async with httpx.AsyncClient(timeout=2.0) as client:
        try:
            res = await client.get(f"{CONSUL_HEALTH_URL}/{service_name}?passing=true")
            if res.status_code == 200:
                services = res.json()
                if services:
                    selected = random.choice(services)["Service"]
                    addr = selected["Address"] or service_name
                    return f"http://{addr}:{selected['Port']}"
        except Exception as e:
            print(f"[Consul Discovery Error] {e}")
    return fallback_url


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
                print(f"[Consul] Ошибка регистрации order (попытка {attempt}): {e}")
                await asyncio.sleep(2)


async def consume_payment_events():
    queue = await rabbit_channel.declare_queue("order_service_queue", durable=True)
    await queue.bind(saga_exchange, routing_key="payment.success")
    await queue.bind(saga_exchange, routing_key="payment.failed")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process(requeue=False):
                try:
                    data = json.loads(message.body.decode())
                    order_id = data.get("order_id")
                    if order_id not in orders_db:
                        continue

                    if message.routing_key == "payment.success":
                        orders_db[order_id]["status"] = "CONFIRMED"
                        print(f"[Saga] Заказ #{order_id} подтвержден (CONFIRMED)")
                    elif message.routing_key == "payment.failed":
                        # Компенсирующая транзакция: заказ не должен остаться в PENDING
                        order = orders_db[order_id]
                        order["status"] = "CANCELLED"
                        print(f"[Saga: Откат] Оплата заказа #{order_id} отклонена. Статус -> CANCELLED")

                        cancelled_event = {
                            "order_id": order_id,
                            "user_id": order["user_id"],
                            "amount": order["amount"],
                            "reason": data.get("reason"),
                        }
                        await saga_exchange.publish(
                            aio_pika.Message(
                                body=json.dumps(cancelled_event).encode(),
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key="order.cancelled",
                        )
                        print(f"[Saga] Заказ #{order_id}: опубликовано событие order.cancelled")
                except Exception as e:
                    print(f"[Saga] Ошибка обработки сообщения: {e}")


async def run_consumer_forever():
    while True:
        try:
            await consume_payment_events()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Saga] Потребитель Order упал: {e!r}. Перезапуск через 3с")
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
            print("[RabbitMQ] Успешно подключен к брокеру")
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


app = FastAPI(title="Order Service", lifespan=lifespan)


class OrderItem(BaseModel):
    product_id: int
    quantity: int = Field(..., gt=0)


class OrderCreate(BaseModel):
    user_id: str
    items: List[OrderItem] = Field(..., min_length=1)


class Order(BaseModel):
    id: int
    user_id: str
    amount: float
    status: str
    items: List[OrderItem]


async def calculate_amount(items: List[OrderItem]) -> float:
    catalog_url = await get_service_url("catalog-service", "http://catalog-1:8002")
    total = 0.0
    async with httpx.AsyncClient(timeout=3.0) as client:
        for item in items:
            try:
                resp = await client.get(f"{catalog_url}/products/{item.product_id}")
            except httpx.HTTPError as e:
                raise HTTPException(status_code=503, detail=f"Catalog unavailable: {e}")

            if resp.status_code == 404:
                raise HTTPException(status_code=400, detail=f"Product {item.product_id} not found")
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Catalog error")

            price = resp.json()["item"]["price"]
            total += price * item.quantity
    return round(total, 2)


@app.get("/health")
def health():
    return {"status": "UP", "service": SERVICE_NAME}


@app.get("/orders", response_model=List[Order])
def get_orders(user_id: Optional[str] = None):
    if user_id:
        return [order for order in orders_db.values() if order["user_id"] == user_id]
    return list(orders_db.values())


@app.get("/orders/{order_id}", response_model=Order)
def get_order(order_id: int):
    order = orders_db.get(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@app.post("/orders", status_code=status.HTTP_201_CREATED, response_model=Order)
async def create_order(order_in: OrderCreate):
    if saga_exchange is None:
        raise HTTPException(status_code=503, detail="Message broker not ready")

    amount = await calculate_amount(order_in.items)
    new_id = max(orders_db.keys(), default=0) + 1
    new_order = {
        "id": new_id,
        "user_id": order_in.user_id,
        "amount": amount,
        "status": "PENDING",
        "items": [item.model_dump() for item in order_in.items],
    }
    orders_db[new_id] = new_order

    event_payload = {
        "order_id": new_id,
        "user_id": order_in.user_id,
        "amount": amount,
    }
    await saga_exchange.publish(
        aio_pika.Message(
            body=json.dumps(event_payload).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key="order.created",
    )
    print(f"[Saga] Заказ #{new_id} создан со статусом PENDING. Отправлено событие order.created")
    return new_order