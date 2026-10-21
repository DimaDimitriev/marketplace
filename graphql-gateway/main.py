import asyncio
import os
import random
from contextlib import asynccontextmanager
from typing import List, Optional

import httpx
import jwt
import strawberry
from fastapi import FastAPI, Request
from graphql import GraphQLError
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

PORT = int(os.getenv("PORT", 8000))
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", 8500))

JWT_SECRET = os.getenv("JWT_SECRET", "super-secret-jwt-key-minimum-32-chars-long")
JWT_ALGORITHM = "HS256"

SERVICE_NAME = "graphql-gateway"
SERVICE_ID = "graphql-gateway"
SERVICE_HOST = os.getenv("INSTANCE_HOST", "graphql-gateway")
CONSUL_AGENT_URL = f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service"
CONSUL_HEALTH_URL = f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/health/service"


async def get_service_url(service_name: str, fallback_url: str) -> str:
    async with httpx.AsyncClient(timeout=2.0) as client:
        try:
            res = await client.get(f"{CONSUL_HEALTH_URL}/{service_name}?passing=true")
            if res.status_code == 200:
                services = res.json()
                if services:
                    selected = random.choice(services)["Service"]
                    addr = selected["Address"] or service_name
                    port = selected["Port"]
                    return f"http://{addr}:{port}"
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
                resp = await client.put(f"{CONSUL_AGENT_URL}/register", json=payload)
                resp.raise_for_status()
                print(f"[Consul] Зарегистрирован {SERVICE_ID}")
                return
            except Exception as e:
                print(f"[Consul] Ошибка регистрации gateway (попытка {attempt}): {e}")
                await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await register_in_consul()
    yield
    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            await client.put(f"{CONSUL_AGENT_URL}/deregister/{SERVICE_ID}")
        except Exception as e:
            print(f"[Consul] Ошибка дерегистрации: {e}")


async def get_context(request: Request):
    return {"request": request}


def get_current_user(info: Info) -> str:
    request: Request = info.context["request"]
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        raise GraphQLError("Unauthorized: expected 'Authorization: Bearer <token>'")
    token = auth.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise GraphQLError("Unauthorized: token expired")
    except jwt.PyJWTError:
        raise GraphQLError("Unauthorized: invalid token")
    sub = payload.get("sub")
    if not sub:
        raise GraphQLError("Unauthorized: token has no subject")
    return sub


@strawberry.type
class Product:
    id: int
    name: str
    price: float
    description: Optional[str] = None


@strawberry.type
class OrderItem:
    product_id: int
    quantity: int

    @strawberry.field
    async def product(self) -> Optional[Product]:
        catalog_url = await get_service_url("catalog-service", "http://catalog-1:8002")
        async with httpx.AsyncClient(timeout=3.0) as client:
            try:
                resp = await client.get(f"{catalog_url}/products/{self.product_id}")
                if resp.status_code == 200:
                    item = resp.json().get("item")
                    return Product(
                        id=item["id"],
                        name=item["name"],
                        price=item["price"],
                        description=item.get("description"),
                    )
            except httpx.HTTPError:
                return None
            return None


@strawberry.type
class Order:
    id: int
    user_id: str
    amount: float
    status: str
    items: List[OrderItem]


def to_order(o: dict) -> Order:
    return Order(
        id=o["id"],
        user_id=o["user_id"],
        amount=o["amount"],
        status=o["status"],
        items=[
            OrderItem(product_id=i["product_id"], quantity=i["quantity"])
            for i in o["items"]
        ],
    )


@strawberry.input
class OrderItemInput:
    product_id: int
    quantity: int


@strawberry.type
class Query:
    @strawberry.field
    async def products(self) -> List[Product]:
        catalog_url = await get_service_url("catalog-service", "http://catalog-1:8002")
        async with httpx.AsyncClient(timeout=3.0) as client:
            try:
                resp = await client.get(f"{catalog_url}/products")
                if resp.status_code == 200:
                    items = resp.json().get("items", [])
                    return [Product(**p) for p in items]
            except httpx.HTTPError as e:
                raise GraphQLError(f"Catalog service unavailable: {e}")
            return []

    @strawberry.field
    async def user_orders(self, info: Info) -> List[Order]:
        user_id = get_current_user(info)
        order_url = await get_service_url("order-service", "http://order-service:8003")
        async with httpx.AsyncClient(timeout=3.0) as client:
            try:
                resp = await client.get(f"{order_url}/orders", params={"user_id": user_id})
                if resp.status_code == 200:
                    return [to_order(o) for o in resp.json()]
            except httpx.HTTPError as e:
                raise GraphQLError(f"Order service unavailable: {e}")
            return []


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def create_order(self, info: Info, items: List[OrderItemInput]) -> Order:
        user_id = get_current_user(info)
        order_url = await get_service_url("order-service", "http://order-service:8003")
        payload = {
            "user_id": user_id,
            "items": [{"product_id": i.product_id, "quantity": i.quantity} for i in items],
        }

        async with httpx.AsyncClient(timeout=5.0) as client:
            try:
                resp = await client.post(f"{order_url}/orders", json=payload)
            except httpx.HTTPError as e:
                raise GraphQLError(f"Order service unavailable: {e}")

            if resp.status_code == 201:
                return to_order(resp.json())

            try:
                detail = resp.json().get("detail", resp.text)
            except Exception:
                detail = resp.text
            raise GraphQLError(f"Failed to create order: {detail}")


schema = strawberry.Schema(query=Query, mutation=Mutation)
graphql_app = GraphQLRouter(schema, context_getter=get_context)

app = FastAPI(title="GraphQL Gateway", lifespan=lifespan)
app.include_router(graphql_app, prefix="/graphql")


@app.get("/health")
def health():
    return {"status": "UP", "service": SERVICE_NAME}