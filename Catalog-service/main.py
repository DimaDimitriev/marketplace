import asyncio
import os
import socket
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

PORT = int(os.getenv("PORT", 8002))
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", 8500))

INSTANCE_ID = os.getenv("INSTANCE_ID", socket.gethostname())
SERVICE_NAME = "catalog-service"
SERVICE_ID = f"catalog-service-{INSTANCE_ID}"
CONSUL_URL = f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service"

products_db: dict[int, dict] = {
    1: {"id": 1, "name": "Laptop", "price": 1200.0, "description": "High-end laptop"},
    2: {"id": 2, "name": "Mouse", "price": 25.0, "description": "Wireless mouse"},
}


async def register_in_consul():
    payload = {
        "ID": SERVICE_ID,
        "Name": SERVICE_NAME,
        "Address": INSTANCE_ID,
        "Port": PORT,
        "Check": {
            "HTTP": f"http://{INSTANCE_ID}:{PORT}/health",
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
                print(f"[Consul] Ошибка регистрации catalog (попытка {attempt}): {e}")
                await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await register_in_consul()
    yield
    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            await client.put(f"{CONSUL_URL}/deregister/{SERVICE_ID}")
        except Exception as e:
            print(f"[Consul] Ошибка дерегистрации: {e}")


app = FastAPI(title="Catalog Service", lifespan=lifespan)


class ProductCreate(BaseModel):
    name: str = Field(..., min_length=1)
    price: float = Field(..., gt=0)
    description: Optional[str] = None


class ProductUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1)
    price: Optional[float] = Field(None, gt=0)
    description: Optional[str] = None


@app.get("/health")
def health():
    return {"status": "UP", "instance_id": INSTANCE_ID}


@app.get("/products")
def get_products():
    return {
        "instance_id": INSTANCE_ID,
        "total": len(products_db),
        "items": list(products_db.values()),
    }


@app.get("/products/{product_id}")
def get_product(product_id: int):
    product = products_db.get(product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"instance_id": INSTANCE_ID, "item": product}


@app.post("/products", status_code=status.HTTP_201_CREATED)
def create_product(product: ProductCreate):
    new_id = max(products_db.keys(), default=0) + 1
    new_item = {"id": new_id, **product.model_dump()}
    products_db[new_id] = new_item
    return {"instance_id": INSTANCE_ID, "item": new_item}


@app.put("/products/{product_id}")
def update_product(product_id: int, product: ProductUpdate):
    if product_id not in products_db:
        raise HTTPException(status_code=404, detail="Product not found")
    item = products_db[product_id]
    if product.name is not None:
        item["name"] = product.name
    if product.price is not None:
        item["price"] = product.price
    if product.description is not None:
        item["description"] = product.description
    return {"instance_id": INSTANCE_ID, "item": item}


@app.delete("/products/{product_id}")
def delete_product(product_id: int):
    if product_id not in products_db:
        raise HTTPException(status_code=404, detail="Product not found")
    deleted = products_db.pop(product_id)
    return {"instance_id": INSTANCE_ID, "deleted": deleted}