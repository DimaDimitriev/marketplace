import asyncio
import os
import socket
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional

import bcrypt
import httpx
import jwt
from fastapi import FastAPI, HTTPException, Header, status
from pydantic import BaseModel, Field, field_validator

PORT = int(os.getenv("PORT", 8001))
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", 8500))

JWT_SECRET = os.getenv("JWT_SECRET", "super-secret-jwt-key-minimum-32-chars-long")
JWT_ALGORITHM = "HS256"

HOSTNAME = socket.gethostname()
SERVICE_NAME = "auth-service"
SERVICE_HOST = os.getenv("INSTANCE_HOST", HOSTNAME)
SERVICE_ID = f"auth-service-{SERVICE_HOST}"
CONSUL_URL = f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service"

users_db: dict[str, str] = {}


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
                print(f"[Consul] Ошибка регистрации auth (попытка {attempt}): {e}")
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


app = FastAPI(title="Auth Service", lifespan=lifespan)


class UserAuth(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6, max_length=128)

    @field_validator("password")
    @classmethod
    def password_fits_bcrypt(cls, v: str) -> str:
        if len(v.encode("utf-8")) > 72:
            raise ValueError("Password must be at most 72 bytes in UTF-8")
        return v


@app.get("/health")
def health():
    return {"status": "UP", "service": SERVICE_NAME}


@app.post("/register", status_code=status.HTTP_201_CREATED)
def register(user: UserAuth):
    if user.username in users_db:
        raise HTTPException(status_code=400, detail="User already exists")
    hashed_password = bcrypt.hashpw(user.password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    users_db[user.username] = hashed_password
    return {"msg": "User registered successfully"}


@app.post("/login")
def login(user: UserAuth):
    db_pass = users_db.get(user.username)
    if not db_pass or not bcrypt.checkpw(user.password.encode("utf-8"), db_pass.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    exp = datetime.now(timezone.utc) + timedelta(hours=2)
    token = jwt.encode({"sub": user.username, "exp": exp}, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return {"access_token": token, "token_type": "bearer"}


@app.get("/me")
def get_me(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid token. Expected 'Bearer <token>'",
        )
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return {
            "username": payload.get("sub"),
            "status": "authenticated",
            "expires_at": payload.get("exp"),
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")