import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from app.api import (
    stores, generator, predictions, lotto, pension, auth
)
from app.core.database import get_pool, close_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작 시 DB 풀을 미리 생성하고, 종료 시 정리한다."""
    await get_pool()
    yield
    await close_pool()


app = FastAPI(
    title="복권지도 API",
    description="동행복권 기반 복권 판매점 지도 서비스",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        elapsed = (time.perf_counter() - start) * 1000
        logger.exception(f"{request.method} {request.url.path} -> 500 ({elapsed:.1f}ms)")
        raise
    elapsed = (time.perf_counter() - start) * 1000
    logger.info(f"{request.method} {request.url.path} -> {response.status_code} ({elapsed:.1f}ms)")
    return response


app.include_router(stores.router)
app.include_router(generator.router)
app.include_router(predictions.router)
app.include_router(lotto.router)
app.include_router(pension.router)
app.include_router(auth.router)
