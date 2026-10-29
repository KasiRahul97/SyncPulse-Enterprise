"""SyncPulse backend (FastAPI) — API contract identical to the Spring Boot version."""
from __future__ import annotations

import logging
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis as redis_lib
from cassandra.cluster import NoHostAvailable
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse

from .cdc import CdcEventPublisher, KafkaEventProducer
from .config import config
from .debounce import DebounceService
from .domain import BenchmarkRequest, DeviceRegistrationRequest, UpdateRequest
from .idempotency import IdempotencyService
from .metrics import LiveEventLog, MetricsService
from .services import (AdminService, BenchmarkService, DemoData, DeviceService,
                       PlayheadService, SystemStatus, ValidationError, make_seed_fn)
from .store import CassandraStore

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("syncpulse")

svc: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = CassandraStore()
    store.connect()
    store.init_schema()

    r = redis_lib.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, socket_timeout=3)
    metrics = MetricsService()
    live = LiveEventLog()
    kafka = KafkaEventProducer(store, metrics)
    cdc = CdcEventPublisher(kafka)
    debounce = DebounceService(r)
    idem = IdempotencyService(r)
    playhead = PlayheadService(store, debounce, idem, metrics, live, cdc)
    devices = DeviceService(store, live)
    seed = make_seed_fn(devices)
    admin = AdminService(store, r, metrics, live, seed)
    status = SystemStatus(store, r)
    benchmark = BenchmarkService(playhead)

    svc.update(dict(store=store, redis=r, metrics=metrics, live=live, cdc=cdc,
                    playhead=playhead, devices=devices, seed=seed, admin=admin,
                    status=status, benchmark=benchmark))

    seed()

    def rate_loop():
        while True:
            time.sleep(1)
            try:
                metrics.sample_rates()
            except Exception:  # noqa: BLE001
                pass
    threading.Thread(target=rate_loop, daemon=True).start()

    log.info("SyncPulse backend (FastAPI) ready.")
    yield


app = FastAPI(title="SyncPulse Enterprise API", version="1.0.0", lifespan=lifespan,
              description="Event-driven NoSQL state backend for cross-device playhead sync.")

app.add_middleware(
    CORSMiddleware, allow_origins=config.CORS_ORIGINS,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"], allow_headers=["*"])


@app.middleware("http")
async def request_id_mw(request: Request, call_next):
    rid = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    request.state.request_id = rid
    response = await call_next(request)
    response.headers["X-Request-Id"] = rid
    return response


def _err(request: Request, status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={
        "timestamp": datetime.now(timezone.utc).isoformat(), "status": status,
        "error": code, "message": message,
        "requestId": getattr(request.state, "request_id", None),
    })


@app.exception_handler(ValidationError)
async def _validation(request: Request, exc: ValidationError):
    return _err(request, 400, "VALIDATION_ERROR", str(exc))


@app.exception_handler(RequestValidationError)
async def _req_validation(request: Request, exc: RequestValidationError):
    msg = "; ".join(f"{'.'.join(str(p) for p in e['loc'][1:])}: {e['msg']}" for e in exc.errors())
    return _err(request, 400, "VALIDATION_ERROR", msg or "validation failed")


@app.exception_handler(NoHostAvailable)
async def _cassandra_down(request: Request, exc: NoHostAvailable):
    log.error("Cassandra unavailable: %s", exc)
    return _err(request, 503, "CASSANDRA_UNAVAILABLE",
                "The durable store is currently unavailable. Please retry.")


@app.exception_handler(redis_lib.exceptions.ConnectionError)
async def _redis_down(request: Request, exc):
    log.error("Redis unavailable: %s", exc)
    return _err(request, 503, "REDIS_UNAVAILABLE",
                "The debounce layer is currently unavailable. Please retry.")


@app.exception_handler(Exception)
async def _generic(request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        raise exc
    log.exception("Unhandled error")
    return _err(request, 500, "INTERNAL_ERROR", str(exc) or "Unexpected error")


# ------------------------- routes -------------------------

@app.post("/api/v1/playhead/update")
def update(req: UpdateRequest):
    return svc["playhead"].update(req)


@app.get("/api/v1/playhead/{user_id}/{content_id}")
def get_state(user_id: str, content_id: str):
    st = svc["playhead"].get_state(user_id, content_id)
    if st is None:
        raise HTTPException(status_code=404, detail="no state")
    return {
        "userId": st["userId"], "contentId": st["contentId"], "deviceId": st["deviceId"],
        "positionSeconds": st["positionSeconds"], "vectorClock": st["vectorClock"],
        "status": st["status"], "revision": st["revision"], "lastRequestId": st["lastRequestId"],
        "lastUpdatedAt": _iso(st["lastUpdatedAt"]),
    }


@app.post("/api/v1/devices/register")
def register_device(req: DeviceRegistrationRequest):
    return svc["devices"].register(req.userId, req.deviceId, req.deviceType)


@app.post("/api/v1/devices/heartbeat")
def heartbeat(userId: str, deviceId: str):
    svc["devices"].heartbeat(userId, deviceId)
    return {"userId": userId, "deviceId": deviceId, "acknowledged": True}


@app.get("/api/v1/users/{user_id}/devices")
def list_devices(user_id: str):
    return svc["devices"].list_for_user(user_id)


@app.get("/api/v1/conflicts/{user_id}")
def conflicts(user_id: str, limit: int = 50):
    limit = max(1, min(limit, 500))
    rows = svc["store"].recent_conflicts(user_id, limit)
    for r in rows:
        r["resolvedAt"] = _iso(r["resolvedAt"])
    return rows


@app.get("/api/v1/metrics")
def metrics():
    return svc["metrics"].snapshot()


@app.get("/api/v1/status")
def status():
    return svc["status"].status()


@app.get("/api/v1/health")
def health():
    return {"status": "UP", "service": "syncpulse-backend"}


@app.get("/api/v1/events")
def events(since: int = 0):
    return {"cursor": svc["live"].current_seq(), "events": svc["live"].since(since)}


@app.post("/api/v1/benchmark/run")
def benchmark(req: BenchmarkRequest | None = None):
    return svc["benchmark"].run(req or BenchmarkRequest())


@app.post("/api/v1/admin/reset")
def admin_reset(clearState: bool = True, reseed: bool = True):
    return svc["admin"].reset(clearState, reseed)


@app.post("/api/v1/admin/seed")
def admin_seed():
    svc["seed"]()
    return {"seeded": True, "user": DemoData.USER,
            "devices": [d for d, _ in DemoData.DEVICES]}


@app.get("/api/v1/admin/demo-info")
def demo_info():
    return {"userId": DemoData.USER, "contentId": DemoData.CONTENT,
            "contentDurationSeconds": DemoData.CONTENT_DURATION,
            "devices": [{"deviceId": d, "type": t} for d, t in DemoData.DEVICES]}


@app.get("/swagger-ui.html")
def swagger_redirect():
    return RedirectResponse(url="/docs")


def _iso(dt):
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    return str(dt)
