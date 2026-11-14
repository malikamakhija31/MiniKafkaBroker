import os

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import router
from app.api.websocket import router as websocket_router
from app.broker.service import BrokerService
from app.cluster.cluster_manager import ClusterManager
from app.consumers.group_manager import ConsumerGroupManager
from app.errors import BrokerError
from app.events.event_manager import EventManager


app = FastAPI(
    title="MiniKafkaBroker",
    description="Partitioned commit-log broker: replication, idempotent producers, atomic transactions and automatic failover",
    version="3.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.events = EventManager()
DATA_DIR = os.getenv("BROKER_DATA_DIR", "data")
NODE_ID = os.getenv("BROKER_NODE_ID", "broker-1")
app.state.broker = BrokerService(data_dir=DATA_DIR, event_manager=app.state.events)
app.state.consumer_groups = ConsumerGroupManager(data_dir=DATA_DIR)
app.state.cluster = ClusterManager(
    broker=app.state.broker,
    node_id=NODE_ID,
    data_dir=DATA_DIR,
    event_manager=app.state.events,
    consumer_groups=app.state.consumer_groups,
)

app.include_router(router)
app.include_router(websocket_router)


@app.exception_handler(BrokerError)
async def broker_error_handler(request: Request, error: BrokerError):
    # Typed errors keep their `code` / `retryable` flag all the way to the frontend.
    return JSONResponse(status_code=error.status_code, content=error.to_dict())


@app.get("/")
async def root():
    return {
        "application": "MiniKafkaBroker",
        "status": "running",
        "node_id": app.state.cluster.node_id,
        "documentation": "/docs",
    }


@app.on_event("startup")
async def synchronize_cluster_metadata():
    # 1) catch up / discard uncommitted tail, 2) start heartbeat + failover + txn recovery loops
    await app.state.cluster.sync_topics_from_cluster()
    app.state.cluster.start_background_tasks()


@app.on_event("shutdown")
async def stop_cluster_tasks():
    await app.state.cluster.stop_background_tasks()
