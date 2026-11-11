from fastapi import APIRouter, HTTPException, Query, Request
from datetime import datetime, timezone
from uuid import uuid4

from app.models.api import CreateTopicRequest, ProduceRequest, CommitOffsetRequest

router = APIRouter(prefix="/api/v1")


def get_broker(request: Request):
    return request.app.state.broker


@router.get("/system/health")
async def health_check(request: Request):
    return {
        "status": "healthy",
        "service": "MiniKafkaBroker",
        "node_id": request.app.state.cluster.node_id,
        "message": "Broker node is running",
    }


@router.get("/cluster")
async def cluster_status(request: Request):
    return await request.app.state.cluster.snapshot()


@router.post("/cluster/failover")
async def cluster_failover(body: dict, request: Request):
    try:
        return await request.app.state.cluster.failover(
            topic=body.get("topic", ""),
            partition=int(body.get("partition", -1)),
            new_leader=body.get("new_leader"),
        )
    except (ValueError, TypeError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.post("/topics")
async def create_topic(body: CreateTopicRequest, request: Request):
    try:
        result = await request.app.state.cluster.create_topic(
            body.name, body.partitions, body.segment_max_bytes
        )
        return {"message": "Topic created and synchronized across broker nodes", "topic": result}
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.get("/topics")
async def list_topics(request: Request):
    return {"topics": get_broker(request).list_topics()}


@router.get("/topics/{topic}")
async def get_topic(topic: str, request: Request):
    broker = get_broker(request)
    topic_data = broker.topic_manager.get_topic(topic)
    if topic_data is None:
        raise HTTPException(status_code=404, detail="Topic not found")
    return {"metadata": topic_data["metadata"], "partitions": broker.get_partition_states(topic)}


@router.get("/topics/{topic}/partitions")
async def get_partitions(topic: str, request: Request):
    try:
        return {"topic": topic, "partitions": get_broker(request).get_partition_states(topic)}
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error))


@router.post("/topics/{topic}/messages")
async def produce_message(topic: str, body: ProduceRequest, request: Request):
    try:
        result = await request.app.state.cluster.produce(
            topic=topic, key=body.key, value=body.value, headers=body.headers
        )
        return {"message": "Message stored by partition leader", "result": result}
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error))
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error))


@router.get("/topics/{topic}/partitions/{partition}/records")
async def fetch_records(topic: str, partition: int, request: Request,
                        offset: int = Query(default=0, ge=0),
                        limit: int = Query(default=20, ge=1, le=100)):
    try:
        return await get_broker(request).fetch(topic, partition, offset, limit)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error))


@router.get("/consumer-groups/{group_id}/topics/{topic}/partitions/{partition}/consume")
async def consume_messages(group_id: str, topic: str, partition: int, request: Request,
                           limit: int = Query(default=10, ge=1, le=100)):
    broker = get_broker(request)
    manager = request.app.state.consumer_groups
    topic_data = broker.topic_manager.get_topic(topic)
    if topic_data is None:
        raise HTTPException(status_code=404, detail="Topic not found")
    if partition < 0 or partition >= len(topic_data["partitions"]):
        raise HTTPException(status_code=404, detail="Partition not found")
    current_offset = manager.get_offset(group_id, topic, partition)
    result = await broker.fetch(topic, partition, current_offset, limit)
    return {
        "consumer_group": group_id, "topic": topic, "partition": partition,
        "current_offset": current_offset, "records": result["records"],
        "next_offset": result["next_offset"],
        "message": "Commit next_offset to save consumer progress",
    }


@router.post("/consumer-groups/{group_id}/topics/{topic}/partitions/{partition}/commit")
async def commit_consumer_offset(group_id: str, topic: str, partition: int,
                                 body: CommitOffsetRequest, request: Request):
    broker = get_broker(request)
    manager = request.app.state.consumer_groups
    topic_data = broker.topic_manager.get_topic(topic)
    if topic_data is None:
        raise HTTPException(status_code=404, detail="Topic not found")
    if partition < 0 or partition >= len(topic_data["partitions"]):
        raise HTTPException(status_code=404, detail="Partition not found")
    if body.offset > topic_data["partitions"][partition].next_offset:
        raise HTTPException(status_code=400, detail="Cannot commit beyond the log end offset")
    try:
        return manager.commit_offset(group_id, topic, partition, body.offset)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.get("/consumer-groups/{group_id}")
async def get_consumer_group(group_id: str, request: Request):
    return {"consumer_group": group_id, "offsets": request.app.state.consumer_groups.get_group_offsets(group_id)}


# E-commerce demonstration endpoints. Downstream services are simulated by
# publishing their domain events through the real clustered broker API.
@router.post("/ecommerce/initialize")
async def initialize_ecommerce(request: Request):
    cluster = request.app.state.cluster
    created = []
    for name in ("orders", "payments", "inventory", "shipments"):
        existing = cluster.broker.topic_manager.get_topic(name)
        if existing is None:
            await cluster.create_topic(name, partitions=3, segment_max_bytes=1048576)
            created.append(name)
    return {"message": "E-commerce topics are ready", "topics": ["orders", "payments", "inventory", "shipments"], "created": created}


@router.post("/ecommerce/orders")
async def place_ecommerce_order(body: dict, request: Request):
    cluster = request.app.state.cluster
    required = ("customer", "product", "quantity", "amount")
    missing = [field for field in required if field not in body]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing fields: {', '.join(missing)}")
    try:
        quantity = int(body["quantity"])
        amount = float(body["amount"])
        if quantity < 1 or amount <= 0:
            raise ValueError("Quantity and amount must be positive")
    except (TypeError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))
    for name in ("orders", "payments", "inventory", "shipments"):
        if cluster.broker.topic_manager.get_topic(name) is None:
            raise HTTPException(status_code=409, detail="Initialize the e-commerce topics first")
    order_id = body.get("order_id") or f"ORD-{uuid4().hex[:8].upper()}"
    timestamp = datetime.now(timezone.utc).isoformat()
    base = {
        "order_id": order_id, "customer": str(body["customer"]),
        "product": str(body["product"]), "quantity": quantity,
        "amount": amount, "created_at": timestamp,
    }
    event_values = [
        ("orders", {**base, "event_type": "ORDER_PLACED", "status": "PLACED"}),
        ("payments", {**base, "event_type": "PAYMENT_AUTHORIZED", "status": "SUCCESS", "payment_method": body.get("payment_method", "CARD")}),
        ("inventory", {**base, "event_type": "STOCK_RESERVED", "status": "RESERVED"}),
        ("shipments", {**base, "event_type": "SHIPMENT_CREATED", "status": "PROCESSING", "carrier": "Demo Express"}),
    ]
    published = []
    for topic, value in event_values:
        result = await cluster.produce(topic, order_id, value, {"source": "ecommerce-demo", "order_id": order_id})
        published.append({"topic": topic, "event_type": value["event_type"], "result": result})
    return {"message": "Demo order processed through the event pipeline", "order_id": order_id, "order": base, "events": published, "simulation": "Payment, inventory and shipment are demo downstream services; all events are written to the real broker logs and replicated by the cluster."}


# Internal node-to-node endpoints. Keep these on the private demo network.
@router.get("/internal/health")
async def internal_health(request: Request):
    return {"node_id": request.app.state.cluster.node_id, "status": "online"}


@router.get("/internal/topics")
async def internal_list_topics(request: Request):
    return {"topics": get_broker(request).list_topics()}


@router.post("/internal/topics")
async def internal_create_topic(body: CreateTopicRequest, request: Request):
    broker = get_broker(request)
    existing = broker.topic_manager.get_topic(body.name)
    if existing:
        return {"status": "already_exists", "topic": existing["metadata"]}
    result = broker.create_topic(body.name, body.partitions, body.segment_max_bytes)
    return {"status": "created", "topic": result}


@router.post("/internal/replicate")
async def internal_replicate(body: dict, request: Request):
    try:
        result = await get_broker(request).append_replica(body["record"])
        return {"node_id": request.app.state.cluster.node_id, **result}
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.post("/internal/produce")
async def internal_produce(body: dict, request: Request):
    try:
        result = await request.app.state.cluster.produce_on_partition(
            topic=body["topic"], partition=int(body["partition"]),
            key=body.get("key"), value=body["value"], headers=body.get("headers", {}),
        )
        return result
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error))
