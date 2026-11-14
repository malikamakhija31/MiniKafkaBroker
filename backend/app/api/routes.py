import hashlib
import json
import re
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request

from app.models.api import CreateTopicRequest, ProduceRequest, CommitOffsetRequest

router = APIRouter(prefix="/api/v1")

ORDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
COMMERCE_TOPICS = ("orders", "payments", "inventory", "shipments")


def get_broker(request: Request):
    return request.app.state.broker


def get_cluster(request: Request):
    return request.app.state.cluster


@router.get("/system/health")
async def health_check(request: Request):
    return {
        "status": "healthy",
        "service": "MiniKafkaBroker",
        "node_id": get_cluster(request).node_id,
        "message": "Broker node is running",
    }


@router.get("/cluster")
async def cluster_status(request: Request):
    return await get_cluster(request).snapshot()


@router.post("/cluster/failover")
async def cluster_failover(body: dict, request: Request):
    try:
        return await get_cluster(request).failover(
            topic=body.get("topic", ""),
            partition=int(body.get("partition", -1)),
            new_leader=body.get("new_leader"),
        )
    except (ValueError, TypeError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.get("/transactions")
async def list_transactions(request: Request, limit: int = Query(default=30, ge=1, le=200)):
    return {"transactions": get_cluster(request).list_transactions(limit)}


@router.post("/topics")
async def create_topic(body: CreateTopicRequest, request: Request):
    try:
        result = await get_cluster(request).create_topic(
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
    return {"metadata": topic_data["metadata"], "partitions": await get_cluster(request).partition_states(topic)}


@router.get("/topics/{topic}/partitions")
async def get_partitions(topic: str, request: Request):
    try:
        return {"topic": topic, "partitions": await get_cluster(request).partition_states(topic)}
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error))


@router.post("/topics/{topic}/messages")
async def produce_message(topic: str, body: ProduceRequest, request: Request):
    """Produce one message. Send an `idempotency_key` (body) or an `Idempotency-Key`
    header and the call becomes safe to retry: the same key never writes twice."""
    idempotency_key = body.idempotency_key or request.headers.get("Idempotency-Key")
    try:
        result = await get_cluster(request).produce(
            topic=topic, key=body.key, value=body.value, headers=body.headers,
            idempotency_key=idempotency_key,
        )
        message = (
            "Duplicate request detected: original message returned, nothing was written again"
            if result.get("duplicate") else "Message committed (replicated to the in-sync replicas)"
        )
        return {"message": message, "result": result}
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error))
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error))


@router.get("/topics/{topic}/partitions/{partition}/records")
async def fetch_records(topic: str, partition: int, request: Request,
                        offset: int = Query(default=0, ge=0),
                        limit: int = Query(default=20, ge=1, le=100),
                        isolation: str = Query(default="read_committed",
                                               pattern="^(read_committed|read_uncommitted)$")):
    try:
        return await get_cluster(request).fetch(topic, partition, offset, limit, isolation)
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
    result = await get_cluster(request).fetch(topic, partition, current_offset, limit, "read_committed")
    return {
        "consumer_group": group_id, "topic": topic, "partition": partition,
        "current_offset": current_offset, "records": result["records"],
        "next_offset": result["next_offset"],
        "message": "Only committed records are delivered. Commit next_offset to save consumer progress",
    }


@router.post("/consumer-groups/{group_id}/topics/{topic}/partitions/{partition}/commit")
async def commit_consumer_offset(group_id: str, topic: str, partition: int,
                                 body: CommitOffsetRequest, request: Request):
    broker = get_broker(request)
    topic_data = broker.topic_manager.get_topic(topic)
    if topic_data is None:
        raise HTTPException(status_code=404, detail="Topic not found")
    if partition < 0 or partition >= len(topic_data["partitions"]):
        raise HTTPException(status_code=404, detail="Partition not found")
    try:
        return await get_cluster(request).commit_offset(group_id, topic, partition, body.offset)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.get("/consumer-groups/{group_id}")
async def get_consumer_group(group_id: str, request: Request):
    return {"consumer_group": group_id, "offsets": request.app.state.consumer_groups.get_group_offsets(group_id)}


# ---------------------------------------------------------------------------
# E-commerce demo: an order is ONE atomic, idempotent transaction over 4 topics
# ---------------------------------------------------------------------------
@router.post("/ecommerce/initialize")
async def initialize_ecommerce(request: Request):
    cluster = get_cluster(request)
    created = []
    for name in COMMERCE_TOPICS:
        existing = cluster.broker.topic_manager.get_topic(name)
        if existing is None:
            await cluster.create_topic(name, partitions=3, segment_max_bytes=1048576)
            created.append(name)
    return {"message": "E-commerce topics are ready", "topics": list(COMMERCE_TOPICS), "created": created}


@router.post("/ecommerce/orders")
async def place_ecommerce_order(body: dict, request: Request):
    """Place an order.

    * ATOMIC     - ORDER_PLACED, PAYMENT_AUTHORIZED, STOCK_RESERVED and SHIPMENT_CREATED
                   become visible together or not at all (transaction + commit marker).
    * IDEMPOTENT - `order_id` (or the `Idempotency-Key` header) is the idempotency token:
                   submitting the same order again returns the original order, never a second one.
    * FAILURE-SAFE - a broker crash mid-order leaves an invisible open transaction; retrying
                   the same order id resumes it, otherwise it is aborted automatically.
    """
    cluster = get_cluster(request)
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
    for name in COMMERCE_TOPICS:
        if cluster.broker.topic_manager.get_topic(name) is None:
            raise HTTPException(status_code=409, detail="Initialize the e-commerce topics first")

    header_key = request.headers.get("Idempotency-Key")
    order_id = body.get("order_id")
    if not order_id:
        order_id = (
            f"ORD-{hashlib.sha256(header_key.encode()).hexdigest()[:8].upper()}"
            if header_key else f"ORD-{uuid4().hex[:8].upper()}"
        )
    order_id = str(order_id)
    if not ORDER_ID_PATTERN.fullmatch(order_id):
        raise HTTPException(status_code=400, detail="order_id may only contain letters, digits, _ and - (max 64)")

    payment_method = str(body.get("payment_method", "CARD"))
    payload_hash = hashlib.sha256(json.dumps({
        "customer": str(body["customer"]), "product": str(body["product"]),
        "quantity": quantity, "amount": amount, "payment_method": payment_method,
    }, sort_keys=True).encode()).hexdigest()[:16]

    timestamp = datetime.now(timezone.utc).isoformat()
    base = {
        "order_id": order_id, "customer": str(body["customer"]),
        "product": str(body["product"]), "quantity": quantity,
        "amount": amount, "created_at": timestamp,
    }
    plan = [
        ("orders", {**base, "event_type": "ORDER_PLACED", "status": "PLACED"}),
        ("payments", {**base, "event_type": "PAYMENT_AUTHORIZED", "status": "SUCCESS", "payment_method": payment_method}),
        ("inventory", {**base, "event_type": "STOCK_RESERVED", "status": "RESERVED"}),
        ("shipments", {**base, "event_type": "SHIPMENT_CREATED", "status": "PROCESSING", "carrier": "Demo Express"}),
    ]
    events = [{
        "topic": topic, "key": order_id, "value": value,
        "idempotency_key": f"{order_id}:{value['event_type']}",
        "headers": {"source": "ecommerce-demo", "order_id": order_id, "payload_hash": payload_hash},
    } for topic, value in plan]

    outcome = await cluster.run_transaction(order_id, events)

    already_present = sum(1 for item in outcome["events"] if item["result"].get("duplicate"))
    stored_value = outcome["events"][0]["result"].get("value") or base
    order = {key: stored_value.get(key, base[key]) for key in base}
    return {
        "message": ("Duplicate order request: the original order was returned, nothing was written twice"
                    if outcome["duplicate"] else "Order committed atomically through the event pipeline"),
        "order_id": order_id, "order": order,
        "state": outcome["state"], "duplicate": outcome["duplicate"],
        "idempotency": {"key": order_id, "events_already_present": already_present,
                        "resumed_after_failure": 0 < already_present < len(events)},
        "transaction": {"txn_id": outcome["txn_id"], "anchor": outcome["anchor"],
                        "coordinator": outcome["coordinator"]},
        "events": [{"topic": item["topic"], "event_type": item["event_type"], "result": item["result"]}
                   for item in outcome["events"]],
        "simulation": "Payment, inventory and shipment are demo downstream services; all events are written "
                      "to the real broker logs and replicated by the cluster.",
    }


# ---------------------------------------------------------------------------
# Fault injection (only when brokers start with BROKER_ENABLE_FAULTS=1)
# ---------------------------------------------------------------------------
@router.get("/debug/faults")
async def get_faults(request: Request):
    cluster = get_cluster(request)
    return {"node_id": cluster.node_id, "enabled": cluster.faults_enabled, "faults": cluster.faults}


@router.post("/debug/faults")
async def set_faults(body: dict, request: Request):
    cluster = get_cluster(request)
    try:
        return {"node_id": cluster.node_id, "faults": cluster.set_fault(body.get("crash_point"))}
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=str(error))
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.post("/debug/crash")
async def crash_now(request: Request):
    cluster = get_cluster(request)
    try:
        cluster.schedule_crash()
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=str(error))
    return {"node_id": cluster.node_id, "message": "This broker process will crash (kill -9 style) in 0.3 s"}


# ---------------------------------------------------------------------------
# Internal node-to-node endpoints. Keep these on the private demo network.
# ---------------------------------------------------------------------------
@router.get("/internal/health")
async def internal_health(request: Request):
    return {"node_id": get_cluster(request).node_id, "status": "online"}


@router.get("/internal/offsets")
async def internal_offsets(request: Request):
    return {"node_id": get_cluster(request).node_id, "partitions": get_broker(request).partition_offsets()}


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
    records = body.get("records")
    if records is None and "record" in body:
        records = [body["record"]]
    try:
        first = records[0] if records else {}
        return await get_cluster(request).receive_replicas(
            topic=body.get("topic") or first["topic"],
            partition=int(body["partition"] if "partition" in body else first["partition"]),
            records=records or [], epoch=int(body.get("epoch", 0)),
            high_watermark=int(body.get("high_watermark", 0)),
        )
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.post("/internal/hw")
async def internal_high_watermark(body: dict, request: Request):
    try:
        return await get_cluster(request).receive_high_watermark(
            body["topic"], int(body["partition"]), int(body.get("epoch", 0)), int(body["high_watermark"]))
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.post("/internal/truncate")
async def internal_truncate(body: dict, request: Request):
    try:
        return await get_cluster(request).truncate_local(
            body["topic"], int(body["partition"]), int(body["offset"]), int(body.get("epoch", 0)))
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.post("/internal/become-leader")
async def internal_become_leader(body: dict, request: Request):
    try:
        return await get_cluster(request).become_leader(
            body["topic"], int(body["partition"]), int(body["epoch"]))
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.post("/internal/produce")
async def internal_produce(body: dict, request: Request):
    try:
        return await get_cluster(request).produce_on_partition(
            topic=body["topic"], partition=int(body["partition"]),
            key=body.get("key"), value=body["value"], headers=body.get("headers") or {},
            idempotency_key=body.get("idempotency_key"), txn_id=body.get("txn_id"),
            record_type=body.get("record_type", "DATA"),
        )
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.get("/internal/log")
async def internal_log(request: Request, topic: str, partition: int,
                       offset: int = Query(default=0, ge=0), limit: int = Query(default=200, ge=1, le=1000)):
    try:
        return await get_broker(request).fetch_raw_log(topic, partition, offset, limit)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error))


@router.get("/internal/fetch")
async def internal_fetch(request: Request, topic: str, partition: int,
                         offset: int = Query(default=0, ge=0), limit: int = Query(default=20, ge=1, le=100),
                         isolation: str = "read_committed"):
    try:
        return await get_broker(request).fetch(topic, partition, offset, limit, isolation)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error))


@router.get("/internal/partition-state")
async def internal_partition_state(request: Request, topic: str, partition: int):
    try:
        return get_broker(request).get_log(topic, partition).metadata()
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error))


@router.get("/internal/txn-decision")
async def internal_txn_decision(request: Request, topic: str, partition: int, txn_id: str):
    try:
        log = get_broker(request).get_log(topic, partition)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error))
    return {"txn_id": txn_id, "decision": log.txn_state.get(txn_id)}


@router.get("/internal/consumer-offsets")
async def internal_consumer_offsets(request: Request):
    return {"commits": request.app.state.consumer_groups.export_commits()}


@router.post("/internal/consumer-offsets")
async def internal_apply_consumer_offsets(body: dict, request: Request):
    manager = request.app.state.consumer_groups
    applied = 0
    try:
        for commit in body.get("commits", []):
            applied += 1 if manager.apply_remote(
                commit["group"], commit["topic"], commit["partition"], commit["offset"], commit["stamp"]
            ) else 0
    except (KeyError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))
    return {"applied": applied}
