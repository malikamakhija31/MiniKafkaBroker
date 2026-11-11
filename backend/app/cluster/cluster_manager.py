import asyncio
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


DEFAULT_NODES = [
    {"id": "broker-1", "url": "http://127.0.0.1:8000"},
    {"id": "broker-2", "url": "http://127.0.0.1:8001"},
    {"id": "broker-3", "url": "http://127.0.0.1:8002"},
]


def _http_json(url: str, payload: dict | None = None, timeout: float = 2.0):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="GET" if payload is None else "POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
        return json.loads(raw.decode("utf-8")) if raw else {}


class ClusterManager:
    """Small multi-process educational cluster with synchronous log replication.

    Each broker runs as an independent FastAPI process with its own data directory.
    The shared state file is used for demonstrator-level manual leader reassignment;
    it is not a consensus protocol or production-grade metadata quorum.
    """

    def __init__(self, broker, node_id: str, data_dir: str, event_manager=None):
        self.broker = broker
        self.node_id = node_id
        self.event_manager = event_manager
        configured = os.getenv("BROKER_NODES")
        self.nodes = json.loads(configured) if configured else DEFAULT_NODES
        if not any(node["id"] == node_id for node in self.nodes):
            raise ValueError(f"BROKER_NODE_ID '{node_id}' is not in BROKER_NODES")
        self.state_path = Path(os.getenv("BROKER_CLUSTER_STATE", "data/cluster_state.json"))
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_state()

    def _ensure_state(self):
        if not self.state_path.exists():
            try:
                self._write_state({"leaders": {}})
            except FileNotFoundError:
                # Another broker process may have won the first-start race.
                if not self.state_path.exists():
                    raise

    def _read_state(self):
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"leaders": {}}

    def _write_state(self, state):
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, indent=2), encoding="utf-8")
        temporary.replace(self.state_path)

    def node(self, node_id: str):
        return next((item for item in self.nodes if item["id"] == node_id), None)

    def leader_for(self, topic: str, partition: int):
        state = self._read_state()
        key = f"{topic}:{partition}"
        leader_id = state.get("leaders", {}).get(key)
        if leader_id and self.node(leader_id):
            return leader_id
        return self.nodes[partition % len(self.nodes)]["id"]

    async def _post(self, node, path: str, payload: dict):
        return await asyncio.to_thread(_http_json, f"{node['url']}{path}", payload)

    async def _get(self, node, path: str):
        return await asyncio.to_thread(_http_json, f"{node['url']}{path}", None)

    async def sync_topics_from_cluster(self):
        """Recover topic metadata and missing log records when a broker rejoins."""
        for node in self.nodes:
            if node["id"] == self.node_id:
                continue
            try:
                remote = await self._get(node, "/api/v1/internal/topics")
                for metadata in remote.get("topics", []):
                    if self.broker.topic_manager.get_topic(metadata["name"]) is None:
                        self.broker.create_topic(
                            metadata["name"],
                            int(metadata["partitions"]),
                            int(metadata.get("segment_max_bytes", 1048576)),
                        )
                break
            except Exception:
                continue

        # Select the freshest reachable copy for each partition and catch up
        # from this node's current log-end offset. append_replica is idempotent.
        for topic_metadata in self.broker.list_topics():
            topic = topic_metadata["name"]
            for partition in range(int(topic_metadata["partitions"])):
                candidates = []
                for node in self.nodes:
                    if node["id"] == self.node_id:
                        continue
                    try:
                        state = await self._get(node, f"/api/v1/topics/{topic}/partitions")
                        partition_state = next(
                            item for item in state.get("partitions", [])
                            if int(item["partition"]) == partition
                        )
                        candidates.append((int(partition_state.get("log_end_offset", 0)), node))
                    except Exception:
                        continue
                if not candidates:
                    continue
                _, source = max(candidates, key=lambda item: item[0])
                local_log = self.broker.topic_manager.get_topic(topic)["partitions"][partition]
                next_offset = local_log.next_offset
                while True:
                    try:
                        page = await self._get(
                            source,
                            f"/api/v1/topics/{topic}/partitions/{partition}/records?offset={next_offset}&limit=100",
                        )
                    except Exception:
                        break
                    records = page.get("records", [])
                    if not records:
                        break
                    for record_data in records:
                        await self.broker.append_replica(record_data)
                    next_offset = local_log.next_offset
                    if not page.get("has_more", False):
                        break

    async def create_topic(self, name: str, partitions: int, segment_max_bytes: int):
        existing = self.broker.topic_manager.get_topic(name)
        local_result = existing["metadata"] if existing else self.broker.create_topic(name, partitions, segment_max_bytes)
        synced, failed = [self.node_id], []
        for node in self.nodes:
            if node["id"] == self.node_id:
                continue
            try:
                await self._post(node, "/api/v1/internal/topics", {
                    "name": name, "partitions": partitions, "segment_max_bytes": segment_max_bytes,
                })
                synced.append(node["id"])
            except Exception as error:
                failed.append({"broker": node["id"], "error": str(error)})
        state = self._read_state()
        leaders = state.setdefault("leaders", {})
        for partition in range(partitions):
            leaders.setdefault(f"{name}:{partition}", self.nodes[partition % len(self.nodes)]["id"])
        self._write_state(state)
        return {**local_result, "cluster_sync": {"synced_brokers": synced, "failed_brokers": failed}}

    async def produce(self, topic: str, key: str | None, value: dict, headers: dict | None = None):
        partition = self.broker.choose_partition(topic, key)
        leader_id = self.leader_for(topic, partition)
        leader = self.node(leader_id)
        if leader is None:
            raise RuntimeError("Partition leader is not configured")
        payload = {"topic": topic, "partition": partition, "key": key, "value": value, "headers": headers or {}}
        if leader_id != self.node_id:
            return await self._post(leader, "/api/v1/internal/produce", payload)
        return await self.produce_on_partition(**payload)

    async def produce_on_partition(self, topic: str, partition: int, key: str | None, value: dict, headers: dict | None = None):
        leader_id = self.leader_for(topic, partition)
        if leader_id != self.node_id:
            raise RuntimeError(f"NOT_LEADER: {leader_id}")
        result, record = await self.broker.produce_to_partition(topic, partition, key, value, headers)
        record_data = record.model_dump(mode="json")
        acknowledgements = [self.node_id]
        failures = []
        for node in self.nodes:
            if node["id"] == self.node_id:
                continue
            try:
                await self._post(node, "/api/v1/internal/replicate", {"record": record_data})
                acknowledgements.append(node["id"])
            except Exception as error:
                failures.append({"broker": node["id"], "error": str(error)})
        replication = {
            "replication_factor": len(self.nodes),
            "acknowledgements": len(acknowledgements),
            "acknowledged_by": acknowledgements,
            "status": "complete" if not failures else "degraded",
            "failures": failures,
        }
        result["replication"] = replication
        await self._broadcast({
            "type": "REPLICATION_COMPLETED" if not failures else "REPLICATION_DEGRADED",
            "topic": topic, "partition": partition, "offset": record.offset,
            "leader": self.node_id, **replication,
        })
        return result

    async def receive_replica(self, record_data: dict):
        return await self.broker.append_replica(record_data)

    async def _broadcast(self, event):
        if self.event_manager:
            await self.event_manager.broadcast(event)

    async def node_statuses(self):
        statuses = []
        for node in self.nodes:
            if node["id"] == self.node_id:
                statuses.append({"id": node["id"], "url": node["url"], "status": "online", "local": True})
                continue
            try:
                data = await self._get(node, "/api/v1/internal/health")
                statuses.append({"id": node["id"], "url": node["url"], "status": "online", "local": False, "data": data})
            except Exception as error:
                statuses.append({"id": node["id"], "url": node["url"], "status": "offline", "local": False, "error": str(error)})
        return statuses

    async def snapshot(self):
        statuses = await self.node_statuses()
        topics = []
        for topic in self.broker.list_topics():
            partitions = []
            for partition in range(topic["partitions"]):
                leader = self.leader_for(topic["name"], partition)
                partitions.append({
                    "partition": partition,
                    "leader": leader,
                    "replicas": [node["id"] for node in self.nodes if node["id"] != leader],
                })
            topics.append({"name": topic["name"], "partitions": partitions})
        return {
            "cluster": "MiniKafkaBroker-Cluster",
            "node_id": self.node_id,
            "replication_factor": len(self.nodes),
            "brokers": statuses,
            "topics": topics,
            "leader_election": "manual demonstrator failover; no consensus quorum",
        }

    async def failover(self, topic: str, partition: int, new_leader: str | None = None):
        topic_data = self.broker.topic_manager.get_topic(topic)
        if topic_data is None:
            raise ValueError("Topic does not exist")
        if partition < 0 or partition >= len(topic_data["partitions"]):
            raise ValueError("Invalid partition")
        statuses = await self.node_statuses()
        online = [item["id"] for item in statuses if item["status"] == "online"]
        old_leader = self.leader_for(topic, partition)
        candidates = [item for item in online if item != old_leader]
        if new_leader:
            if new_leader not in candidates:
                raise ValueError("Requested new leader must be an online replica")
            selected = new_leader
        else:
            if not candidates:
                raise ValueError("No online replica is available for failover")
            selected = candidates[0]
        state = self._read_state()
        state.setdefault("leaders", {})[f"{topic}:{partition}"] = selected
        self._write_state(state)
        await self._broadcast({"type": "LEADER_CHANGED", "topic": topic, "partition": partition, "old_leader": old_leader, "new_leader": selected})
        return {"topic": topic, "partition": partition, "old_leader": old_leader, "new_leader": selected, "status": "leader reassigned"}
