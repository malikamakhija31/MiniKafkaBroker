import asyncio
import json
import os
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from app.errors import (
    BrokerError, IdempotencyConflict, LogDivergence, NoAvailableLeader, NotEnoughReplicas,
    NotLeader, OffsetGap, PeerUnreachable, StaleEpoch, TransactionAborted, error_from_payload,
)
from app.utils.atomic import FileLock, atomic_write_json, read_json


DEFAULT_NODES = [
    {"id": "broker-1", "url": "http://127.0.0.1:8000"},
    {"id": "broker-2", "url": "http://127.0.0.1:8001"},
    {"id": "broker-3", "url": "http://127.0.0.1:8002"},
]

# ---- tunables (environment variables override, handy for tests) -----------
MIN_ISR = int(os.getenv("BROKER_MIN_ISR", "2"))                      # copies needed before an ack
HEARTBEAT_SECONDS = float(os.getenv("BROKER_HEARTBEAT_SECONDS", "1.0"))
FAILURE_THRESHOLD = int(os.getenv("BROKER_FAILURE_THRESHOLD", "3"))  # missed heartbeats => DOWN
TXN_TIMEOUT_SECONDS = float(os.getenv("BROKER_TXN_TIMEOUT_SECONDS", "15"))
PRODUCE_ATTEMPTS = 4

CRASH_POINTS = ("after_event_1", "after_event_2", "after_event_3", "after_event_4", "after_decision")


def _http_json(url: str, payload: dict | None = None, timeout: float = 2.0):
    """Blocking HTTP call. Network failure -> PeerUnreachable, HTTP error -> typed BrokerError."""
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"},
        method="GET" if payload is None else "POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as error:
        try:
            body = json.loads(error.read().decode("utf-8") or "{}")
        except Exception:
            body = {}
        raise error_from_payload(error.code, body)
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as error:
        raise PeerUnreachable(str(getattr(error, "reason", error)))


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


class ClusterManager:
    """Multi-process cluster logic with real failure handling.

    Guarantees (3 brokers, min.insync.replicas = 2):
      * ACK  = the record is on >= 2 brokers' disks (survives any single broker failure)
      * a broker crash is detected by heartbeats and the partition leaders it owned are
        re-elected automatically (most complete replica wins, epoch fencing stops zombies)
      * a returning broker discards its uncommitted tail and catches up from the leader
      * producers can retry forever: idempotency keys make retries duplicate-free
      * multi-topic writes (an order) are atomic: all events visible or none (2-phase
        commit with a durable decision record on an anchor partition)
    This is still an educational design: leader state lives in a shared file, not in a
    consensus quorum (Raft/ZooKeeper), see "leader_election" in /cluster.
    """

    def __init__(self, broker, node_id: str, data_dir: str, event_manager=None, consumer_groups=None):
        self.broker = broker
        self.node_id = node_id
        self.event_manager = event_manager
        self.consumer_groups = consumer_groups
        configured = os.getenv("BROKER_NODES")
        self.nodes = json.loads(configured) if configured else DEFAULT_NODES
        if not any(node["id"] == node_id for node in self.nodes):
            raise ValueError(f"BROKER_NODE_ID '{node_id}' is not in BROKER_NODES")
        self.min_isr = max(1, min(MIN_ISR, len(self.nodes)))
        self.state_path = Path(os.getenv("BROKER_CLUSTER_STATE", "data/cluster_state.json"))
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_lock_path = self.state_path.with_name(self.state_path.name + ".lock")
        self.journal_path = Path(data_dir) / "txn_journal.json"
        self.peer_state = {
            node["id"]: {"status": "online", "failures": 0, "last_seen": None, "offsets": {}, "error": None}
            for node in self.nodes if node["id"] != node_id
        }
        self.partition_locks = {}
        self.election_locks = {}
        self.cluster_events = deque(maxlen=60)
        self.faults = {}
        self.faults_enabled = os.getenv("BROKER_ENABLE_FAULTS", "0") == "1"
        self._inflight = {}
        self._tasks = []
        self._ensure_state()

    # ------------------------------------------------------------------
    # shared metadata (leaders + epochs), crash/multi-process safe
    # ------------------------------------------------------------------
    def _ensure_state(self):
        if not self.state_path.exists():
            self._mutate_state(lambda state: None)

    def _read_state(self):
        state = read_json(self.state_path, {})
        if not isinstance(state, dict):
            state = {}
        state.setdefault("leaders", {})
        state.setdefault("epochs", {})
        return state

    def _mutate_state(self, mutator):
        with FileLock(self.state_lock_path):
            state = self._read_state()
            result = mutator(state)
            atomic_write_json(self.state_path, state)
            return result

    def _default_leader(self, partition: int):
        return self.nodes[partition % len(self.nodes)]["id"]

    def node(self, node_id: str):
        return next((item for item in self.nodes if item["id"] == node_id), None)

    def leader_for(self, topic: str, partition: int):
        leader_id = self._read_state()["leaders"].get(f"{topic}:{partition}")
        if leader_id and self.node(leader_id):
            return leader_id
        return self._default_leader(partition)

    def epoch_for(self, topic: str, partition: int) -> int:
        return int(self._read_state()["epochs"].get(f"{topic}:{partition}", 0))

    # ------------------------------------------------------------------
    # small helpers
    # ------------------------------------------------------------------
    async def _post(self, node, path: str, payload: dict, timeout: float = 2.0):
        return await asyncio.to_thread(_http_json, f"{node['url']}{path}", payload, timeout)

    async def _get(self, node, path: str, timeout: float = 2.0):
        return await asyncio.to_thread(_http_json, f"{node['url']}{path}", None, timeout)

    async def _broadcast(self, event):
        if self.event_manager:
            await self.event_manager.broadcast(event)

    async def _cluster_event(self, event_type: str, **fields):
        event = {"type": event_type, "at": _now_iso(), "observer": self.node_id, **fields}
        self.cluster_events.appendleft(event)
        await self._broadcast(event)

    def is_online(self, node_id: str) -> bool:
        if node_id == self.node_id:
            return True
        state = self.peer_state.get(node_id)
        return bool(state and state["status"] == "online")

    def controller_id(self):
        """Lowest-id live broker acts as controller (runs failure handling)."""
        for node in self.nodes:
            if self.is_online(node["id"]):
                return node["id"]
        return self.node_id

    def _partition_lock(self, topic, partition):
        return self.partition_locks.setdefault(f"{topic}:{partition}", asyncio.Lock())

    def _local_partitions(self):
        for topic in self.broker.list_topics():
            for partition in range(int(topic["partitions"])):
                yield topic["name"], partition

    def _followers(self, only_online=False):
        return [
            node for node in self.nodes
            if node["id"] != self.node_id and (not only_online or self.is_online(node["id"]))
        ]

    # ------------------------------------------------------------------
    # fault injection (demo / testing only, BROKER_ENABLE_FAULTS=1)
    # ------------------------------------------------------------------
    def set_fault(self, crash_point):
        if not self.faults_enabled:
            raise PermissionError("Fault injection is disabled (start brokers with BROKER_ENABLE_FAULTS=1)")
        if crash_point in (None, "", "none"):
            self.faults = {}
        elif crash_point in CRASH_POINTS:
            self.faults = {"crash_point": crash_point}
        else:
            raise ValueError(f"crash_point must be one of {', '.join(CRASH_POINTS)}")
        return self.faults

    def _crash(self, reason: str):
        os._exit(137)  # simulates kill -9: no cleanup, no goodbye

    def _maybe_crash(self, point: str):
        if self.faults_enabled and self.faults.get("crash_point") == point:
            print(f"[FAULT] {self.node_id} crashing at {point}", flush=True)
            self._crash(point)

    def schedule_crash(self, delay: float = 0.3):
        if not self.faults_enabled:
            raise PermissionError("Fault injection is disabled (start brokers with BROKER_ENABLE_FAULTS=1)")
        asyncio.get_running_loop().call_later(delay, self._crash, "manual")

    # ------------------------------------------------------------------
    # start-up synchronisation (broker rejoining the cluster)
    # ------------------------------------------------------------------
    async def sync_topics_from_cluster(self):
        """Run once at boot: learn topics, drop the uncommitted tail, catch up from the leader."""
        for node in self._followers():
            try:
                remote = await self._get(node, "/api/v1/internal/topics")
                for metadata in remote.get("topics", []):
                    if self.broker.topic_manager.get_topic(metadata["name"]) is None:
                        self.broker.create_topic(
                            metadata["name"], int(metadata["partitions"]),
                            int(metadata.get("segment_max_bytes", 1048576)),
                        )
                break
            except BrokerError:
                continue

        await self._sync_consumer_offsets()

        for topic, partition in self._local_partitions():
            leader_id = self.leader_for(topic, partition)
            if leader_id == self.node_id:
                continue  # we lead it: the maintenance loop reconciles the followers
            log = self.broker.get_log(topic, partition)
            if log.next_offset > log.high_watermark:
                # Records above the high watermark were never confirmed on enough brokers;
                # a new leader may have written different data at those offsets.
                removed = await log.truncate_to(log.high_watermark)
                await self._cluster_event(
                    "LOG_TRUNCATED", topic=topic, partition=partition, removed=removed,
                    reason="discard uncommitted tail on rejoin",
                )
            leader = self.node(leader_id)
            try:
                await self._pull_from(leader, topic, partition, log, adopt_high_watermark=True)
            except BrokerError:
                continue  # leader down right now: it will push to us when we are back in the ISR

    async def _pull_from(self, node, topic, partition, log, adopt_high_watermark=False):
        pulled = 0
        while True:
            page = await self._get(
                node,
                f"/api/v1/internal/log?topic={topic}&partition={partition}&offset={log.next_offset}&limit=200",
                timeout=5.0,
            )
            records = page.get("records", [])
            for record_data in records:
                result = await self.broker.append_replica(record_data)
                pulled += 0 if result.get("duplicate") else 1
            if adopt_high_watermark:
                log.set_high_watermark(min(int(page.get("high_watermark", 0)), log.next_offset))
            if not records or not page.get("has_more"):
                break
        if pulled:
            await self._cluster_event(
                "FOLLOWER_CAUGHT_UP", topic=topic, partition=partition, records=pulled, source=node["id"]
            )
        return pulled

    async def _sync_consumer_offsets(self):
        if self.consumer_groups is None:
            return
        for node in self._followers():
            try:
                remote = await self._get(node, "/api/v1/internal/consumer-offsets")
                for commit in remote.get("commits", []):
                    self.consumer_groups.apply_remote(
                        commit["group"], commit["topic"], commit["partition"], commit["offset"], commit["stamp"]
                    )
            except (BrokerError, ValueError):
                continue

    # ------------------------------------------------------------------
    # topics
    # ------------------------------------------------------------------
    async def create_topic(self, name: str, partitions: int, segment_max_bytes: int):
        existing = self.broker.topic_manager.get_topic(name)
        local_result = existing["metadata"] if existing else self.broker.create_topic(name, partitions, segment_max_bytes)
        synced, failed = [self.node_id], []
        for node in self._followers():
            try:
                await self._post(node, "/api/v1/internal/topics", {
                    "name": name, "partitions": partitions, "segment_max_bytes": segment_max_bytes,
                })
                synced.append(node["id"])
            except BrokerError as error:
                failed.append({"broker": node["id"], "error": str(error)})

        def assign(state):
            for partition in range(partitions):
                state["leaders"].setdefault(f"{name}:{partition}", self._default_leader(partition))
                state["epochs"].setdefault(f"{name}:{partition}", 0)

        self._mutate_state(assign)
        return {**local_result, "cluster_sync": {"synced_brokers": synced, "failed_brokers": failed}}

    # ------------------------------------------------------------------
    # PRODUCE path  (idempotent + replicated + commit by high watermark)
    # ------------------------------------------------------------------
    async def produce(self, topic: str, key: str | None, value: dict, headers: dict | None = None,
                      idempotency_key: str | None = None, txn_id: str | None = None,
                      record_type: str = "DATA"):
        partition = self.broker.choose_partition(topic, key, idempotency_key)
        return await self._send_to_partition(topic, partition, {
            "topic": topic, "partition": partition, "key": key, "value": value,
            "headers": headers or {}, "idempotency_key": idempotency_key,
            "txn_id": txn_id, "record_type": record_type,
        })

    async def _send_to_partition(self, topic: str, partition: int, payload: dict):
        """Route to the partition leader; survive leader crashes by re-electing and retrying.

        Retrying is safe because every retry carries the same idempotency key.
        """
        failovers = []
        last_error = None
        for attempt in range(1, PRODUCE_ATTEMPTS + 1):
            leader_id = self.leader_for(topic, partition)
            try:
                if leader_id == self.node_id:
                    result = await self.produce_on_partition(**payload)
                else:
                    result = await self._post(
                        self.node(leader_id), "/api/v1/internal/produce", payload, timeout=8.0
                    )
                result["routing"] = {
                    "entry_broker": self.node_id, "leader": self.leader_for(topic, partition),
                    "attempts": attempt, "failovers": failovers,
                }
                return result
            except PeerUnreachable as error:
                last_error = error
                self.peer_state[leader_id]["failures"] += 1
                new_leader = await self.elect_leader(topic, partition, failed_leader=leader_id,
                                                     reason="produce_failed")
                if new_leader != leader_id:
                    failovers.append({"from": leader_id, "to": new_leader})
                else:
                    await asyncio.sleep(0.2)
            except (NotLeader, StaleEpoch) as error:
                last_error = error
                await asyncio.sleep(0.1 * attempt)
            except NotEnoughReplicas as error:
                last_error = error
                await asyncio.sleep(0.4 * attempt)
        raise last_error

    async def produce_on_partition(self, topic: str, partition: int, key: str | None, value: dict,
                                   headers: dict | None = None, idempotency_key: str | None = None,
                                   txn_id: str | None = None, record_type: str = "DATA"):
        """Runs on the partition leader. One produce at a time per partition keeps
        offsets and replication strictly ordered."""
        async with self._partition_lock(topic, partition):
            leader_id = self.leader_for(topic, partition)
            if leader_id != self.node_id:
                raise NotLeader(f"NOT_LEADER: {leader_id}", leader=leader_id)
            epoch = self.epoch_for(topic, partition)
            log = self.broker.get_log(topic, partition)

            # transaction control marker: the first decision wins, forever
            if record_type != "DATA":
                existing = log.txn_state.get(txn_id)
                if existing:
                    return {
                        "topic": topic, "partition": partition, "decision": existing,
                        "duplicate": True, "conflict": existing != record_type, "committed": True,
                        "txn_id": txn_id, "record_type": record_type,
                    }

            # idempotent replay: same token => return the original write, never write twice
            if idempotency_key and idempotency_key in log.idem:
                return await self._replay_duplicate(topic, partition, log, idempotency_key, headers, epoch)

            alive = 1 + sum(1 for node in self._followers() if self.is_online(node["id"]))
            if alive < self.min_isr:
                raise NotEnoughReplicas(
                    f"Only {alive} broker(s) alive, need {self.min_isr} (min.insync.replicas). "
                    "Write refused so no acknowledged data can be lost.",
                    alive=alive, required=self.min_isr,
                )

            result, record = await self.broker.produce_to_partition(
                topic, partition, key, value, headers, idempotency_key, txn_id, record_type
            )
            replication = await self._commit_pending(topic, partition, log, epoch)
            if record.offset >= log.high_watermark:
                await self._cluster_event(
                    "REPLICATION_FAILED", topic=topic, partition=partition, offset=record.offset,
                    leader=self.node_id, **replication,
                )
                raise NotEnoughReplicas(
                    f"Record stored on the leader but only {replication['acknowledgements']}/"
                    f"{self.min_isr} required copies exist. It is NOT committed; retry with the same "
                    "idempotency key.",
                    offset=record.offset, acknowledged_by=replication["acknowledged_by"],
                    failures=replication["failures"],
                )

            result.update({
                "replication": replication, "committed": True, "duplicate": False,
                "leader": self.node_id, "epoch": epoch,
            })
            if record_type == "DATA":
                await self._broadcast({
                    "type": "MESSAGE_APPENDED", "topic": topic, "partition": partition,
                    "offset": record.offset, "key": record.key, "value": record.value,
                    "timestamp": record.timestamp.isoformat(), "txn_id": txn_id,
                })
            else:
                result["decision"] = record_type
                result["conflict"] = False
                await self._broadcast({
                    "type": "TXN_MARKER_APPENDED", "topic": topic, "partition": partition,
                    "offset": record.offset, "txn_id": txn_id, "decision": record_type,
                })
            await self._broadcast({
                "type": "REPLICATION_COMPLETED" if replication["status"] == "complete" else "REPLICATION_DEGRADED",
                "topic": topic, "partition": partition, "offset": record.offset,
                "leader": self.node_id, **replication,
            })
            return result

    async def _replay_duplicate(self, topic, partition, log, idempotency_key, headers, epoch):
        offset = log.idem[idempotency_key]
        stored = (await log.read_raw(offset, 1))[0]
        incoming_hash = (headers or {}).get("payload_hash")
        stored_hash = stored.headers.get("payload_hash")
        if incoming_hash and stored_hash and incoming_hash != stored_hash:
            raise IdempotencyConflict(
                f"Idempotency key '{idempotency_key}' was already used with a different payload.",
                idempotency_key=idempotency_key,
            )
        replication = {"status": "already_committed", "high_watermark": log.high_watermark}
        if offset >= log.high_watermark:
            # The original attempt stored the record but died before it was confirmed.
            replication = await self._commit_pending(topic, partition, log, epoch)
            if offset >= log.high_watermark:
                raise NotEnoughReplicas(
                    "Earlier attempt is stored but still cannot reach enough replicas.",
                    offset=offset, failures=replication["failures"],
                )
        return {
            "topic": topic, "partition": partition, "offset": offset,
            "timestamp": stored.timestamp, "key": stored.key, "value": stored.value,
            "headers": stored.headers, "idempotency_key": idempotency_key,
            "txn_id": stored.txn_id, "record_type": stored.record_type,
            "acknowledged": True, "duplicate": True, "committed": True,
            "replication": replication, "leader": self.node_id, "epoch": epoch,
        }

    async def _read_all_from(self, log, start: int):
        records = []
        while True:
            page = await log.read_raw(start, 500)
            if not page:
                break
            records.extend(page)
            start = page[-1].offset + 1
            if len(page) < 500:
                break
        return records

    async def _commit_pending(self, topic, partition, log, epoch):
        """Replicate the uncommitted tail to every follower; once >= min-ISR copies
        exist, advance the high watermark (= the commit point)."""
        tail = await self._read_all_from(log, log.high_watermark)
        followers = self._followers(only_online=True)  # skip brokers the failure detector marked DOWN

        async def push(node):
            try:
                await self._replicate(node, topic, partition, tail, epoch, log)
                return node["id"], None
            except BrokerError as error:
                return node["id"], error

        outcomes = await asyncio.gather(*(push(node) for node in followers)) if tail else []
        acknowledged = [self.node_id] + [nid for nid, error in outcomes if error is None]
        failures = []
        for nid, error in outcomes:
            if error is None:
                continue
            if isinstance(error, StaleEpoch):
                raise NotLeader("A newer leader epoch exists: this broker was deposed.", epoch=epoch)
            failures.append({"broker": nid, "code": error.code, "error": str(error)})
            if isinstance(error, PeerUnreachable):
                self.peer_state[nid]["failures"] += 1

        if tail and len(acknowledged) >= self.min_isr:
            log.set_high_watermark(log.next_offset)
            await self._push_hw(
                [self.node(nid) for nid in acknowledged if nid != self.node_id], topic, partition, log, epoch
            )
        return {
            "replication_factor": len(self.nodes),
            "min_insync_replicas": self.min_isr,
            "acknowledgements": len(acknowledged),
            "acknowledged_by": acknowledged,
            "status": "complete" if len(acknowledged) == len(self.nodes) else "degraded",
            "failures": failures,
            "high_watermark": log.high_watermark,
        }

    async def _replicate(self, node, topic, partition, records, epoch, log):
        """Send records to one follower, transparently healing gaps and divergence."""
        batch = list(records)
        for _ in range(4):
            payload = {
                "topic": topic, "partition": partition, "epoch": epoch,
                "high_watermark": log.high_watermark,
                "records": [record.model_dump(mode="json") for record in batch],
            }
            try:
                return await self._post(node, "/api/v1/internal/replicate", payload, timeout=4.0)
            except OffsetGap as gap:
                start = int(gap.extra.get("expected", 0))
            except LogDivergence as divergence:
                start = int(divergence.extra.get("truncate_to", 0))
                await self._post(node, "/api/v1/internal/truncate", {
                    "topic": topic, "partition": partition, "offset": start, "epoch": epoch,
                })
            batch = await self._read_all_from(log, start)
        raise OffsetGap("Follower could not be repaired", broker=node["id"])

    async def _push_hw(self, nodes, topic, partition, log, epoch):
        async def push(node):
            try:
                await self._post(node, "/api/v1/internal/hw", {
                    "topic": topic, "partition": partition, "epoch": epoch,
                    "high_watermark": log.high_watermark,
                }, timeout=1.5)
            except BrokerError:
                pass
        await asyncio.gather(*(push(node) for node in nodes if node))

    # ---- follower side ---------------------------------------------------
    async def receive_replicas(self, topic, partition, records, epoch, high_watermark):
        current = self.epoch_for(topic, partition)
        if epoch < current:
            raise StaleEpoch(f"Leader epoch {epoch} is older than current epoch {current}", epoch=current)
        log = self.broker.get_log(topic, partition)
        appended = 0
        for record_data in records:
            result = await self.broker.append_replica(record_data)
            appended += 0 if result["duplicate"] else 1
        log.set_high_watermark(min(int(high_watermark), log.next_offset))
        return {"node_id": self.node_id, "appended": appended,
                "log_end": log.next_offset, "high_watermark": log.high_watermark}

    async def receive_high_watermark(self, topic, partition, epoch, high_watermark):
        if epoch < self.epoch_for(topic, partition):
            raise StaleEpoch("Stale epoch")
        log = self.broker.get_log(topic, partition)
        log.set_high_watermark(min(int(high_watermark), log.next_offset))
        return {"high_watermark": log.high_watermark}

    async def truncate_local(self, topic, partition, offset, epoch):
        if epoch < self.epoch_for(topic, partition):
            raise StaleEpoch("Stale epoch")
        log = self.broker.get_log(topic, partition)
        removed = await log.truncate_to(offset)
        if removed:
            await self._cluster_event("LOG_TRUNCATED", topic=topic, partition=partition, removed=removed,
                                      reason="leader ordered truncation of divergent records")
        return {"removed": removed, "log_end": log.next_offset}

    # ------------------------------------------------------------------
    # LEADER ELECTION / FAILOVER
    # ------------------------------------------------------------------
    async def _collect_offsets(self, topic, partition):
        """log-end offset of this partition on every reachable broker (live query)."""
        key = f"{topic}:{partition}"
        found = {self.node_id: self.broker.get_log(topic, partition).next_offset}

        async def ask(node):
            try:
                data = await self._get(node, "/api/v1/internal/offsets", timeout=1.5)
                entry = data.get("partitions", {}).get(key)
                if entry is not None:
                    found[node["id"]] = int(entry["log_end"])
            except BrokerError:
                pass

        await asyncio.gather(*(ask(node) for node in self._followers()))
        return found

    async def _switch_leader(self, topic, partition, expected_current, chosen, reason, automatic):
        key = f"{topic}:{partition}"

        def mutate(state):
            current = state["leaders"].get(key) or self._default_leader(partition)
            if current != expected_current:
                return None  # somebody else already changed it
            state["leaders"][key] = chosen
            state["epochs"][key] = int(state["epochs"].get(key, 0)) + 1
            return state["epochs"][key]

        epoch = self._mutate_state(mutate)
        if epoch is None:
            return self.leader_for(topic, partition)
        await self._cluster_event(
            "LEADER_CHANGED", topic=topic, partition=partition, old_leader=expected_current,
            new_leader=chosen, epoch=epoch, reason=reason, automatic=automatic,
        )
        try:
            if chosen == self.node_id:
                await self.become_leader(topic, partition, epoch)
            else:
                await self._post(self.node(chosen), "/api/v1/internal/become-leader",
                                 {"topic": topic, "partition": partition, "epoch": epoch}, timeout=10.0)
        except BrokerError:
            pass  # the new leader's maintenance loop reconciles anyway
        return chosen

    async def elect_leader(self, topic, partition, failed_leader=None, reason="leader_down"):
        key = f"{topic}:{partition}"
        lock = self.election_locks.setdefault(key, asyncio.Lock())
        async with lock:
            current = self.leader_for(topic, partition)
            if failed_leader and current != failed_leader:
                return current  # leadership already moved
            if failed_leader and failed_leader != self.node_id:
                try:  # leader may only have been slow: do not depose a healthy leader
                    await self._get(self.node(failed_leader), "/api/v1/internal/health", timeout=1.0)
                    return current
                except BrokerError:
                    pass
            offsets = await self._collect_offsets(topic, partition)
            offsets.pop(failed_leader, None)
            if not offsets:
                raise NoAvailableLeader(f"No live replica can lead {key}")
            order = {node["id"]: index for index, node in enumerate(self.nodes)}
            chosen = max(offsets, key=lambda nid: (offsets[nid], -order[nid]))  # most complete log wins
            return await self._switch_leader(topic, partition, current, chosen, reason, automatic=True)

    async def become_leader(self, topic, partition, epoch):
        """New leader: pull anything it is missing, push its log to followers, commit what is safe."""
        log = self.broker.get_log(topic, partition)
        async with self._partition_lock(topic, partition):
            offsets = await self._collect_offsets(topic, partition)
            best = max(offsets, key=lambda nid: (offsets[nid], nid == self.node_id))
            if offsets[best] > log.next_offset and best != self.node_id:
                await self._pull_from(self.node(best), topic, partition, log)
            replication = await self._commit_pending(topic, partition, log, epoch)
        return replication

    async def failover(self, topic: str, partition: int, new_leader: str | None = None):
        topic_data = self.broker.topic_manager.get_topic(topic)
        if topic_data is None:
            raise ValueError("Topic does not exist")
        if partition < 0 or partition >= len(topic_data["partitions"]):
            raise ValueError("Invalid partition")
        view = await self._cluster_view()
        old_leader = self.leader_for(topic, partition)
        candidates = [nid for nid, item in view.items() if item["status"] == "online" and nid != old_leader]
        if new_leader:
            if new_leader not in candidates:
                raise ValueError("Requested new leader must be an online replica")
            selected = new_leader
        else:
            if not candidates:
                raise ValueError("No online replica is available for failover")
            key = f"{topic}:{partition}"
            selected = max(candidates, key=lambda nid: int(
                view[nid]["offsets"].get(key, {}).get("log_end", 0)))
        await self._switch_leader(topic, partition, old_leader, selected, "manual", automatic=False)
        return {"topic": topic, "partition": partition, "old_leader": old_leader,
                "new_leader": selected, "epoch": self.epoch_for(topic, partition),
                "status": "leader reassigned"}

    # ------------------------------------------------------------------
    # BACKGROUND MONITOR: failure detector + repair + transaction resolver
    # ------------------------------------------------------------------
    def start_background_tasks(self):
        if not self._tasks:
            # Two independent loops: heartbeats must never wait for slow repair work.
            self._tasks = [asyncio.create_task(self._detector_loop()),
                           asyncio.create_task(self._maintenance_loop())]

    async def stop_background_tasks(self):
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks = []

    async def _detector_loop(self):
        while True:
            try:
                await asyncio.gather(*(self._probe_peer(node) for node in self._followers()))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                print(f"[detector:{self.node_id}] {type(error).__name__}: {error}", flush=True)
            await asyncio.sleep(HEARTBEAT_SECONDS)

    async def _maintenance_loop(self):
        while True:
            try:
                if self.controller_id() == self.node_id:
                    await self._detect_and_elect()
                await self._leader_maintenance()
                await self._resolve_pending_transactions()
                await self._drive_journal()
            except asyncio.CancelledError:
                raise
            except Exception as error:  # the loop must never die
                print(f"[maintenance:{self.node_id}] {type(error).__name__}: {error}", flush=True)
            await asyncio.sleep(HEARTBEAT_SECONDS)

    async def _probe_peer(self, node):
        state = self.peer_state[node["id"]]
        try:
            data = await self._get(node, "/api/v1/internal/offsets", timeout=1.0)
        except BrokerError as error:
            state["failures"] += 1
            state["error"] = str(error)
            if state["status"] == "online" and state["failures"] >= FAILURE_THRESHOLD:
                state["status"] = "offline"
                await self._cluster_event("BROKER_DOWN", broker=node["id"],
                                          detail=f"{state['failures']} missed heartbeats")
            return
        was_offline = state["status"] == "offline"
        state.update(status="online", failures=0, last_seen=time.time(), error=None,
                     offsets=data.get("partitions", {}))
        if was_offline:
            await self._cluster_event("BROKER_UP", broker=node["id"], detail="heartbeat restored")
        await self._heal_topics(node, state["offsets"])

    async def _heal_topics(self, node, remote_offsets):
        for topic in self.broker.list_topics():
            if f"{topic['name']}:0" not in remote_offsets:
                try:
                    await self._post(node, "/api/v1/internal/topics", {
                        "name": topic["name"], "partitions": topic["partitions"],
                        "segment_max_bytes": topic.get("segment_max_bytes", 1048576),
                    })
                except BrokerError:
                    pass

    async def _detect_and_elect(self):
        for topic, partition in list(self._local_partitions()):
            leader_id = self.leader_for(topic, partition)
            if leader_id != self.node_id and not self.is_online(leader_id):
                try:
                    await self.elect_leader(topic, partition, failed_leader=leader_id,
                                            reason="heartbeat_timeout")
                except BrokerError:
                    pass

    async def _leader_maintenance(self):
        for topic, partition in list(self._local_partitions()):
            if self.leader_for(topic, partition) == self.node_id:
                try:
                    await self._maintain_partition(topic, partition)
                except BrokerError:
                    pass

    async def _maintain_partition(self, topic, partition):
        """Leader duty: bring lagging / returning followers up to date and commit stragglers."""
        log = self.broker.get_log(topic, partition)
        lock = self._partition_lock(topic, partition)
        if lock.locked():
            return
        async with lock:
            if self.leader_for(topic, partition) != self.node_id:
                return
            epoch = self.epoch_for(topic, partition)
            key = f"{topic}:{partition}"
            for node in self._followers(only_online=True):
                remote = self.peer_state[node["id"]]["offsets"].get(key)
                if remote is None:
                    continue
                try:
                    if remote["log_end"] > log.next_offset:
                        await self._post(node, "/api/v1/internal/truncate", {
                            "topic": topic, "partition": partition,
                            "offset": log.next_offset, "epoch": epoch})
                    elif remote["log_end"] < log.next_offset:
                        records = await self._read_all_from(log, remote["log_end"])
                        await self._replicate(node, topic, partition, records, epoch, log)
                        await self._cluster_event("FOLLOWER_CAUGHT_UP", topic=topic, partition=partition,
                                                  records=len(records), broker=node["id"], source=self.node_id)
                except BrokerError:
                    continue
            if log.high_watermark < log.next_offset:
                await self._commit_pending(topic, partition, log, epoch)
            stale = [
                node for node in self._followers(only_online=True)
                if self.peer_state[node["id"]]["offsets"].get(key, {}).get("hw", log.high_watermark)
                < log.high_watermark
            ]
            if stale:
                await self._push_hw(stale, topic, partition, log, epoch)

    # ------------------------------------------------------------------
    # TRANSACTIONS (atomic multi-topic writes)
    # ------------------------------------------------------------------
    def _journal_read(self):
        data = read_json(self.journal_path, {})
        return data if isinstance(data, dict) else {}

    def _journal_set(self, txn_id, entry):
        journal = self._journal_read()
        cutoff = time.time() - 3600
        journal = {k: v for k, v in journal.items()
                   if v.get("status") == "PREPARING" or v.get("finished_at", time.time()) > cutoff}
        journal[txn_id] = entry
        atomic_write_json(self.journal_path, journal)

    def _journal_finish(self, txn_id, status):
        journal = self._journal_read()
        if txn_id in journal:
            journal[txn_id]["status"] = status
            journal[txn_id]["finished_at"] = time.time()
            atomic_write_json(self.journal_path, journal)

    @staticmethod
    def _split_anchor(anchor: str):
        topic, partition = anchor.rsplit(":", 1)
        return topic, int(partition)

    async def _send_marker(self, topic, partition, txn_id, anchor, decision):
        return await self._send_to_partition(topic, partition, {
            "topic": topic, "partition": partition, "key": None,
            "value": {"txn_id": txn_id, "decision": decision},
            "headers": {"txn_anchor": anchor}, "idempotency_key": f"marker:{txn_id}:{decision}",
            "txn_id": txn_id, "record_type": decision,
        })

    async def _decide(self, anchor: str, txn_id: str, decision: str):
        """Durable commit point: the first COMMIT/ABORT marker on the anchor partition wins."""
        topic, partition = self._split_anchor(anchor)
        result = await self._send_marker(topic, partition, txn_id, anchor, decision)
        return result.get("decision", decision), result

    async def run_transaction(self, txn_id: str, events: list, resume: bool = False):
        """Write `events` (one per topic) so that they become visible ALL together or NOT AT ALL.

        1. write-ahead journal entry (survives a coordinator crash)
        2. PREPARE: write every event (idempotent, replicated, but invisible: open transaction)
        3. DECIDE : one COMMIT marker on the anchor partition = the atomic commit point
        4. APPLY  : COMMIT markers on the other partitions (best effort; resolver finishes it)
        Failures before step 3 leave the transaction open: retrying with the same id resumes it,
        otherwise the timeout resolver ABORTs it. Nothing partial is ever visible.
        """
        first = events[0]
        anchor_partition = self.broker.choose_partition(first["topic"], first.get("key"), first.get("idempotency_key"))
        anchor = f"{first['topic']}:{anchor_partition}"
        journal = self._journal_read().get(txn_id)
        if journal is None or journal.get("status") == "PREPARING":
            started = journal["started_at"] if journal else time.time()
            self._journal_set(txn_id, {
                "status": "PREPARING", "events": events, "anchor": anchor,
                "started_at": started, "coordinator": self.node_id,
            })
        self._inflight[txn_id] = self._inflight.get(txn_id, 0) + 1
        try:
            results = []
            try:
                for index, event in enumerate(events, 1):
                    headers = {**event.get("headers", {}), "txn_anchor": anchor}
                    result = await self.produce(
                        event["topic"], event.get("key"), event["value"], headers,
                        idempotency_key=event["idempotency_key"], txn_id=txn_id,
                    )
                    results.append({"topic": event["topic"], "event_type": event["value"].get("event_type"),
                                    "result": result})
                    self._maybe_crash(f"after_event_{index}")
            except IdempotencyConflict:
                raise
            except BrokerError as error:
                if not error.retryable:
                    await self._decide(anchor, txn_id, "ABORT")
                    self._journal_finish(txn_id, "ABORTED")
                raise  # retryable: leave the transaction open so a retry can resume it

            decision, decision_result = await self._decide(anchor, txn_id, "COMMIT")
            self._maybe_crash("after_decision")
            participants = {(item["result"]["topic"], item["result"]["partition"]) for item in results}
            others = [p for p in participants if f"{p[0]}:{p[1]}" != anchor]
            await asyncio.gather(
                *(self._send_marker(t, p, txn_id, anchor, decision) for t, p in others),
                return_exceptions=True,
            )
            self._journal_finish(txn_id, "COMMITTED" if decision == "COMMIT" else "ABORTED")
            if decision != "COMMIT":
                raise TransactionAborted(
                    f"Transaction {txn_id} was ABORTED (timed out or failed earlier). "
                    "Submit the order again with a new order id.", txn_id=txn_id,
                )
            return {
                "txn_id": txn_id, "state": "COMMITTED", "anchor": anchor,
                "coordinator": self.node_id, "events": results,
                "duplicate": all(item["result"].get("duplicate") for item in results)
                and bool(decision_result.get("duplicate")),
            }
        finally:
            self._inflight[txn_id] -= 1
            if self._inflight[txn_id] <= 0:
                self._inflight.pop(txn_id, None)

    async def _lookup_decision(self, anchor: str, txn_id: str):
        topic, partition = self._split_anchor(anchor)
        leader_id = self.leader_for(topic, partition)
        if leader_id == self.node_id:
            return self.broker.get_log(topic, partition).txn_state.get(txn_id)
        try:
            data = await self._get(
                self.node(leader_id),
                f"/api/v1/internal/txn-decision?topic={topic}&partition={partition}&txn_id={txn_id}",
            )
            return data.get("decision")
        except BrokerError:
            return None

    async def _resolve_pending_transactions(self):
        """Partition leaders finish transactions whose coordinator vanished."""
        for topic, partition in list(self._local_partitions()):
            if self.leader_for(topic, partition) != self.node_id:
                continue
            log = self.broker.get_log(topic, partition)
            for txn_id, _first, info in log.pending_txns():
                anchor = info.get("anchor")
                if not anchor:
                    continue
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(info["timestamp"])).total_seconds()
                if age < 2 or self._inflight.get(txn_id):
                    continue
                try:
                    decision = await self._lookup_decision(anchor, txn_id)
                    if decision is None and age >= TXN_TIMEOUT_SECONDS:
                        decision, _ = await self._decide(anchor, txn_id, "ABORT")
                    if decision:
                        await self._send_marker(topic, partition, txn_id, anchor, decision)
                        await self._cluster_event("TXN_RESOLVED", txn_id=txn_id, topic=topic,
                                                  partition=partition, decision=decision)
                except BrokerError:
                    continue

    async def _drive_journal(self):
        """Coordinator crash recovery: resume (or abort when too old) our own open transactions."""
        for txn_id, entry in list(self._journal_read().items()):
            if entry.get("status") != "PREPARING" or self._inflight.get(txn_id):
                continue
            age = time.time() - entry["started_at"]
            if age < 3:
                continue
            try:
                if age >= TXN_TIMEOUT_SECONDS:
                    decision, _ = await self._decide(entry["anchor"], txn_id, "ABORT")
                    self._journal_finish(txn_id, "COMMITTED" if decision == "COMMIT" else "ABORTED")
                    await self._cluster_event("TXN_RESOLVED", txn_id=txn_id, decision=decision,
                                              reason="coordinator timeout")
                else:
                    await self.run_transaction(txn_id, entry["events"], resume=True)
                    await self._cluster_event("TXN_RECOVERED", txn_id=txn_id,
                                              reason="coordinator resumed after restart/failure")
            except BrokerError:
                continue

    def list_transactions(self, limit: int = 30):
        transactions = {}
        journal = self._journal_read()
        for topic, partition in self._local_partitions():
            log = self.broker.get_log(topic, partition)
            for txn_id, first in log.txn_first.items():
                info = log.txn_info[txn_id]
                entry = transactions.setdefault(txn_id, {
                    "txn_id": txn_id, "anchor": info.get("anchor"), "started_at": info["timestamp"],
                    "participants": [], "coordinator": journal.get(txn_id, {}).get("coordinator"),
                })
                entry["participants"].append({
                    "topic": topic, "partition": partition, "offset": first,
                    "state": log.txn_state.get(txn_id, "PENDING"),
                })
        for entry in transactions.values():
            states = {item["state"] for item in entry["participants"]}
            entry["state"] = "COMMITTED" if "COMMIT" in states else "ABORTED" if "ABORT" in states else "PENDING"
            entry["finalized"] = sum(1 for item in entry["participants"] if item["state"] != "PENDING")
        ordered = sorted(transactions.values(), key=lambda item: item["started_at"], reverse=True)
        return ordered[:limit]

    # ------------------------------------------------------------------
    # READ path (always through the partition leader, local fallback)
    # ------------------------------------------------------------------
    async def _via_leader(self, topic, partition, internal_path, local_call):
        leader_id = self.leader_for(topic, partition)
        if leader_id != self.node_id and self.is_online(leader_id):
            try:
                return await self._get(self.node(leader_id), internal_path, timeout=3.0)
            except BrokerError:
                pass
        return await local_call()

    async def fetch(self, topic, partition, offset=0, limit=20, isolation="read_committed"):
        path = (f"/api/v1/internal/fetch?topic={topic}&partition={partition}"
                f"&offset={offset}&limit={limit}&isolation={isolation}")
        return await self._via_leader(
            topic, partition, path,
            lambda: self.broker.fetch(topic, partition, offset, limit, isolation),
        )

    async def partition_state(self, topic, partition):
        async def local():
            return self.broker.get_log(topic, partition).metadata()
        return await self._via_leader(
            topic, partition, f"/api/v1/internal/partition-state?topic={topic}&partition={partition}", local
        )

    async def partition_states(self, topic):
        topic_data = self.broker.topic_manager.get_topic(topic)
        if topic_data is None:
            raise ValueError("Topic does not exist")
        return list(await asyncio.gather(
            *(self.partition_state(topic, index) for index in range(len(topic_data["partitions"])))
        ))

    async def commit_offset(self, group_id, topic, partition, offset):
        state = await self.partition_state(topic, partition)
        if offset > state["last_stable_offset"]:
            raise ValueError(
                f"Cannot commit beyond the last stable offset ({state['last_stable_offset']}): "
                "records after it are not committed yet"
            )
        result = self.consumer_groups.commit_offset(group_id, topic, partition, offset)
        replicated = [self.node_id]

        async def push(node):
            try:
                await self._post(node, "/api/v1/internal/consumer-offsets", {"commits": [{
                    "group": group_id, "topic": topic, "partition": partition,
                    "offset": offset, "stamp": result["stamp"]}]})
                replicated.append(node["id"])
            except BrokerError:
                pass

        await asyncio.gather(*(push(node) for node in self._followers()))
        return {**result, "replicated_to": replicated}

    # ------------------------------------------------------------------
    # observability
    # ------------------------------------------------------------------
    async def _cluster_view(self):
        async def probe(node):
            if node["id"] == self.node_id:
                return node["id"], {"status": "online", "offsets": self.broker.partition_offsets(), "error": None}
            try:
                data = await self._get(node, "/api/v1/internal/offsets", timeout=1.5)
                return node["id"], {"status": "online", "offsets": data.get("partitions", {}), "error": None}
            except BrokerError as error:
                return node["id"], {"status": "offline", "offsets": {}, "error": str(error)}

        return dict(await asyncio.gather(*(probe(node) for node in self.nodes)))

    async def node_statuses(self, view=None):
        view = view or await self._cluster_view()
        controller = next((n["id"] for n in self.nodes if view[n["id"]]["status"] == "online"), None)
        statuses = []
        for node in self.nodes:
            item = view[node["id"]]
            detector = self.peer_state.get(node["id"], {})
            last_seen = detector.get("last_seen")
            statuses.append({
                "id": node["id"], "broker_id": node["id"], "url": node["url"],
                "status": item["status"], "local": node["id"] == self.node_id,
                "controller": node["id"] == controller,
                "missed_heartbeats": detector.get("failures", 0),
                "last_seen_seconds_ago": round(time.time() - last_seen, 1) if last_seen else None,
                "error": item["error"],
            })
        return statuses

    async def snapshot(self):
        view = await self._cluster_view()
        statuses = await self.node_statuses(view)
        topics = []
        for topic in self.broker.list_topics():
            partitions = []
            for partition in range(topic["partitions"]):
                key = f"{topic['name']}:{partition}"
                leader = self.leader_for(topic["name"], partition)
                leader_end = int(view[leader]["offsets"].get(key, {}).get("log_end", 0)) \
                    if view[leader]["status"] == "online" else 0
                details = []
                for node in self.nodes:
                    entry = view[node["id"]]["offsets"].get(key)
                    online = view[node["id"]]["status"] == "online"
                    log_end = int(entry["log_end"]) if entry else None
                    details.append({
                        "broker": node["id"], "leader": node["id"] == leader,
                        "status": "online" if online else "offline",
                        "log_end_offset": log_end,
                        "lag": max(0, leader_end - log_end) if (entry and online) else None,
                        "in_sync": bool(online and entry and log_end == leader_end),
                    })
                leader_entry = view[leader]["offsets"].get(key, {})
                partitions.append({
                    "partition": partition, "leader": leader,
                    "epoch": self.epoch_for(topic["name"], partition),
                    "replicas": [n["id"] for n in self.nodes if n["id"] != leader],
                    "isr": [d["broker"] for d in details if d["in_sync"]],
                    "high_watermark": leader_entry.get("hw"),
                    "last_stable_offset": leader_entry.get("lso"),
                    "log_end_offset": leader_entry.get("log_end"),
                    "open_transactions": leader_entry.get("pending_txns", 0),
                    "replica_details": details,
                })
            topics.append({"name": topic["name"], "partitions": partitions})
        return {
            "cluster": "MiniKafkaBroker-Cluster",
            "node_id": self.node_id,
            "replication_factor": len(self.nodes),
            "min_insync_replicas": self.min_isr,
            "controller": next((s["id"] for s in statuses if s["controller"]), None),
            "brokers": statuses,
            "topics": topics,
            "recent_events": list(self.cluster_events),
            "faults_enabled": self.faults_enabled,
            "armed_fault": self.faults.get("crash_point"),
            "leader_election": "automatic: heartbeat failure detector, most-complete-replica election, "
                               "epoch fencing (leader metadata is a shared file, not a Raft quorum)",
        }
