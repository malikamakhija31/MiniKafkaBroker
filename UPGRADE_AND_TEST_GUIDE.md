# MiniKafkaBroker: reliability upgrade

## 1. How to apply
Copy the folders in this package over your project (same relative paths, overwrite when asked). No new pip/npm packages are needed.

| New files | Replaced files |
|---|---|
| `backend/app/errors.py`, `backend/app/utils/atomic.py`, `backend/scripts/start_broker.ps1`, `backend/scripts/kill_broker.ps1` | `backend/app/main.py`, `api/routes.py`, `broker/service.py`, `cluster/cluster_manager.py`, `consumers/group_manager.py`, `models/record.py`, `models/api.py`, `storage/segment.py`, `storage/partition_log.py`, `scripts/start_cluster.ps1`, `frontend/src/App.jsx` |

Your existing `data/` folders keep working (old logs are read as already-committed).
Optional clean start: stop all brokers and delete `backend/data`.

Start: `cd backend; .\scripts\start_cluster.ps1` then `cd ..\frontend; npm run dev`, open http://localhost:5173 and click **Initialize topics**.

## 2. What was added

| Problem before | Now |
|---|---|
| Retry/double-click created duplicate orders | **Idempotency**: `order_id` / `idempotency_key` / `Idempotency-Key` header. Same key = original result returned (`duplicate: true`). Same key + different payload = `409 IDEMPOTENCY_CONFLICT`. Index is rebuilt from the log, so it survives restarts and failover. |
| An order = 4 independent writes; a crash left half an order | **Atomic order** (2-phase commit): events are written invisible, one COMMIT marker on an anchor partition is the commit point, then markers on the other partitions. Consumers use read-committed + last-stable-offset, so partial orders are never visible. Coordinator write-ahead journal + resolver loops finish or abort stuck orders (timeout 15 s). |
| Ack after the leader wrote, replicas "best effort" | **min.insync.replicas = 2** and a persisted **high watermark**: an ack means the record is on 2 brokers. With fewer than 2 brokers alive writes are refused (`503 NOT_ENOUGH_REPLICAS`), never silently lost. |
| Leader dead = errors until you clicked failover | **Heartbeat failure detector** (3 missed beats = DOWN), **automatic leader election** (most complete log wins), **epoch fencing** (a deposed leader is rejected), and the produce path re-elects + retries by itself. |
| Returning broker could keep stale/divergent data | On rejoin it **truncates its uncommitted tail** and catches up from the leader; leaders also back-fill lagging followers continuously (gaps / divergence are repaired automatically). |
| Consumer offsets lived on one broker only | Commits are replicated to all brokers (last-writer-wins) and merged at startup. Commits beyond the last stable offset are rejected. |
| Shared JSON files could be corrupted by 3 processes | Atomic writes (temp+fsync+replace) and a cross-process lock for leader state. |
| UI always showed brokers green (`broker_id` bug) | UI shows real broker state, controller, ISR/lag per replica, epoch, HW/LSO, transactions, failure log, idempotency controls and a **Failure lab**. |

## 3. Test steps
PowerShell helper (paste once):
```powershell
function Call($m,$port,$path,$body=$null,$h=@{}) {
  try { $a=@{Method=$m;Uri="http://127.0.0.1:$port/api/v1$path";Headers=$h;ContentType="application/json"}
        if($body){$a.Body=($body|ConvertTo-Json -Depth 8)}; Invoke-RestMethod @a }
  catch { if($_.ErrorDetails){$_.ErrorDetails.Message}else{"NETWORK ERROR: "+$_.Exception.Message} } }
function Order($port,$id,$amount=2499){ Call Post $port "/ecommerce/orders" @{order_id=$id;customer="Aarav";product="Smart Watch";quantity=1;amount=$amount;payment_method="UPI"} }
```
Ports: broker-1 = 8000, broker-2 = 8001, broker-3 = 8002. Kill = `.\scripts\kill_broker.ps1 -Port 8002` (or close that broker's window). Restart = `.\scripts\start_broker.ps1 -Id broker-3 -Port 8002`.

**T1. Idempotency**
1. UI: place an order, then click **Resubmit last order**. Banner says DUPLICATE, "4/4 events already existed", pipeline counts did not grow.
2. PowerShell: `Order 8000 "ORD-T1"` then `Order 8001 "ORD-T1"` then `Order 8002 "ORD-T1"`. Run 1 returns `duplicate: False`, runs 2 and 3 return `duplicate: True` with the same order.
3. `Order 8000 "ORD-T1" 1` returns `IDEMPOTENCY_CONFLICT` (same id, different amount).
4. `Call Post 8000 "/topics/orders/messages" @{value=@{x=1}} @{"Idempotency-Key"="K1"}` three times: same partition/offset, `duplicate: True` after the first.

**T2. Concurrent double submit:** UI **Send same order twice at once**. Notice says 1 write, 1 duplicate; exactly one new event per topic.

**T3. Follower broker dies**
1. Kill broker-3. After about 3 s UI shows it DOWN and `isr 2/3`.
2. Place an order: succeeds, events show **2/3 replicas acked**.
3. Restart broker-3. Events show `BROKER_UP` + `FOLLOWER_CAUGHT_UP`; replica chips return to green with lag 0; its pipeline counts match.

**T4. Leader broker dies (automatic failover)**
1. Find partitions led by broker-1 in the cluster table (note their epoch). Kill broker-1.
2. The UI keeps working (falls back to 8001). Within about 3 s: `BROKER_DOWN`, `LEADER_CHANGED … automatic failover`, those partitions now have a new leader and **epoch +1**.
3. Place orders: they succeed. Optionally `Order 8001 "ORD-T4"` twice: second is a duplicate.
4. Restart broker-1: it rejoins as follower (`LOG_TRUNCATED` only if it had unconfirmed data, then `FOLLOWER_CAUGHT_UP`). No duplicates in any topic.

**T5. Quorum lost (data safety over availability)**
1. Kill broker-2 and broker-3, wait 4 s, `Order 8000 "ORD-T5"`. Result: `503 NOT_ENOUGH_REPLICAS … Write refused`. UI shows the amber banner with **Retry same order**. Reads/UI lists still work and nothing partial is visible.
2. Restart one broker, then Retry within 15 s: completes. If you wait longer than 15 s the stuck order is safely ABORTED and the UI asks for a new Order ID (`TRANSACTION_ABORTED`).

**T6. Broker dies WHILE writing an order (the main case)**
1. UI, **Failure lab**: choose "after event 2", click **Arm crash**, then **Place order**.
2. The connected broker dies mid-order. The UI retries the same Order ID on another broker automatically. Result banner: `RESUMED AFTER FAILURE`, events 1-2 show "duplicate, reused", events 3-4 "new write".
3. Verify exactly one event per topic. Restart the crashed broker: its journal replays harmlessly (`TXN_RECOVERED`), still no duplicates.

**T7. Nothing partial is ever visible; stuck order is auto-aborted**
1. Bypass the UI failover by talking to one broker: `Call Post 8000 "/debug/faults" @{crash_point="after_event_3"}` then `Order 8000 "ORD-T7"` (network error; broker-1 is dead).
2. UI > Log storage > toggle **Raw log**: ORD-T7 records show `PENDING`; the default **Consumer view** and the pipeline counters do not show them. The Transactions table shows `PENDING`, and a `Call Get 8001 "/transactions"` too.
3. Either run `Order 8001 "ORD-T7"` within 15 s (completes the same order), or wait ~20 s: `TXN_RESOLVED ABORT`, state ABORTED, records `ABORTED` in raw view. Then `Order 8001 "ORD-T7"` returns `TRANSACTION_ABORTED`; a new id works.
4. Restart broker-1 afterwards.

**T8. Crash after the commit decision:** arm "after COMMIT decision", place the order against a single broker (as in T7). Within about 3 s the partition leaders apply the already-durable COMMIT and the order becomes fully visible without any retry.

**T9. Consumer progress survives broker loss:** commit an offset for a group in the UI, kill the broker the UI is connected to, reload: the committed offset is unchanged. Committing above the shown last-stable offset returns `400`.

## 4. Verification and honest limits
* I verified the logic with an in-process simulation of 3 brokers (real code and real files on disk, simulated network, crash and restart of brokers): idempotency incl. concurrency, follower crash, leader crash + auto-failover, quorum loss, coordinator crash at each point, auto-abort, divergent-tail truncation, consumer offset replication, legacy-data loading. All passed. I could not run real `uvicorn` processes or Windows here, so please run T1-T9 once on your machine; the frontend was syntax- and identifier-checked but not rendered.
* Still an educational design: leader/epoch metadata is a shared file (not Raft/ZooKeeper), so it assumes all brokers run on one machine; the idempotency window is unbounded; `/internal/*` and `/debug/*` endpoints are unauthenticated (fault injection only works when `BROKER_ENABLE_FAULTS=1`, which `start_cluster.ps1` sets). After failovers the leaders stay where they were elected (no preferred-leader rebalance); use the existing **failover** button to move them.
* Tunables (environment variables): `BROKER_MIN_ISR` (2), `BROKER_HEARTBEAT_SECONDS` (1), `BROKER_FAILURE_THRESHOLD` (3), `BROKER_TXN_TIMEOUT_SECONDS` (15).
