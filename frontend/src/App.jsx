import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity, Boxes, Database, Radio, RefreshCw, Send, Server, Zap,
  CircleCheck, CircleAlert, Wifi, WifiOff, ShoppingBag, CreditCard,
  Package, Truck, HardDrive, ArrowRight, RotateCcw, Terminal
} from "lucide-react";


const API_ORIGIN = (import.meta.env.VITE_API_ORIGIN || "").replace(/\/$/, "");

const API_NODES = API_ORIGIN
  ? [
      `${API_ORIGIN}/broker1/api/v1`,
      `${API_ORIGIN}/broker2/api/v1`,
      `${API_ORIGIN}/broker3/api/v1`
    ]
  : [
      "http://127.0.0.1:8000/api/v1",
      "http://127.0.0.1:8001/api/v1",
      "http://127.0.0.1:8002/api/v1"
    ];


let activeApiBase = API_NODES[0];

// Client-side idempotency token: one id per order *attempt*. Re-sending the same id
// (retry button, double click, automatic failover to another broker) can never
// create a second order.
function newOrderId() {
  const raw = (globalThis.crypto?.randomUUID?.() || Math.random().toString(16).slice(2) + Date.now().toString(16))
    .replace(/-/g, "");
  return `ORD-${raw.slice(0, 8).toUpperCase()}`;
}

const CRASH_POINTS = [
  ["after_event_1", "after event 1 (ORDER_PLACED written)"],
  ["after_event_2", "after event 2 (payment written)"],
  ["after_event_3", "after event 3 (stock written)"],
  ["after_event_4", "after event 4 (all written, before COMMIT)"],
  ["after_decision", "after COMMIT decision (markers not applied)"]
];

function eventDetail(e) {
  switch (e.type) {
    case "LEADER_CHANGED":
      return `${e.topic}-P${e.partition}: ${e.old_leader} → ${e.new_leader} · epoch ${e.epoch} · ${e.automatic ? "automatic failover" : "manual"} (${e.reason})`;
    case "BROKER_DOWN":
    case "BROKER_UP":
      return `${e.broker} · ${e.detail || ""} · seen by ${e.observer}`;
    case "FOLLOWER_CAUGHT_UP":
      return `${e.topic}-P${e.partition}: ${e.records} record(s) re-synced${e.broker ? ` to ${e.broker}` : ""}`;
    case "LOG_TRUNCATED":
      return `${e.topic}-P${e.partition}: ${e.removed} uncommitted record(s) discarded (${e.reason})`;
    case "TXN_RESOLVED":
    case "TXN_RECOVERED":
      return `${e.txn_id} → ${e.decision || "COMMIT"} · ${e.reason || "resolved by partition leader"}`;
    case "TXN_MARKER_APPENDED":
      return `${e.txn_id} · ${e.decision} marker · ${e.topic}-P${e.partition}`;
    case "REPLICATION_FAILED":
      return `${e.topic}-P${e.partition} offset ${e.offset}: only ${e.acknowledgements}/${e.min_insync_replicas} copies, NOT committed`;
    default:
      return `${e.topic || "cluster"} · Partition ${e.partition ?? "—"} · Offset ${e.offset ?? "—"} ${e.leader ? `· Leader ${e.leader}` : ""}`;
  }
}

function eventTone(type) {
  if (["BROKER_DOWN", "REPLICATION_FAILED", "TXN_RESOLVED", "LOG_TRUNCATED"].includes(type)) return "bg-red-50 text-red-700";
  if (["LEADER_CHANGED", "REPLICATION_DEGRADED", "TXN_RECOVERED"].includes(type)) return "bg-amber-50 text-amber-700";
  if (["BROKER_UP", "FOLLOWER_CAUGHT_UP", "TXN_MARKER_APPENDED"].includes(type)) return "bg-emerald-50 text-emerald-700";
  return "bg-blue-50 text-blue-700";
}

function stateBadge(state) {
  const map = {
    COMMITTED: "bg-emerald-50 text-emerald-700",
    COMMIT: "bg-emerald-50 text-emerald-700",
    PENDING: "bg-amber-50 text-amber-700",
    ABORTED: "bg-red-50 text-red-700",
    ABORT: "bg-red-50 text-red-700",
    MARKER: "bg-slate-100 text-slate-600",
    UNREPLICATED: "bg-orange-50 text-orange-700"
  };
  return map[state] || "bg-slate-100 text-slate-600";
}

async function request(path, options = {}) {
  const bases = options.noFailover
    ? [activeApiBase]
    : [activeApiBase, ...API_NODES.filter(x => x !== activeApiBase)];
  let lastError;

  for (const base of bases) {
    try {
      const response = await fetch(`${base}${path}`, {
        ...options,
        headers: {
          "Content-Type": "application/json",
          ...(options.headers || {})
        }
      });

      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        // keep the broker's typed error (code / retryable) so the UI can react correctly
        throw Object.assign(new Error(data.detail || `HTTP ${response.status}`), {
          code: data.code, retryable: data.retryable, status: response.status, data
        });
      }

      activeApiBase = base;
      return data;
    } catch (error) {
      lastError = error;
      if (error instanceof TypeError) continue;
      throw error;
    }
  }

  throw new Error(lastError?.message || "No broker node is reachable.");
}

function PanelTitle({ eyebrow, title, action }) {
  return (
    <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
      <div>
        <p className="text-[10px] font-black uppercase tracking-[.18em] text-blue-600">
          {eyebrow}
        </p>
        <h2 className="mt-1 text-lg font-extrabold tracking-tight">{title}</h2>
      </div>
      {action}
    </div>
  );
}

function Metric({ label, value, detail, icon: Icon, tone = "blue" }) {
  const tones = {
    blue: "bg-blue-50 text-blue-700",
    green: "bg-emerald-50 text-emerald-700",
    orange: "bg-orange-50 text-orange-700",
    violet: "bg-violet-50 text-violet-700"
  };

  return (
    <div className="panel p-4 sm:p-5">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-[10px] font-bold uppercase tracking-wider text-slate-500">
            {label}
          </p>
          <p className="mt-3 text-2xl font-black sm:text-3xl">{value}</p>
          <p className="mt-1 text-[11px] text-slate-500">{detail}</p>
        </div>
        <div className={`rounded-xl p-3 ${tones[tone]}`}>
          <Icon size={20} />
        </div>
      </div>
    </div>
  );
}

function App() {
  const [health, setHealth] = useState("Checking");
  const [topics, setTopics] = useState([]);
  const [selectedTopic, setSelectedTopic] = useState("orders");
  const [partitions, setPartitions] = useState([]);
  const [records, setRecords] = useState([]);
  const [selectedPartition, setSelectedPartition] = useState(0);
  const [cluster, setCluster] = useState({ brokers: [], topics: [] });
  const [business, setBusiness] = useState({
    orders: [],
    payments: [],
    inventory: [],
    shipments: []
  });
  const [events, setEvents] = useState([]);
  const [wsStatus, setWsStatus] = useState("connecting");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [lastOrder, setLastOrder] = useState(null);
  const [orderId, setOrderId] = useState(() => newOrderId());
  const [orderProblem, setOrderProblem] = useState(null);
  const [orderBusy, setOrderBusy] = useState(false);
  const [transactions, setTransactions] = useState([]);
  const [faultPoint, setFaultPoint] = useState("after_event_2");
  const [showRaw, setShowRaw] = useState(false);

  const [customer, setCustomer] = useState("Aarav Mehta");
  const [product, setProduct] = useState("Wireless Headphones");
  const [quantity, setQuantity] = useState("1");
  const [amount, setAmount] = useState("2499");
  const [paymentMethod, setPaymentMethod] = useState("UPI");

  const [groupId, setGroupId] = useState("ecommerce-fulfillment");
  const [groupData, setGroupData] = useState({});
  const [commitOffset, setCommitOffset] = useState("0");
  const [newTopic, setNewTopic] = useState("");
  const [newPartitions, setNewPartitions] = useState("3");

  const loadTopics = useCallback(async () => {
    const data = await request("/topics");
    const list = data.topics || [];
    setTopics(list);
    setSelectedTopic(current => current || list[0]?.name || "");
  }, []);

  const loadCluster = useCallback(async () => {
    const [snapshot, tx] = await Promise.all([
      request("/cluster"),
      request("/transactions").catch(() => ({ transactions: [] }))
    ]);
    setCluster(snapshot);
    setTransactions(tx.transactions || []);
  }, []);

  const loadTopicData = useCallback(async () => {
    if (!selectedTopic) {
      setPartitions([]);
      setRecords([]);
      return;
    }

    try {
      const data = await request(
        `/topics/${encodeURIComponent(selectedTopic)}/partitions`
      );

      const list = data.partitions || [];
      setPartitions(list);

      const valid = list.some(
        x => x.partition === Number(selectedPartition)
      );

      const part = valid
        ? Number(selectedPartition)
        : (list[0]?.partition ?? 0);

      if (!valid) setSelectedPartition(part);

      if (list.length) {
        const r = await request(
          `/topics/${encodeURIComponent(selectedTopic)}/partitions/${part}/records?offset=0&limit=100&isolation=${showRaw ? "read_uncommitted" : "read_committed"}`
        );
        setRecords(r.records || []);
      } else {
        setRecords([]);
      }
    } catch {
      setPartitions([]);
      setRecords([]);
    }
  }, [selectedTopic, selectedPartition, showRaw]);

  const loadBusiness = useCallback(async () => {
    const result = {
      orders: [],
      payments: [],
      inventory: [],
      shipments: []
    };

    for (const name of Object.keys(result)) {
      try {
        const p = await request(`/topics/${name}/partitions`);

        for (const part of p.partitions || []) {
          const r = await request(
            `/topics/${name}/partitions/${part.partition}/records?offset=0&limit=100`
          );

          result[name].push(
            ...(r.records || []).map(record => ({
              ...record,
              topic: name
            }))
          );
        }
      } catch {}
    }

    setBusiness(result);
  }, []);

  const loadGroup = useCallback(async () => {
    if (!groupId.trim()) return;

    try {
      const data = await request(
        `/consumer-groups/${encodeURIComponent(groupId)}`
      );
      setGroupData(data.offsets || {});
    } catch {
      setGroupData({});
    }
  }, [groupId]);

  const refreshAll = useCallback(async () => {
    setLoading(true);
    setError("");

    try {
      const status = await request("/system/health");

      setHealth(
        status.status === "healthy"
          ? "Healthy"
          : status.status || "Online"
      );

      await Promise.all([
        loadTopics(),
        loadCluster(),
        loadBusiness(),
        loadTopicData(),
        loadGroup()
      ]);
    } catch (err) {
      setHealth("Offline");
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [
    loadTopics,
    loadCluster,
    loadBusiness,
    loadTopicData,
    loadGroup
  ]);

  useEffect(() => {
    refreshAll();
  }, [refreshAll]);

  useEffect(() => {
    loadTopicData();
  }, [loadTopicData]);

  useEffect(() => {
    loadGroup();
  }, [loadGroup]);

  useEffect(() => {
    let cancelled = false;
    let retryTimer;
    let socket;

    async function connect() {
      for (const base of [
        activeApiBase,
        ...API_NODES.filter(x => x !== activeApiBase)
      ]) {
        if (cancelled) return;

        try {
          const check = await fetch(`${base}/system/health`);

          if (!check.ok) continue;

          const wsUrl = new URL(`${base}/ws/events`);
wsUrl.protocol = wsUrl.protocol === "https:" ? "wss:" : "ws:";

          socket = new WebSocket(wsUrl);

          socket.onopen = () => {
            if (!cancelled) setWsStatus("connected");
          };

          socket.onclose = () => {
            if (!cancelled) {
              setWsStatus("disconnected");
              retryTimer = setTimeout(connect, 3000);
            }
          };

          socket.onerror = () => {
            setWsStatus("disconnected");
          };

          socket.onmessage = message => {
            try {
              const event = JSON.parse(message.data);

              setEvents(old => [
                {
                  ...event,
                  receivedAt: new Date().toLocaleTimeString()
                },
                ...old
              ].slice(0, 40));

              if ([
                "MESSAGE_APPENDED",
                "SEGMENT_ROLLED_OVER",
                "REPLICA_APPENDED",
                "REPLICATION_COMPLETED",
                "REPLICATION_DEGRADED",
                "REPLICATION_FAILED",
                "LEADER_CHANGED",
                "BROKER_DOWN",
                "BROKER_UP",
                "FOLLOWER_CAUGHT_UP",
                "LOG_TRUNCATED",
                "TXN_MARKER_APPENDED",
                "TXN_RESOLVED",
                "TXN_RECOVERED"
              ].includes(event.type)) {
                loadTopics();
                loadCluster();
                loadTopicData();
                loadBusiness();
              }
            } catch {}
          };

          return;
        } catch {}
      }

      if (!cancelled) {
        setWsStatus("disconnected");
        retryTimer = setTimeout(connect, 3000);
      }
    }

    connect();

    return () => {
      cancelled = true;
      clearTimeout(retryTimer);
      socket?.close();
    };
  }, [loadTopics, loadCluster, loadTopicData, loadBusiness]);

  useEffect(() => {
    const timer = setInterval(() => {
      loadTopics();
      loadCluster();
      loadBusiness();
      loadTopicData();
      loadGroup();
    }, 4000);

    return () => clearInterval(timer);
  }, [
    loadTopics,
    loadCluster,
    loadBusiness,
    loadTopicData,
    loadGroup
  ]);

  const orderValues = business.orders
    .map(x => x.value || {})
    .filter(x => x.event_type === "ORDER_PLACED");

  const revenue = orderValues.reduce(
    (sum, x) => sum + Number(x.amount || 0),
    0
  );

  const totalEvents = Object.values(business).reduce(
    (sum, list) => sum + list.length,
    0
  );

  const onlineBrokers = (cluster.brokers || []).filter(
    x => x.status === "online"
  ).length;

  const selectedData = partitions.find(
    x => x.partition === Number(selectedPartition)
  );

  const committedOffset = Number(
    groupData?.[selectedTopic]?.[String(selectedPartition)] ?? 0
  );

  // consumers can only read up to the last stable offset (committed, no open transaction)
  const logEndOffset = selectedData?.last_stable_offset ?? selectedData?.log_end_offset ?? 0;
  const lag = Math.max(0, logEndOffset - committedOffset);

  const segments = partitions.reduce(
    (sum, x) => sum + (x.segment_count || 0),
    0
  );

  const storage = partitions.reduce(
    (sum, x) =>
      sum +
      (x.segments || []).reduce(
        (s, a) => s + (a.size_bytes || 0),
        0
      ),
    0
  );

  const storageLabel =
    storage < 1024
      ? `${storage} B`
      : storage < 1048576
        ? `${(storage / 1024).toFixed(1)} KB`
        : `${(storage / 1048576).toFixed(2)} MB`;

  const pipeline = [
    {
      name: "Orders",
      topic: "orders",
      count: business.orders.length,
      icon: ShoppingBag
    },
    {
      name: "Payments",
      topic: "payments",
      count: business.payments.length,
      icon: CreditCard
    },
    {
      name: "Inventory",
      topic: "inventory",
      count: business.inventory.length,
      icon: Package
    },
    {
      name: "Shipments",
      topic: "shipments",
      count: business.shipments.length,
      icon: Truck
    }
  ];

  const assignment = useMemo(() => {
    return (cluster.topics || []).find(
      x => x.name === selectedTopic
    )?.partitions || [];
  }, [cluster, selectedTopic]);

  async function initializeTopics() {
    try {
      const result = await request(
        "/ecommerce/initialize",
        {
          method: "POST",
          body: "{}"
        }
      );

      setNotice(
        `${result.message}. Topics: ${result.topics.join(", ")}`
      );

      await refreshAll();
    } catch (err) {
      setError(err.message);
    }
  }

  function orderPayload(id) {
    return {
      order_id: id,
      customer,
      product,
      quantity: Number(quantity),
      amount: Number(amount),
      payment_method: paymentMethod
    };
  }

  async function submitOrder(id) {
    return request("/ecommerce/orders", {
      method: "POST",
      body: JSON.stringify(orderPayload(id))
    });
  }

  function describeResult(result) {
    const bits = [];
    if (result.duplicate) bits.push("DUPLICATE detected, original order returned, nothing written twice");
    else if (result.idempotency?.resumed_after_failure)
      bits.push(`resumed after a broker failure (${result.idempotency.events_already_present} event(s) were already stored)`);
    else bits.push("committed atomically on all 4 topics");
    const failovers = result.events.flatMap(e => e.result?.routing?.failovers || []);
    if (failovers.length)
      bits.push(`leader failover: ${failovers.map(f => `${f.from}→${f.to}`).join(", ")}`);
    return `Order ${result.order_id}: ${bits.join(" · ")}`;
  }

  async function runOrder(id, { rotate = true } = {}) {
    setError("");
    setNotice("");
    setOrderProblem(null);
    setOrderBusy(true);

    try {
      const result = await submitOrder(id);
      setLastOrder(result);
      setNotice(describeResult(result));
      if (rotate) setOrderId(newOrderId());
      await refreshAll();
    } catch (err) {
      if (err.code === "TRANSACTION_ABORTED") {
        setOrderProblem({
          orderId: id, code: err.code, retryable: false,
          message: `${err.message} A new Order ID was generated.`
        });
        setOrderId(newOrderId());
      } else if (err.code === "IDEMPOTENCY_CONFLICT") {
        setOrderProblem({
          orderId: id, code: err.code, retryable: false,
          message: `${err.message} Change the Order ID or restore the original values.`
        });
      } else {
        // network failure / NOT_ENOUGH_REPLICAS: the order may be half-written but invisible
        // (open transaction). Retrying with the SAME id is always safe.
        setOrderProblem({
          orderId: id, code: err.code || "NETWORK", retryable: true,
          message: `${err.message}. Nothing partial is visible. Retry with the same Order ID is safe.`
        });
      }
      await refreshAll().catch(() => {});
    } finally {
      setOrderBusy(false);
    }
  }

  async function placeOrder(event) {
    event.preventDefault();
    await runOrder(orderId);
  }

  async function resubmitLastOrder() {
    if (lastOrder) await runOrder(lastOrder.order_id, { rotate: false });
  }

  async function sendTwiceAtOnce() {
    setError("");
    setNotice("");
    setOrderProblem(null);
    setOrderBusy(true);
    try {
      const results = await Promise.allSettled([submitOrder(orderId), submitOrder(orderId)]);
      const ok = results.filter(r => r.status === "fulfilled").map(r => r.value);
      if (ok.length) setLastOrder(ok[0]);
      const dup = ok.filter(r => r.duplicate).length;
      setNotice(
        `Same order id sent twice at the same time: ${ok.length - dup} write(s), ${dup} duplicate(s) ignored. ` +
        "Check the pipeline: exactly one event per topic."
      );
      setOrderId(newOrderId());
      await refreshAll();
    } finally {
      setOrderBusy(false);
    }
  }

  async function armFault() {
    try {
      const r = await request("/debug/faults", {
        method: "POST", noFailover: true,
        body: JSON.stringify({ crash_point: faultPoint })
      });
      setNotice(`Armed on ${r.node_id}: it will crash ${faultPoint.replace(/_/g, " ")} during the next order. Now place an order.`);
      await loadCluster();
    } catch (err) {
      setError(err.message);
    }
  }

  async function crashBrokerNow() {
    try {
      const r = await request("/debug/crash", { method: "POST", noFailover: true });
      setNotice(`${r.node_id} is crashing now (kill -9 style). Watch the failure detector and the leader election below.`);
      setTimeout(() => refreshAll(), 2500);
    } catch (err) {
      setError(err.message);
    }
  }

  async function createTopic(event) {
    event.preventDefault();

    try {
      const result = await request(
        "/topics",
        {
          method: "POST",
          body: JSON.stringify({
            name: newTopic.trim(),
            partitions: Number(newPartitions),
            segment_max_bytes: 1048576
          })
        }
      );

      setNotice(`Topic ${result.topic.name} created.`);
      setSelectedTopic(newTopic.trim());
      setNewTopic("");

      await refreshAll();
    } catch (err) {
      setError(err.message);
    }
  }

  async function failover() {
    try {
      const result = await request(
        "/cluster/failover",
        {
          method: "POST",
          body: JSON.stringify({
            topic: selectedTopic,
            partition: Number(selectedPartition)
          })
        }
      );

      setNotice(
        `Leader changed: ${result.old_leader} → ${result.new_leader}`
      );

      await refreshAll();
    } catch (err) {
      setError(err.message);
    }
  }

  // FIX: Function renamed to avoid conflict with commitOffset state variable.
  async function handleCommitOffset(event) {
    event.preventDefault();

    try {
      await request(
        `/consumer-groups/${encodeURIComponent(groupId)}/topics/${encodeURIComponent(selectedTopic)}/partitions/${selectedPartition}/commit`,
        {
          method: "POST",
          body: JSON.stringify({
            offset: Number(commitOffset)
          })
        }
      );

      setNotice("Consumer offset committed.");
      await loadGroup();
    } catch (err) {
      setError(err.message);
    }
  }

  return (
    <div className="app-shell min-h-screen">
      <header className="sticky top-0 z-30 border-b border-slate-200 bg-white/95">
        <div className="mx-auto flex max-w-[1700px] items-center justify-between gap-4 px-4 py-3 sm:px-6">
          <div className="flex items-center gap-3">
            <div className="rounded-xl bg-slate-950 p-2.5 text-white">
              <Database size={22} />
            </div>

            <div>
              <h1 className="text-base font-black sm:text-lg">
                MiniKafkaBroker
              </h1>
              <p className="text-[9px] font-extrabold tracking-[.17em] text-slate-500">
                PARTITIONED DISTRIBUTED COMMIT LOG
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <span className="rounded-full border border-slate-200 px-3 py-2 text-[10px] font-bold">
              <span
                className={`mr-2 inline-block h-2 w-2 rounded-full ${
                  health === "Healthy"
                    ? "bg-emerald-500"
                    : "bg-red-500"
                }`}
              />
              API {health}
            </span>

            <span
              className={`rounded-full px-3 py-2 text-[10px] font-black ${
                wsStatus === "connected"
                  ? "bg-emerald-50 text-emerald-700"
                  : "bg-orange-50 text-orange-700"
              }`}
            >
              {wsStatus === "connected" ? (
                <Wifi size={13} className="inline" />
              ) : (
                <WifiOff size={13} className="inline" />
              )}
              {" "}
              {wsStatus === "connected" ? "LIVE STREAM" : "RECONNECTING"}
            </span>

            <button
              onClick={refreshAll}
              className="rounded-lg border border-slate-200 p-2.5"
            >
              <RefreshCw
                size={15}
                className={loading ? "animate-spin" : ""}
              />
            </button>
          </div>
        </div>
      </header>

      <aside className="app-sidebar">
        <div className="sidebar-label">WORKSPACE</div>
        <a href="#overview" className="sidebar-link active">
          <Activity size={16} /> Overview
        </a>
        <a href="#orders" className="sidebar-link">
          <ShoppingBag size={16} /> Order workflow
        </a>
        <a href="#brokers" className="sidebar-link">
          <Server size={16} /> Broker cluster
        </a>
        <a href="#reliability" className="sidebar-link">
          <CircleAlert size={16} /> Failure lab
        </a>
        <a href="#partitions" className="sidebar-link">
          <Boxes size={16} /> Partitions
        </a>
        <a href="#consumers" className="sidebar-link">
          <CreditCard size={16} /> Consumers
        </a>
        <a href="#storage" className="sidebar-link">
          <HardDrive size={16} /> Log storage
        </a>
        <a href="#events" className="sidebar-link">
          <Radio size={16} /> Live events
        </a>
        <a href="#topics" className="sidebar-link">
          <Database size={16} /> Topic explorer
        </a>

        <div className="sidebar-foot">
          <span className="status-dot" /> Distributed cluster
          <br />
          <small>3 broker nodes · RF 3 · min ISR {cluster.min_insync_replicas ?? 2}</small>
        </div>
      </aside>

      <main
        id="overview"
        className="app-main mx-auto max-w-[1700px] space-y-6 px-4 py-6 sm:px-6"
      >
        <section className="flex flex-col justify-between gap-4 xl:flex-row xl:items-end">
          <div>
            <p className="text-[10px] font-black uppercase tracking-[.2em] text-blue-700">
              Distributed systems / live operations
            </p>

            <h2 className="mt-2 text-3xl font-black sm:text-4xl">
              E-Commerce Event{" "}
              <span className="text-blue-600">
                Control Center.
              </span>
            </h2>

            <p className="mt-2 max-w-2xl text-sm text-slate-500">
              A live e-commerce event pipeline powered by a
              three-node partitioned commit-log broker cluster.
            </p>
          </div>

          <div className="flex flex-wrap gap-2">
            <button
              onClick={initializeTopics}
              className="primary-button"
            >
              <Boxes size={15} /> Initialize commerce topics
            </button>

            <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-[11px] font-bold">
              {onlineBrokers}/3 brokers online
            </div>
          </div>
        </section>

        {error && (
          <div className="rounded-xl border border-red-200 bg-red-50 p-3 text-sm text-red-700">
            <CircleAlert size={16} className="mr-2 inline" />
            {error}
          </div>
        )}

        {notice && (
          <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-800">
            <CircleCheck size={16} className="mr-2 inline" />
            {notice}
          </div>
        )}

        <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <Metric
            label="Orders placed"
            value={orderValues.length}
            detail="ORDER_PLACED events in broker logs"
            icon={ShoppingBag}
          />

          <Metric
            label="Gross order value"
            value={`₹${revenue.toLocaleString("en-IN")}`}
            detail="Calculated from stored order events"
            icon={CreditCard}
            tone="green"
          />

          <Metric
            label="Business events"
            value={totalEvents}
            detail="Across orders, payments, stock and shipping"
            icon={Activity}
            tone="violet"
          />

          <Metric
            label="Broker cluster"
            value={`${onlineBrokers}/3`}
            detail="Independent broker processes reachable"
            icon={Server}
            tone={onlineBrokers === 3 ? "green" : "orange"}
          />
        </section>

        <section
          id="orders"
          className="grid gap-5 xl:grid-cols-[1.05fr_1.4fr]"
        >
          <div className="panel p-5">
            <PanelTitle
              eyebrow="01 / Business workflow"
              title="Place a demo order"
            />

            <form onSubmit={placeOrder} className="space-y-3">
              <div>
                <label className="field-label">Order ID (idempotency key)</label>
                <div className="flex gap-2">
                  <input
                    className="field min-w-0 font-mono"
                    value={orderId}
                    onChange={e => setOrderId(e.target.value.replace(/[^A-Za-z0-9_-]/g, ""))}
                    required
                  />
                  <button
                    type="button"
                    className="rounded-lg border border-slate-200 px-3"
                    title="Generate a new order id"
                    onClick={() => setOrderId(newOrderId())}
                  >
                    <RotateCcw size={14} />
                  </button>
                </div>
                <p className="mt-1 text-[10px] text-slate-500">
                  Same ID = same order. Retries can never create a duplicate.
                </p>
              </div>

              <div>
                <label className="field-label">Customer name</label>
                <input
                  className="field"
                  value={customer}
                  onChange={e => setCustomer(e.target.value)}
                  required
                />
              </div>

              <div>
                <label className="field-label">Product</label>
                <select
                  className="field"
                  value={product}
                  onChange={e => setProduct(e.target.value)}
                >
                  <option>Wireless Headphones</option>
                  <option>Smart Watch</option>
                  <option>Mechanical Keyboard</option>
                  <option>Bluetooth Speaker</option>
                  <option>USB-C Hub</option>
                </select>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="field-label">Quantity</label>
                  <input
                    className="field"
                    type="number"
                    min="1"
                    value={quantity}
                    onChange={e => setQuantity(e.target.value)}
                  />
                </div>

                <div>
                  <label className="field-label">Amount (₹)</label>
                  <input
                    className="field"
                    type="number"
                    min="1"
                    value={amount}
                    onChange={e => setAmount(e.target.value)}
                  />
                </div>
              </div>

              <div>
                <label className="field-label">Payment method</label>
                <select
                  className="field"
                  value={paymentMethod}
                  onChange={e => setPaymentMethod(e.target.value)}
                >
                  <option>UPI</option>
                  <option>CARD</option>
                  <option>NET_BANKING</option>
                  <option>COD</option>
                </select>
              </div>

              <button
                className="primary-button w-full"
                disabled={orderBusy || !topics.some(x => x.name === "orders")}
              >
                <Send size={15} /> {orderBusy ? "Committing…" : "Place order & stream events"}
              </button>

              <div className="grid grid-cols-2 gap-2">
                <button
                  type="button"
                  className="rounded-lg border border-slate-200 px-2 py-2 text-[11px] font-bold disabled:opacity-40"
                  disabled={orderBusy || !lastOrder}
                  onClick={resubmitLastOrder}
                  title="Sends the previous order id again: the broker must answer 'duplicate'"
                >
                  Resubmit last order (idempotency test)
                </button>
                <button
                  type="button"
                  className="rounded-lg border border-slate-200 px-2 py-2 text-[11px] font-bold disabled:opacity-40"
                  disabled={orderBusy || !topics.some(x => x.name === "orders")}
                  onClick={sendTwiceAtOnce}
                  title="Two parallel requests with the same order id"
                >
                  Send same order twice at once
                </button>
              </div>
            </form>

            {orderProblem && (
              <div className={`mt-4 rounded-xl border p-3 ${orderProblem.retryable ? "border-amber-200 bg-amber-50" : "border-red-200 bg-red-50"}`}>
                <p className="text-xs font-black">
                  {orderProblem.orderId} · {orderProblem.code}
                </p>
                <p className="mt-1 text-[11px]">{orderProblem.message}</p>
                {orderProblem.retryable && (
                  <button
                    type="button"
                    className="mt-2 rounded-lg bg-slate-900 px-3 py-1.5 text-[11px] font-bold text-white"
                    disabled={orderBusy}
                    onClick={() => runOrder(orderProblem.orderId)}
                  >
                    Retry same order ({orderProblem.orderId})
                  </button>
                )}
              </div>
            )}

            {lastOrder && (
              <div className="mt-4 rounded-xl border border-emerald-200 bg-emerald-50 p-3">
                <p className="text-xs font-black text-emerald-800">
                  {lastOrder.order_id} · {lastOrder.state}
                  {lastOrder.duplicate ? " · DUPLICATE (not written again)" : ""}
                  {lastOrder.idempotency?.resumed_after_failure ? " · RESUMED AFTER FAILURE" : ""}
                </p>
                <p className="mt-1 text-[10px] text-emerald-900">
                  Coordinator {lastOrder.transaction?.coordinator} · anchor {lastOrder.transaction?.anchor} ·
                  {" "}{lastOrder.idempotency?.events_already_present}/{lastOrder.events.length} events already existed
                </p>

                <div className="mt-2 grid grid-cols-2 gap-2">
                  {lastOrder.events.map(item => (
                    <div
                      key={item.topic}
                      className="event-result-card"
                    >
                      <p className="text-[10px] font-bold">
                        {item.topic}
                      </p>

                      <p className="mt-1 text-[11px] font-extrabold">
                        {item.event_type}
                      </p>

                      <p className="mt-1 text-[9px]">
                        {item.result.replication?.acknowledgements || 1}/
                        {item.result.replication?.replication_factor || 1}
                        {" "}replicas acked
                      </p>

                      <p className="mt-1 text-[9px] font-bold">
                        {item.result.duplicate ? "⟲ duplicate, reused" : "✓ new write"}
                        {item.result.routing?.failovers?.length ? " · failover" : ""}
                      </p>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          <div className="panel p-5">
            <PanelTitle
              eyebrow="02 / Event pipeline"
              title="Order lifecycle"
            />

            <div className="grid gap-3 sm:grid-cols-2 2xl:grid-cols-4">
              {pipeline.map((stage, index) => {
                const Icon = stage.icon;

                return (
                  <div
                    key={stage.topic}
                    className="rounded-xl border border-slate-200 bg-slate-50/70 p-4"
                  >
                    <div className="flex items-center justify-between">
                      <div className="rounded-lg bg-blue-100 p-2 text-blue-700">
                        <Icon size={17} />
                      </div>
                      <span className="text-[9px] font-black text-slate-400">
                        0{index + 1}
                      </span>
                    </div>

                    <p className="mt-4 text-sm font-extrabold">
                      {stage.name}
                    </p>

                    <p className="font-mono text-[10px] text-slate-500">
                      {stage.topic}
                    </p>

                    <p className="mt-3 text-2xl font-black">
                      {stage.count}
                    </p>

                    <p className="text-[10px] text-slate-500">
                      stored events
                    </p>
                  </div>
                );
              })}
            </div>

            <div className="mt-4 rounded-xl bg-slate-950 p-4 text-slate-100">
              <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-widest text-emerald-400">
                <Terminal size={13} /> Event processing trace
              </div>

              <div className="mt-3 flex flex-wrap items-center gap-2 text-[11px] font-semibold">
                <span className="rounded bg-white/10 px-2.5 py-1.5">
                  ORDER_PLACED
                </span>
                <ArrowRight size={13} />

                <span className="rounded bg-white/10 px-2.5 py-1.5">
                  PAYMENT_AUTHORIZED
                </span>
                <ArrowRight size={13} />

                <span className="rounded bg-white/10 px-2.5 py-1.5">
                  STOCK_RESERVED
                </span>
                <ArrowRight size={13} />

                <span className="rounded bg-white/10 px-2.5 py-1.5">
                  SHIPMENT_CREATED
                </span>
              </div>
            </div>
          </div>
        </section>

        <section id="brokers" className="panel p-5">
          <PanelTitle
            eyebrow="03 / Distributed architecture"
            title="Broker cluster"
            action={
              <span className="text-[10px] text-slate-500">
                RF {cluster.replication_factor ?? 3} · min ISR {cluster.min_insync_replicas ?? 2} · controller {cluster.controller || "—"}
              </span>
            }
          />

          <div className="grid gap-3 md:grid-cols-3">
            {(cluster.brokers || []).map(broker => {
              const up = broker.status === "online";
              return (
                <div
                  key={broker.id}
                  className={`rounded-xl border p-4 ${up ? "border-emerald-200 bg-emerald-50/40" : "border-red-200 bg-red-50"}`}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Server size={18} />
                      <b>{broker.id}</b>
                      {broker.controller && (
                        <span className="rounded bg-blue-100 px-1.5 py-0.5 text-[9px] font-black text-blue-700">
                          CONTROLLER
                        </span>
                      )}
                    </div>

                    <span className={`text-[10px] font-black ${up ? "text-emerald-700" : "text-red-700"}`}>
                      {up ? "ONLINE" : "DOWN"}
                    </span>
                  </div>

                  <p className="mt-3 font-mono text-[10px] text-slate-500">{broker.url}</p>

                  <p className="mt-2 text-[10px] text-slate-500">
                    {up
                      ? `Heartbeat OK${broker.local ? " · answering this request" : ""}`
                      : `Failure detector: ${broker.missed_heartbeats} missed heartbeats · ${broker.error || "unreachable"}`}
                  </p>
                </div>
              );
            })}
          </div>

          <div className="mt-4 overflow-x-auto">
            <table className="w-full min-w-[820px] text-left text-xs">
              <thead className="bg-slate-50 text-[9px] uppercase tracking-wider text-slate-500">
                <tr>
                  <th className="px-3 py-3">Topic</th>
                  <th className="px-3 py-3">Partition</th>
                  <th className="px-3 py-3">Leader (epoch)</th>
                  <th className="px-3 py-3">Replicas (log end · lag)</th>
                  <th className="px-3 py-3">In-sync</th>
                  <th className="px-3 py-3">HW / LSO</th>
                </tr>
              </thead>

              <tbody>
                {(cluster.topics || []).flatMap(topic =>
                  (topic.partitions || []).map(part => (
                    <tr
                      key={`${topic.name}-${part.partition}`}
                      className="border-t border-slate-100"
                    >
                      <td className="px-3 py-3 font-bold">{topic.name}</td>
                      <td className="px-3 py-3">P{part.partition}</td>

                      <td className="px-3 py-3">
                        <span className="rounded bg-blue-50 px-2 py-1 font-bold text-blue-700">
                          {part.leader}
                        </span>
                        <span className="ml-1 font-mono text-[10px] text-slate-500">e{part.epoch}</span>
                      </td>

                      <td className="px-3 py-3">
                        <div className="flex flex-wrap gap-1">
                          {(part.replica_details || []).map(r => (
                            <span
                              key={r.broker}
                              className={`rounded px-1.5 py-0.5 text-[10px] font-bold ${
                                r.status !== "online"
                                  ? "bg-red-100 text-red-700"
                                  : r.in_sync
                                    ? "bg-emerald-50 text-emerald-700"
                                    : "bg-amber-100 text-amber-700"
                              }`}
                            >
                              {r.broker.replace("broker-", "B")}
                              {r.leader ? "★" : ""}: {r.status === "online" ? `${r.log_end_offset ?? "—"}${r.lag ? ` (-${r.lag})` : ""}` : "DOWN"}
                            </span>
                          ))}
                        </div>
                      </td>

                      <td className="px-3 py-3 text-[11px] font-bold text-emerald-700">
                        {(part.isr || []).length}/{(part.replica_details || []).length}
                        {(part.isr || []).length < (cluster.min_insync_replicas ?? 2) && (
                          <span className="ml-1 text-red-600">writes refused</span>
                        )}
                      </td>

                      <td className="px-3 py-3 font-mono text-[10px] text-slate-600">
                        {part.high_watermark ?? "—"} / {part.last_stable_offset ?? "—"}
                        {part.open_transactions > 0 && (
                          <span className="ml-1 rounded bg-amber-100 px-1 font-black text-amber-700">
                            {part.open_transactions} open txn
                          </span>
                        )}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>

            {!(cluster.topics || []).length && (
              <p className="py-6 text-center text-xs text-slate-400">
                Initialize topics to view partition ownership.
              </p>
            )}
          </div>
        </section>

        <section id="reliability" className="grid gap-5 xl:grid-cols-[.9fr_1.4fr]">
          <div className="panel p-5">
            <PanelTitle
              eyebrow="04 / Failure lab"
              title="Break a broker on purpose"
            />

            <p className="text-[11px] text-slate-600">
              Brokers must be started with fault injection enabled (the provided start_cluster.ps1 does this).
              The crash hits the broker this page is currently talking to ({activeApiBase.replace("/api/v1", "")}).
            </p>

            <label className="field-label mt-4">Crash the coordinator broker…</label>
            <select className="field" value={faultPoint} onChange={e => setFaultPoint(e.target.value)}>
              {CRASH_POINTS.map(([value, label]) => (
                <option key={value} value={value}>{label}</option>
              ))}
            </select>

            <button type="button" className="primary-button mt-3 w-full" onClick={armFault}>
              <CircleAlert size={15} /> Arm crash for the next order
            </button>

            {cluster.armed_fault && (
              <p className="mt-2 rounded-lg bg-amber-50 p-2 text-[11px] font-bold text-amber-800">
                Armed on {cluster.node_id}: {cluster.armed_fault}
              </p>
            )}

            <button
              type="button"
              className="mt-3 w-full rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs font-bold text-red-700"
              onClick={crashBrokerNow}
            >
              Crash this broker now (kill -9)
            </button>

            <p className="mt-3 text-[10px] text-slate-500">
              After a crash the page keeps working: requests fail over to another broker, the failure
              detector marks the broker DOWN, partition leaders are re-elected, and an interrupted order
              is resumed by retrying the same Order ID, or aborted automatically after the transaction timeout.
            </p>
          </div>

          <div className="panel overflow-hidden">
            <div className="p-5 pb-3">
              <PanelTitle
                eyebrow="05 / Atomic orders"
                title="Transactions (all-or-nothing)"
                action={<span className="text-[10px] text-slate-500">{transactions.length} recent</span>}
              />
            </div>

            <div className="overflow-x-auto">
              <table className="w-full min-w-[560px] text-left text-xs">
                <thead className="bg-slate-50 text-[9px] uppercase tracking-wider text-slate-500">
                  <tr>
                    <th className="px-4 py-3">Order / txn</th>
                    <th className="px-4 py-3">State</th>
                    <th className="px-4 py-3">Finalized</th>
                    <th className="px-4 py-3">Partitions</th>
                    <th className="px-4 py-3">Coordinator</th>
                  </tr>
                </thead>
                <tbody>
                  {transactions.map(tx => (
                    <tr key={tx.txn_id} className="border-t border-slate-100">
                      <td className="px-4 py-3 font-mono font-bold">{tx.txn_id}</td>
                      <td className="px-4 py-3">
                        <span className={`rounded px-2 py-1 text-[10px] font-black ${stateBadge(tx.state)}`}>
                          {tx.state}
                        </span>
                      </td>
                      <td className="px-4 py-3">{tx.finalized}/{tx.participants.length}</td>
                      <td className="px-4 py-3 font-mono text-[10px] text-slate-600">
                        {tx.participants.map(p => `${p.topic}-P${p.partition}`).join(" · ")}
                      </td>
                      <td className="px-4 py-3 text-slate-500">{tx.coordinator || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {!transactions.length && (
                <p className="py-8 text-center text-xs text-slate-400">
                  No transactions yet. PENDING orders are invisible to consumers until committed.
                </p>
              )}
            </div>

            <div className="border-t border-slate-100 p-5">
              <p className="text-[10px] font-black uppercase tracking-widest text-slate-500">
                Failure handling log (seen by {cluster.node_id || "—"})
              </p>
              <div className="mt-2 max-h-[180px] space-y-1.5 overflow-y-auto">
                {(cluster.recent_events || []).map((e, i) => (
                  <div key={`${e.at}-${i}`} className="flex items-start gap-2 text-[10px]">
                    <span className={`shrink-0 rounded px-1.5 py-0.5 font-black ${eventTone(e.type)}`}>{e.type}</span>
                    <span className="text-slate-600">{eventDetail(e)}</span>
                  </div>
                ))}
                {!(cluster.recent_events || []).length && (
                  <p className="text-[11px] text-slate-400">No failures observed yet.</p>
                )}
              </div>
            </div>
          </div>
        </section>

        <section
          id="partitions"
          className="grid gap-5 xl:grid-cols-[1.2fr_.8fr]"
        >
          <div className="panel p-5">
            <PanelTitle
              eyebrow="04 / Partition operations"
              title="Partition monitor"
              action={
                <select
                  className="field max-w-[190px]"
                  value={selectedTopic}
                  onChange={e => {
                    setSelectedTopic(e.target.value);
                    setSelectedPartition(0);
                  }}
                >
                  {topics.map(topic => (
                    <option key={topic.name} value={topic.name}>
                      {topic.name}
                    </option>
                  ))}
                </select>
              }
            />

            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {partitions.map(part => {
                const owner = assignment.find(
                  x => x.partition === part.partition
                );

                return (
                  <button
                    key={part.partition}
                    onClick={() => setSelectedPartition(part.partition)}
                    className={`partition-card ${
                      Number(selectedPartition) === part.partition
                        ? "selected"
                        : ""
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <p className="text-[10px] font-black uppercase tracking-wider text-slate-500">
                        Partition {part.partition}
                      </p>
                      <span className="h-2 w-2 rounded-full bg-emerald-500" />
                    </div>

                    <p className="mt-3 text-2xl font-black">
                      {part.record_count}
                    </p>

                    <p className="text-[10px] text-slate-500">
                      records · LEO {part.log_end_offset}
                    </p>

                    <div className="mt-3 border-t border-slate-200 pt-2 text-[10px]">
                      <p className="font-bold text-blue-700">
                        Leader: {owner?.leader || "—"}
                      </p>

                      <p className="mt-1 text-slate-500">
                        Replicas: {owner?.replicas?.join(", ") || "—"}
                      </p>
                    </div>
                  </button>
                );
              })}
            </div>

            <div className="mt-4 flex flex-wrap items-center justify-between gap-3 rounded-xl bg-slate-50 p-3">
              <div>
                <p className="text-xs font-extrabold">
                  Selected: {selectedTopic} / Partition {selectedPartition}
                </p>

                <p className="mt-1 text-[10px] text-slate-500">
                  Manually promote an online replica to demonstrate leader failover.
                </p>
              </div>

              <button
                onClick={failover}
                className="inline-flex items-center gap-2 rounded-lg border border-orange-200 bg-white px-3 py-2 text-[11px] font-extrabold text-orange-700"
              >
                <RotateCcw size={14} /> Trigger failover
              </button>
            </div>
          </div>

          <div id="consumers" className="panel p-5">
            <PanelTitle
              eyebrow="05 / Consumer coordination"
              title="Consumer group"
            />

            <label className="field-label">Group ID</label>

            <input
              className="field"
              value={groupId}
              onChange={e => setGroupId(e.target.value)}
            />

            <div className="mt-4 grid grid-cols-3 gap-2">
              <div className="rounded-lg bg-slate-50 p-3">
                <p className="text-[9px] text-slate-500">Committed</p>
                <p className="mt-1 text-xl font-black">
                  {committedOffset}
                </p>
              </div>

              <div className="rounded-lg bg-slate-50 p-3">
                <p className="text-[9px] text-slate-500">Log end</p>
                <p className="mt-1 text-xl font-black">
                  {logEndOffset}
                </p>
              </div>

              <div className="rounded-lg bg-slate-50 p-3">
                <p className="text-[9px] text-slate-500">Lag</p>
                <p className="mt-1 text-xl font-black">
                  {lag}
                </p>
              </div>
            </div>

            <div className="mt-4">
              <div className="mb-2 flex justify-between text-[10px] text-slate-500">
                <span>Consumer progress</span>
                <span>
                  {logEndOffset
                    ? Math.min(
                        100,
                        Math.round(
                          committedOffset / logEndOffset * 100
                        )
                      )
                    : 0}%
                </span>
              </div>

              <div className="h-2 overflow-hidden rounded-full bg-slate-100">
                <div
                  className="h-full rounded-full bg-blue-600"
                  style={{
                    width: `${
                      logEndOffset
                        ? Math.min(
                            100,
                            committedOffset / logEndOffset * 100
                          )
                        : 0
                    }%`
                  }}
                />
              </div>
            </div>

            {/* FIX: Submit handler uses handleCommitOffset */}
            <form
              onSubmit={handleCommitOffset}
              className="mt-4 flex gap-2"
            >
              <input
                className="field min-w-0"
                type="number"
                min="0"
                value={commitOffset}
                onChange={e => setCommitOffset(e.target.value)}
              />

              <button className="primary-button shrink-0">
                Commit offset
              </button>
            </form>
          </div>
        </section>

        <section
          id="storage"
          className="grid gap-5 xl:grid-cols-[1.2fr_.8fr]"
        >
          <div className="panel overflow-hidden">
            <div className="p-5 pb-3">
              <PanelTitle
                eyebrow="06 / Durable storage"
                title="Append-only commit log"
                action={
                  <div className="flex items-center gap-2">
                    <span className="text-[10px] text-slate-500">
                      {records.length} records shown
                    </span>
                    <button
                      type="button"
                      className={`rounded-lg border px-2 py-1 text-[10px] font-bold ${showRaw ? "border-amber-300 bg-amber-50 text-amber-800" : "border-slate-200"}`}
                      onClick={() => setShowRaw(v => !v)}
                      title="Raw = physical log including pending/aborted records and COMMIT/ABORT markers"
                    >
                      {showRaw ? "Raw log (incl. pending & markers)" : "Consumer view (committed only)"}
                    </button>
                  </div>
                }
              />
            </div>

            <div className="overflow-x-auto">
              <table className="w-full min-w-[620px] text-left text-xs">
                <thead className="bg-slate-50 text-[9px] uppercase tracking-wider text-slate-500">
                  <tr>
                    <th className="px-4 py-3">Offset</th>
                    <th className="px-4 py-3">Partition</th>
                    <th className="px-4 py-3">Key</th>
                    <th className="px-4 py-3">Event payload</th>
                    <th className="px-4 py-3">Time</th>
                    <th className="px-4 py-3">Status</th>
                  </tr>
                </thead>

                <tbody>
                  {records.map(record => (
                    <tr
                      key={`${record.partition}-${record.offset}`}
                      className="border-t border-slate-100"
                    >
                      <td className="px-4 py-3 font-mono font-bold text-blue-700">
                        {record.offset}
                      </td>

                      <td className="px-4 py-3">
                        {record.partition}
                      </td>

                      <td className="px-4 py-3 font-mono">
                        {record.key || "—"}
                      </td>

                      <td className="max-w-[280px] truncate px-4 py-3 font-mono text-slate-600">
                        {JSON.stringify(record.value)}
                      </td>

                      <td className="whitespace-nowrap px-4 py-3 text-slate-500">
                        {record.timestamp
                          ? new Date(record.timestamp).toLocaleTimeString()
                          : "—"}
                      </td>

                      <td className="px-4 py-3">
                        <span className={`rounded px-2 py-1 text-[9px] font-black ${stateBadge(record.status)}`}>
                          {record.status || "COMMITTED"}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {!records.length && (
                <p className="py-10 text-center text-xs text-slate-400">
                  No records in this partition.
                </p>
              )}
            </div>
          </div>

          <div className="panel p-5">
            <PanelTitle
              eyebrow="07 / Segment files"
              title="Disk segment map"
            />

            <div className="mb-3 flex justify-between rounded-lg bg-slate-50 p-3 text-xs">
              <span className="text-slate-500">Storage footprint</span>
              <strong>{storageLabel}</strong>
            </div>

            <div className="space-y-2">
              {(selectedData?.segments || []).map(segment => (
                <div
                  key={segment.log_file}
                  className="rounded-lg border border-slate-200 p-3"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <HardDrive size={14} className="text-blue-600" />
                      <span className="font-mono text-[11px] font-bold">
                        {segment.log_file}
                      </span>
                    </div>

                    <span
                      className={`rounded px-2 py-1 text-[9px] font-black ${
                        segment.active
                          ? "bg-emerald-50 text-emerald-700"
                          : "bg-slate-100 text-slate-500"
                      }`}
                    >
                      {segment.active ? "ACTIVE" : "CLOSED"}
                    </span>
                  </div>

                  <div className="mt-3 grid grid-cols-3 gap-2 text-[10px]">
                    <div>
                      <p className="text-slate-400">Records</p>
                      <b>{segment.record_count}</b>
                    </div>

                    <div>
                      <p className="text-slate-400">Size</p>
                      <b>{segment.size_bytes} B</b>
                    </div>

                    <div>
                      <p className="text-slate-400">Offsets</p>
                      <b>
                        {segment.first_offset ?? "—"}–
                        {segment.last_offset ?? "—"}
                      </b>
                    </div>
                  </div>
                </div>
              ))}

              {!selectedData?.segments?.length && (
                <p className="py-8 text-center text-xs text-slate-400">
                  No segment information.
                </p>
              )}
            </div>

            <div className="mt-4 grid grid-cols-2 gap-2 text-[10px]">
              <div className="rounded-lg bg-slate-50 p-3">
                <p className="text-slate-500">Total segments</p>
                <p className="mt-1 text-lg font-black">{segments}</p>
              </div>

              <div className="rounded-lg bg-slate-50 p-3">
                <p className="text-slate-500">Log storage</p>
                <p className="mt-1 text-lg font-black">
                  {storageLabel}
                </p>
              </div>
            </div>
          </div>
        </section>

        <section id="events" className="panel overflow-hidden">
          <div className="p-5 pb-3">
            <PanelTitle
              eyebrow="08 / WebSocket telemetry"
              title="Live cluster events"
              action={
                <span className="text-[10px] text-slate-500">
                  {events.length} recent
                </span>
              }
            />
          </div>

          <div className="max-h-[300px] space-y-2 overflow-y-auto px-5 pb-5">
            {events.map((event, index) => (
              <div
                key={`${event.receivedAt}-${index}`}
                className="live-event-card"
              >
                <div className="flex min-w-0 items-center gap-3">
                  <span className={`rounded-lg p-2 ${eventTone(event.type)}`}>
                    <Zap size={14} />
                  </span>

                  <div className="min-w-0">
                    <p className="truncate text-[11px] font-black">
                      {event.type || "BROKER EVENT"}
                    </p>

                    <p className="truncate text-[10px] text-slate-500">
                      {eventDetail(event)}
                    </p>
                  </div>
                </div>

                <span className="font-mono text-[10px] text-slate-400">
                  {event.receivedAt}
                </span>
              </div>
            ))}

            {!events.length && (
              <div className="rounded-lg border border-dashed border-slate-300 py-8 text-center">
                <Radio size={20} className="mx-auto mb-2 text-slate-300" />
                <p className="text-xs font-bold text-slate-500">
                  Waiting for broker telemetry
                </p>
                <p className="mt-1 text-[10px] text-slate-400">
                  Produce an order or broker record to see live events.
                </p>
              </div>
            )}
          </div>
        </section>

        <section id="topics" className="panel p-5">
          <PanelTitle
            eyebrow="09 / Topic administration"
            title="Topic explorer"
          />

          <div className="flex flex-wrap gap-2">
            {topics.map(topic => (
              <button
                key={topic.name}
                onClick={() => {
                  setSelectedTopic(topic.name);
                  setSelectedPartition(0);
                }}
                className={`rounded-lg border px-3 py-2 text-xs font-bold ${
                  selectedTopic === topic.name
                    ? "border-blue-600 bg-blue-600 text-white"
                    : "border-slate-200 bg-white hover:border-blue-300"
                }`}
              >
                {topic.name}
                <span className="ml-2 opacity-70">
                  {topic.partitions}P
                </span>
              </button>
            ))}
          </div>

          <form
            onSubmit={createTopic}
            className="mt-4 grid gap-2 border-t border-slate-100 pt-4 sm:grid-cols-[1fr_120px_auto]"
          >
            <input
              className="field"
              value={newTopic}
              onChange={e => setNewTopic(e.target.value)}
              placeholder="Additional topic name"
              pattern="[a-zA-Z0-9_-]+"
              required
            />

            <input
              className="field"
              type="number"
              min="1"
              max="32"
              value={newPartitions}
              onChange={e => setNewPartitions(e.target.value)}
            />

            <button className="primary-button">
              <Boxes size={14} /> Create topic
            </button>
          </form>
        </section>

        <footer className="flex flex-col justify-between gap-2 border-t border-slate-200 py-4 text-[9px] font-bold tracking-wider text-slate-400 sm:flex-row">
          <span>
            MINIKAFKABROKER · COMMERCE EVENT STREAMING DEMONSTRATOR
          </span>

          <span>
            3 BROKER PROCESSES · SYNC LOG REPLICATION · MANUAL FAILOVER
          </span>
        </footer>
      </main>
    </div>
  );
}

export default App;