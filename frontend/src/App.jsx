import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity, Boxes, Database, Radio, RefreshCw, Send, Server, Zap,
  CircleCheck, CircleAlert, Wifi, WifiOff, ShoppingBag, CreditCard,
  Package, Truck, HardDrive, ArrowRight, RotateCcw, Terminal
} from "lucide-react";

const API_NODES = [
  "http://127.0.0.1:8000/api/v1",
  "http://127.0.0.1:8001/api/v1",
  "http://127.0.0.1:8002/api/v1"
];

let activeApiBase = API_NODES[0];

async function request(path, options = {}) {
  const bases = [activeApiBase, ...API_NODES.filter(x => x !== activeApiBase)];
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
        throw new Error(data.detail || `HTTP ${response.status}`);
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
    setCluster(await request("/cluster"));
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
          `/topics/${encodeURIComponent(selectedTopic)}/partitions/${part}/records?offset=0&limit=100`
        );
        setRecords(r.records || []);
      } else {
        setRecords([]);
      }
    } catch {
      setPartitions([]);
      setRecords([]);
    }
  }, [selectedTopic, selectedPartition]);

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

          const wsUrl =
            base.replace("http://", "ws://") + "/ws/events";

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
                "LEADER_CHANGED"
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
    }, 10000);

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

  const logEndOffset = selectedData?.log_end_offset ?? 0;
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

  async function placeOrder(event) {
    event.preventDefault();
    setError("");
    setNotice("");
    setLastOrder(null);

    try {
      const result = await request(
        "/ecommerce/orders",
        {
          method: "POST",
          body: JSON.stringify({
            customer,
            product,
            quantity: Number(quantity),
            amount: Number(amount),
            payment_method: paymentMethod
          })
        }
      );

      setLastOrder(result);

      setNotice(
        `Order ${result.order_id} published through the event pipeline.`
      );

      await refreshAll();
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
          <small>3 broker nodes · RF 3</small>
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
              Commerce Event{" "}
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
                disabled={!topics.some(x => x.name === "orders")}
              >
                <Send size={15} /> Place order & stream events
              </button>
            </form>

            {lastOrder && (
              <div className="mt-4 rounded-xl border border-emerald-200 bg-emerald-50 p-3">
                <p className="text-xs font-black text-emerald-800">
                  {lastOrder.order_id} · PIPELINE PUBLISHED
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
          />

          <div className="grid gap-3 md:grid-cols-3">
            {(cluster.brokers || []).map(broker => (
              <div
                key={broker.broker_id}
                className="rounded-xl border border-emerald-200 bg-emerald-50/40 p-4"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Server size={18} />
                    <b>{broker.broker_id}</b>
                  </div>

                  <span className="text-[10px] font-black text-emerald-700">
                    {broker.status?.toUpperCase()}
                  </span>
                </div>

                <p className="mt-3 font-mono text-[10px] text-slate-500">
                  {broker.url}
                </p>

                <p className="mt-2 text-[10px] text-slate-500">
                  Independent broker process
                </p>
              </div>
            ))}
          </div>

          <div className="mt-4 overflow-x-auto">
            <table className="w-full min-w-[650px] text-left text-xs">
              <thead className="bg-slate-50 text-[9px] uppercase tracking-wider text-slate-500">
                <tr>
                  <th className="px-3 py-3">Topic</th>
                  <th className="px-3 py-3">Partition</th>
                  <th className="px-3 py-3">Leader</th>
                  <th className="px-3 py-3">Follower replicas</th>
                  <th className="px-3 py-3">Assignment</th>
                </tr>
              </thead>

              <tbody>
                {(cluster.topics || []).flatMap(topic =>
                  (topic.partitions || []).map(part => (
                    <tr
                      key={`${topic.name}-${part.partition}`}
                      className="border-t border-slate-100"
                    >
                      <td className="px-3 py-3 font-bold">
                        {topic.name}
                      </td>

                      <td className="px-3 py-3">
                        P{part.partition}
                      </td>

                      <td className="px-3 py-3">
                        <span className="rounded bg-blue-50 px-2 py-1 font-bold text-blue-700">
                          {part.leader}
                        </span>
                      </td>

                      <td className="px-3 py-3 text-slate-600">
                        {(part.replicas || []).join(" · ")}
                      </td>

                      <td className="px-3 py-3 text-[10px] font-bold text-emerald-700">
                        Leader + {(part.replicas || []).length} replicas
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
                  <span className="text-[10px] text-slate-500">
                    {records.length} records shown
                  </span>
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
                  <span className="rounded-lg bg-blue-50 p-2 text-blue-700">
                    <Zap size={14} />
                  </span>

                  <div className="min-w-0">
                    <p className="truncate text-[11px] font-black">
                      {event.type || "BROKER EVENT"}
                    </p>

                    <p className="truncate text-[10px] text-slate-500">
                      {event.topic || "cluster"} · Partition{" "}
                      {event.partition ?? "—"} · Offset{" "}
                      {event.offset ?? "—"}{" "}
                      {event.leader ? `· Leader ${event.leader}` : ""}
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