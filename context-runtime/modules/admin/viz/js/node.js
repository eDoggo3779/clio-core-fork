// Per-node detail. Everything here is one Monitor query per panel, addressed at
// ?node= (a node id, or "local"); the admin ChiMod forwards each one and hands
// back the decoded payload.

const nodeParam = new URLSearchParams(window.location.search).get('node') || 'local';
const nodePath = `/api/nodes/${encodeURIComponent(nodeParam)}`;
let lastEventId = 0;
const cpuHistory = [];

document.getElementById('title').textContent =
  nodeParam === 'local' ? 'This node' : `Node ${nodeParam}`;

async function renderSystem() {
  const stats = await API.get(`${nodePath}/system_stats?min_event_id=${lastEventId}`);
  (stats.entries || []).forEach((e) => {
    if (e.event_id !== undefined) lastEventId = Math.max(lastEventId, e.event_id + 1);
    cpuHistory.push(e.cpu_usage_pct);
  });
  while (cpuHistory.length > 120) cpuHistory.shift();

  const entries = stats.entries || [];
  const latest = entries.length ? entries[entries.length - 1] : null;
  if (!latest && cpuHistory.length === 0) {
    document.getElementById('system').innerHTML =
      '<div class="empty">No samples yet.</div>';
    return;
  }
  const host = latest
    ? `<div class="sub">${esc(latest.hostname)} · ${esc(latest.ip_address)}
       ${latest.is_leader ? '<span class="badge leader">leader</span>' : ''}</div>`
    : '';
  document.getElementById('system').innerHTML = `
    <div class="card">
      <h3>CPU</h3>
      ${meter('utilization', latest ? latest.cpu_usage_pct : cpuHistory[cpuHistory.length - 1])}
      ${sparkline(cpuHistory)}
    </div>
    <div class="card">
      <h3>Memory</h3>
      ${latest ? meter('used', latest.ram_usage_pct) : ''}
      ${latest ? `<div class="sub">${bytes(latest.ram_available_bytes)} free of
                  ${bytes(latest.ram_total_bytes)}</div>` : ''}
    </div>
    <div class="card">
      <h3>Identity</h3>
      ${host}
      <div class="sub">node ${esc(stats.node_id)}</div>
    </div>`;
}

async function renderWorkers() {
  const data = await API.get(`${nodePath}/workers`);
  document.getElementById('workers').innerHTML = table(data.workers, [
    ['worker_id', 'worker'],
    ['is_active', 'active', (v) => (v ? '<span class="badge alive">yes</span>' : '<span class="badge">idle</span>')],
    ['num_queued_tasks', 'queued'],
    ['num_blocked_tasks', 'blocked'],
    ['num_periodic_tasks', 'periodic'],
    ['num_retry_tasks', 'retry'],
    ['num_tasks_processed', 'processed'],
    ['load', 'load', (v) => num(v, 2)],
    ['suspend_period_us', 'suspend µs'],
  ]);
}

async function renderBdevs() {
  const data = await API.get(`${nodePath}/bdevs`);
  // bdev_stats nests the device payload under "stats" alongside its pool id.
  const rows = (data.devices || []).map((d) => {
    const s = d.stats || d;
    return Object.assign({ pool_id: d.pool_id || '' }, s);
  });
  document.getElementById('bdevs').innerHTML = table(rows, [
    ['pool_id', 'pool'],
    ['pool_name', 'name'],
    ['total_capacity', 'capacity', (v) => bytes(v)],
    ['remaining_capacity', 'free', (v) => bytes(v)],
    ['read_bandwidth_mbps', 'read MB/s', (v) => num(v)],
    ['write_bandwidth_mbps', 'write MB/s', (v) => num(v)],
    ['total_reads', 'reads'],
    ['total_writes', 'writes'],
    ['total_bytes_read', 'bytes read', (v) => bytes(v)],
    ['total_bytes_written', 'bytes written', (v) => bytes(v)],
  ]);
}

async function renderContainers() {
  const data = await API.get(`${nodePath}/containers`);
  const rows = (data.containers || []).map((c) => Object.assign({}, c, {
    methods_learned: (c.methods || []).filter((m) => m.name).length,
  }));
  document.getElementById('containers').innerHTML = table(rows, [
    ['pool_id', 'pool'],
    ['pool_name', 'name'],
    ['chimod_name', 'chimod'],
    ['container_id', 'container'],
    ['methods_learned', 'methods'],
    ['learning_rate', 'learning rate', (v) => num(v, 2)],
  ]);
}

poll(async () => {
  await renderSystem();
  await renderWorkers();
  await renderBdevs();
  await renderContainers();
}, 2000);
