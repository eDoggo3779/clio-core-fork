// CTE page: the pool's storage-target roster, plus register / unregister.
// All endpoints are the clio_cte_core module's own (see core_viz.cc).

const MOD = 'clio_cte_core';
let CUR_POOL = null;

async function post(path, fields) {
  const resp = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams(fields).toString(),
  });
  let body = {};
  try { body = await resp.json(); } catch (e) { /* non-JSON error */ }
  if (!resp.ok || body.ok === false) {
    const msg = body.error
      || (body.errors ? Object.entries(body.errors).map(([k, v]) => `${k}: ${v}`).join('; ')
                      : `${resp.status}`);
    throw new Error(msg);
  }
  return body;
}

async function loadPools() {
  const sel = document.getElementById('pool-select');
  const data = await API.get(`/api/mod/${MOD}/pools`);
  const pools = data.pools || [];
  const prev = CUR_POOL;
  sel.innerHTML = '';
  pools.forEach((p) => {
    const o = document.createElement('option');
    o.value = p.pool_id;
    o.textContent = `${p.pool_name} (${p.pool_id})`;
    sel.appendChild(o);
  });
  if (pools.length === 0) {
    CUR_POOL = null;
    document.getElementById('targets').innerHTML =
      '<div class="empty">No CTE pools on this node.</div>';
    return;
  }
  CUR_POOL = pools.some((p) => p.pool_id === prev) ? prev : pools[0].pool_id;
  sel.value = CUR_POOL;
}

async function renderTargets() {
  if (!CUR_POOL) return;
  const data = await API.get(
    `/api/mod/${MOD}/${encodeURIComponent(CUR_POOL)}/targets`);
  document.getElementById('targets').innerHTML = table(data.targets, [
    ['name', 'target'],
    ['score', 'score', (v) => num(v, 3)],
    ['remaining_space', 'free', (v) => bytes(v)],
    ['bytes_read', 'bytes read', (v) => bytes(v)],
    ['bytes_written', 'bytes written', (v) => bytes(v)],
    ['ops_read', 'reads'],
    ['ops_written', 'writes'],
    ['name', '', (v) =>
      `<button onclick="unregisterTarget('${esc(v)}')">Unregister</button>`],
  ]);
}

// eslint-disable-next-line no-unused-vars
async function unregisterTarget(name) {
  if (!window.confirm(`Unregister target ${name}? Placement stops using it.`)) return;
  const msg = document.getElementById('rt-msg');
  try {
    await post(`/api/mod/${MOD}/${encodeURIComponent(CUR_POOL)}/unregister_target`,
      { name });
    msg.textContent = `unregistered ${name}`;
  } catch (e) {
    msg.textContent = `unregister failed: ${e.message}`;
  }
  renderTargets();
}

document.getElementById('rt-submit').onclick = async () => {
  const msg = document.getElementById('rt-msg');
  if (!CUR_POOL) { msg.textContent = 'no pool selected'; return; }
  const fields = { name: document.getElementById('rt-name').value.trim() };
  const attach = document.getElementById('rt-attach').value.trim();
  if (attach) {
    fields.attach_pool_id = attach;
  } else {
    fields.bdev_type = document.getElementById('rt-type').value;
    fields.capacity = document.getElementById('rt-capacity').value.trim();
  }
  try {
    const body = await post(
      `/api/mod/${MOD}/${encodeURIComponent(CUR_POOL)}/register_target`, fields);
    msg.textContent = `registered ${body.name} (bdev ${body.bdev_pool_id})`;
  } catch (e) {
    msg.textContent = `register failed: ${e.message}`;
  }
  renderTargets();
};

document.getElementById('pool-select').onchange = (e) => {
  CUR_POOL = e.target.value;
  renderTargets();
};

poll(async () => {
  await loadPools();
  await renderTargets();
}, 3000);
