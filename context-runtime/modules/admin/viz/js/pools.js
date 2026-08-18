// Pools composed on this node, plus a generic Monitor() explorer.

async function renderPools() {
  const data = await API.get('/api/pools');
  const rows = data.pools || [];
  document.getElementById('pools').innerHTML = table(rows, [
    ['pool_id', 'pool id'],
    ['pool_name', 'name'],
    ['chimod_name', 'chimod'],
    ['container_id', 'container'],
  ]);
  // Clicking a row loads it into the explorer below.
  document.querySelectorAll('#pools tbody tr').forEach((tr, i) => {
    tr.style.cursor = 'pointer';
    tr.onclick = () => {
      document.getElementById('pool').value = rows[i].pool_id;
      runQuery();
    };
  });
}

async function runQuery() {
  const pool = document.getElementById('pool').value.trim();
  const query = document.getElementById('query').value.trim();
  const routing = document.getElementById('routing').value;
  const out = document.getElementById('result');
  if (!pool || !query) {
    out.textContent = 'Need both a pool id and a query.';
    return;
  }
  out.textContent = 'querying…';
  try {
    const data = await API.get(
      `/api/pools/${encodeURIComponent(pool)}/monitor` +
      `?query=${encodeURIComponent(query)}&routing=${encodeURIComponent(routing)}`);
    out.textContent = JSON.stringify(data.results, null, 2);
  } catch (e) {
    out.textContent = String(e.message || e);
  }
}

document.getElementById('run').onclick = runQuery;
poll(renderPools, 5000);
