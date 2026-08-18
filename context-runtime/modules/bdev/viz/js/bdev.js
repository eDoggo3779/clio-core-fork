// Block-device page. Two endpoints, both of them the module's own:
//   /api/mod/clio_bdev/pools           -- registered by bdev's RegisterViz()
//   /api/pools/<id>/monitor?query=stats -- reaches bdev's Monitor("stats")

const BDEV_TYPES = ['file', 'ram', 'hbm', 'pinned', 'noop', 's3', 'gcs'];

function typeName(v) {
  return BDEV_TYPES[v] !== undefined ? BDEV_TYPES[v] : `type ${v}`;
}

async function statsFor(poolId) {
  const data = await API.get(
    `/api/pools/${encodeURIComponent(poolId)}/monitor?query=stats&routing=local`);
  // One container per bdev pool per node; take whichever answered.
  const values = Object.values(data.results || {}).filter((v) => v);
  return values.length ? values[0] : null;
}

async function render() {
  const index = await API.get('/api/mod/clio_bdev/pools');
  const pools = index.pools || [];
  document.getElementById('errors').innerHTML = '';
  if (pools.length === 0) {
    document.getElementById('devices').innerHTML =
      '<div class="empty">No block-device pools on this node.</div>';
    return;
  }

  const cards = await Promise.all(pools.map(async (pool) => {
    let s = null;
    let error = '';
    try {
      s = await statsFor(pool.pool_id);
    } catch (e) {
      error = String(e.message || e);
    }
    if (!s) {
      return `<div class="card"><h3>${esc(pool.pool_name)}</h3>
        <div class="sub">${esc(pool.pool_id)}</div>
        <div class="empty">${esc(error || 'no stats')}</div></div>`;
    }
    const used = s.total_capacity
      ? ((s.total_capacity - s.remaining_capacity) / s.total_capacity) * 100
      : 0;
    return `<div class="card">
      <h3>${esc(pool.pool_name)} <span class="badge">${esc(typeName(s.bdev_type))}</span></h3>
      <div class="sub">${esc(pool.pool_id)}</div>
      ${meter('capacity used', used)}
      <div class="sub">${bytes(s.remaining_capacity)} free of ${bytes(s.total_capacity)}</div>
      <div class="table-wrap"><table><tbody>
        <tr><td>read</td><td class="num">${num(s.read_bandwidth_mbps)} MB/s</td></tr>
        <tr><td>write</td><td class="num">${num(s.write_bandwidth_mbps)} MB/s</td></tr>
        <tr><td>read latency</td><td class="num">${num(s.read_latency_us)} µs</td></tr>
        <tr><td>write latency</td><td class="num">${num(s.write_latency_us)} µs</td></tr>
        <tr><td>reads</td><td class="num">${esc(s.total_reads)}</td></tr>
        <tr><td>writes</td><td class="num">${esc(s.total_writes)}</td></tr>
        <tr><td>bytes read</td><td class="num">${bytes(s.total_bytes_read)}</td></tr>
        <tr><td>bytes written</td><td class="num">${bytes(s.total_bytes_written)}</td></tr>
      </tbody></table></div>
    </div>`;
  }));

  document.getElementById('devices').innerHTML = cards.join('');
}

poll(render, 3000);
