// Block-device page. Two endpoints, both of them the module's own:
//   /api/mod/clio_bdev/pools           -- registered by bdev's RegisterViz()
//   /api/pools/<id>/monitor?query=stats -- reaches bdev's Monitor("stats")

const BDEV_TYPES = ['file', 'ram', 'hbm', 'pinned', 'noop', 's3', 'gcs'];
// A pool card navigated here with ?pool= -> focus on that device.
const FOCUS_POOL = poolParam();

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
  let pools = index.pools || [];
  document.getElementById('errors').innerHTML = '';

  // Focused view: the card the user clicked on the Pools tab.
  if (FOCUS_POOL) {
    pools = pools.filter((p) => p.pool_id === FOCUS_POOL);
    document.getElementById('focus-line').innerHTML = pools.length
      ? `showing pool ${esc(FOCUS_POOL)} — <a href="index.html">show all devices</a>`
      : `pool ${esc(FOCUS_POOL)} is not a bdev on this node — <a href="index.html">show all devices</a>`;
    document.getElementById('pred-heading').style.display = '';
    document.getElementById('pred-sub').style.display = '';
    await renderPredictions('predictions', FOCUS_POOL);
  }

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
    // Everything Monitor("stats") reports, minus the two device-health JSON
    // blobs (they are nested documents, not stats) and the fields the header
    // and meter already show.
    return `<div class="card">
      <h3>${esc(pool.pool_name)} <span class="badge">${esc(typeName(s.bdev_type))}</span></h3>
      <div class="sub">${esc(pool.pool_id)}</div>
      ${meter('capacity used', used)}
      <div class="sub">${bytes(s.remaining_capacity)} free of ${bytes(s.total_capacity)}</div>
      ${kvTable(s, ['pool_name', 'bdev_type', 'device_health', 'failure_prediction'])}
    </div>`;
  }));

  document.getElementById('devices').innerHTML = cards.join('');
}

poll(render, 3000);
