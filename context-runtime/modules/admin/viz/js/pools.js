// Pools composed on this node, the Add Pool flow, and a generic Monitor()
// explorer.
//
// Add Pool: the module list comes from /api/chimods. A module that registered
// the create convention (GET /api/mod/<mod>/create) supplies its own form spec,
// which we render field-by-field; validation and creation POST back to that
// same route, so the module owns what is acceptable. Modules without a form get
// the generic compose editor (identity fields + raw YAML), which reaches the
// same code path as `clio_run compose`.

let CHIMODS = [];
let CUR_SPEC = null;   // the selected module's form spec, or null for generic

async function post(path, fields) {
  const resp = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams(fields).toString(),
  });
  let body = {};
  try { body = await resp.json(); } catch (e) { /* non-JSON error */ }
  return { ok: resp.ok && body.ok !== false, body, status: resp.status };
}

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

async function renderModulePages() {
  const data = await API.get('/api/routes');
  const mounts = (data.mounts || []).filter((m) => m.mod_name !== 'clio_admin');
  document.getElementById('module-pages').innerHTML = mounts.length
    ? mounts.map((m) => `<div class="card clickable"
          onclick="location.href='${esc(m.url_prefix)}/index.html'">
        <h3>${esc(m.mod_name)}</h3>
        <div class="sub">${esc(m.url_prefix)}</div>
      </div>`).join('')
    : '<div class="empty">No module shipped a page (compose one of its pools first).</div>';
}

// ---- Add Pool flow ---------------------------------------------------------

function fieldHtml(f) {
  const id = `apf-${f.name}`;
  const label = `<label for="${id}">${esc(f.label || f.name)}${f.required ? ' *' : ''}</label>`;
  let input;
  if (f.type === 'select') {
    const opts = (f.options || []).map((o) =>
      `<option value="${esc(o)}" ${o === f.default ? 'selected' : ''}>${esc(o)}</option>`).join('');
    input = `<select id="${id}" data-field="${esc(f.name)}">${opts}</select>`;
  } else if (f.type === 'textarea') {
    input = `<textarea id="${id}" data-field="${esc(f.name)}" rows="6" cols="48"
      placeholder="${esc(f.placeholder || '')}">${esc(f.default || '')}</textarea>`;
  } else {
    input = `<input id="${id}" data-field="${esc(f.name)}" size="28"
      value="${esc(f.default || '')}" placeholder="${esc(f.placeholder || '')}">`;
  }
  const help = f.help ? `<span class="sub">${esc(f.help)}</span>` : '';
  return `<div class="row" style="margin:0.35rem 0;">${label}${input}${help}</div>`;
}

/** The generic compose editor, shown for modules without a create form. */
const GENERIC_FIELDS = [
  { name: 'pool_name', label: 'Pool name', type: 'text', required: true },
  { name: 'pool_id', label: 'Pool ID', type: 'text', required: true, help: 'major.minor' },
  { name: 'pool_query', label: 'Routing', type: 'select', options: ['local', 'dynamic', 'broadcast'], default: 'local' },
  { name: 'config', label: 'Module params (YAML)', type: 'textarea', placeholder: 'key: value' },
];

async function renderModuleForm() {
  const mod = document.getElementById('ap-module').value;
  const host = document.getElementById('ap-form');
  const info = CHIMODS.find((c) => c.name === mod);
  document.getElementById('ap-result').textContent = '';
  if (!mod) { host.innerHTML = ''; CUR_SPEC = null; return; }
  if (info && info.has_create) {
    try {
      CUR_SPEC = await API.get(`/api/mod/${encodeURIComponent(mod)}/create`);
      host.innerHTML =
        `<div class="sub">${esc(CUR_SPEC.title || mod)}${CUR_SPEC.note ? ' — ' + esc(CUR_SPEC.note) : ''}</div>`
        + (CUR_SPEC.fields || []).map(fieldHtml).join('');
      return;
    } catch (e) { /* fall through to generic */ }
  }
  CUR_SPEC = null;
  host.innerHTML =
    '<div class="sub">This module has no form; parameters go in as compose YAML.</div>'
    + GENERIC_FIELDS.map(fieldHtml).join('');
}

function collectFields() {
  const fields = {};
  document.querySelectorAll('#ap-form [data-field]').forEach((el) => {
    fields[el.dataset.field] = el.value;
  });
  return fields;
}

async function submitAddPool(action) {
  const mod = document.getElementById('ap-module').value;
  const out = document.getElementById('ap-result');
  if (!mod) { out.textContent = 'pick a module first'; return; }
  const fields = collectFields();
  fields.action = action;
  const path = CUR_SPEC
    ? `/api/mod/${encodeURIComponent(mod)}/create`
    : '/api/pools/compose';
  if (!CUR_SPEC) fields.mod_name = mod;
  out.textContent = action === 'validate' ? 'validating…' : 'creating…';
  const { ok, body, status } = await post(path, fields);
  if (!ok) {
    out.textContent = body.errors
      ? Object.entries(body.errors).map(([k, v]) => `${k}: ${v}`).join(' · ')
      : (body.error || `failed (${status})`);
    return;
  }
  if (action === 'validate') {
    out.textContent = 'valid ✓';
    return;
  }
  out.textContent = `created ${body.pool_name} (${body.pool_id})`;
  renderPools();
  renderModulePages();
}

async function openAddPool() {
  const panel = document.getElementById('add-pool-panel');
  panel.style.display = 'block';
  const data = await API.get('/api/chimods');
  CHIMODS = data.chimods || [];
  fillModuleSelect('');
  renderModuleForm();
}

function fillModuleSelect(filter) {
  const sel = document.getElementById('ap-module');
  const prev = sel.value;
  sel.innerHTML = '';
  CHIMODS
    .filter((c) => c.name.includes(filter))
    .forEach((c) => {
      const o = document.createElement('option');
      o.value = c.name;
      o.textContent = c.name + (c.has_create ? ' (form)' : '');
      sel.appendChild(o);
    });
  if ([...sel.options].some((o) => o.value === prev)) sel.value = prev;
}

document.getElementById('add-pool-btn').onclick = openAddPool;
document.getElementById('ap-cancel').onclick = () => {
  document.getElementById('add-pool-panel').style.display = 'none';
};
document.getElementById('ap-search').oninput = (e) => {
  fillModuleSelect(e.target.value.trim());
  renderModuleForm();
};
document.getElementById('ap-module').onchange = renderModuleForm;
document.getElementById('ap-validate').onclick = () => submitAddPool('validate');
document.getElementById('ap-create').onclick = () => submitAddPool('create');

// ---- Monitor explorer -------------------------------------------------------

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
poll(async () => {
  await renderPools();
  await renderModulePages();
}, 5000);
