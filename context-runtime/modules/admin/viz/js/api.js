// Shared helpers for the Clio dashboard pages.
//
// Every page is plain static HTML served by the daemon and driven by fetch()
// against the routes the admin ChiMod registered (see admin_viz.cc). Keeping the
// API surface in one place means a ChiMod that adds its own page can reuse it.

const API = {
  /** GET a dashboard route and parse its JSON body. Throws on HTTP errors with
   *  the server's own {"error": ...} message when there is one. */
  async get(path) {
    const resp = await fetch(path, { headers: { Accept: 'application/json' } });
    let body = null;
    try {
      body = await resp.json();
    } catch (e) {
      throw new Error(`${path}: ${resp.status} ${resp.statusText}`);
    }
    if (!resp.ok) {
      throw new Error(body && body.error ? body.error
                                        : `${path}: ${resp.status}`);
    }
    return body;
  },
};

/** Reflect connection health in the navbar. */
function setStatus(text, cls) {
  const el = document.getElementById('conn-status');
  if (!el) return;
  el.textContent = text;
  el.className = 'nav-status' + (cls ? ' ' + cls : '');
}

/** Render an error banner into `containerId`, replacing any previous one. */
function showError(containerId, message) {
  const host = document.getElementById(containerId);
  if (!host) return;
  host.innerHTML = '';
  const div = document.createElement('div');
  div.className = 'error';
  div.textContent = message;
  host.appendChild(div);
}

/** Escape text for innerHTML use. */
function esc(value) {
  const div = document.createElement('div');
  div.textContent = value === undefined || value === null ? '' : String(value);
  return div.innerHTML;
}

/** Human-readable byte count. */
function bytes(n) {
  if (n === undefined || n === null || isNaN(n)) return '-';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let v = Number(n);
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

/** Round to at most `digits` decimals, tolerating null. */
function num(v, digits = 1) {
  if (v === undefined || v === null || isNaN(v)) return '-';
  return Number(v).toFixed(digits);
}

/** A labelled percentage meter, coloured by how hot the value is. */
function meter(label, pct) {
  const value = Math.max(0, Math.min(100, Number(pct) || 0));
  const cls = value > 85 ? 'hot' : value > 60 ? 'warm' : '';
  return `<div class="meter">
    <div class="meter-label"><span>${esc(label)}</span><span>${num(value)}%</span></div>
    <div class="meter-bar"><div class="meter-fill ${cls}" style="width:${value}%"></div></div>
  </div>`;
}

/** Build a table from an array of objects, using `columns` = [[key, label, fmt]]. */
function table(rows, columns) {
  if (!rows || rows.length === 0) {
    return '<div class="empty">Nothing to show.</div>';
  }
  const head = columns.map(([, label]) => `<th>${esc(label)}</th>`).join('');
  const body = rows.map((row) => {
    const cells = columns.map(([key, , fmt]) => {
      const raw = row[key];
      const text = fmt ? fmt(raw, row) : esc(raw);
      const cls = typeof raw === 'number' ? ' class="num"' : '';
      return `<td${cls}>${text}</td>`;
    }).join('');
    return `<tr>${cells}</tr>`;
  }).join('');
  return `<div class="table-wrap"><table><thead><tr>${head}</tr></thead>
          <tbody>${body}</tbody></table></div>`;
}

/** A minimal inline-SVG sparkline: no charting library, so pages render on a
 *  node with no network access. */
function sparkline(values) {
  const vals = (values || []).filter((v) => v !== null && !isNaN(v));
  if (vals.length < 2) return '<div class="empty">Not enough samples yet.</div>';
  const w = 300;
  const h = 40;
  const max = Math.max(...vals, 1);
  const step = w / (vals.length - 1);
  const points = vals.map((v, i) => {
    const x = (i * step).toFixed(1);
    const y = (h - (v / max) * (h - 4) - 2).toFixed(1);
    return `${i === 0 ? 'M' : 'L'}${x},${y}`;
  }).join(' ');
  return `<svg class="spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
            <rect width="${w}" height="${h}" rx="3"/><path d="${points}"/>
          </svg>`;
}

/** Mark the current page's nav link active. */
function markNav() {
  const here = window.location.pathname.split('/').pop() || 'index.html';
  document.querySelectorAll('.nav-links a').forEach((a) => {
    if (a.getAttribute('href') === here) a.classList.add('active');
  });
}

/** Run `fn` now and every `ms`, keeping the navbar status in sync. */
function poll(fn, ms) {
  const tick = async () => {
    try {
      await fn();
      setStatus('connected', 'ok');
    } catch (e) {
      setStatus(String(e.message || e), 'bad');
    }
  };
  tick();
  return setInterval(tick, ms);
}

document.addEventListener('DOMContentLoaded', markNav);
