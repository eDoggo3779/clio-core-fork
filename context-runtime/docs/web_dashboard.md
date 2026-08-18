# The web dashboard ("viz")

The runtime serves its own dashboard. The admin ChiMod starts one HTTP server per
node, every ChiMod may ship a `viz/` directory of HTML/CSS/JS, and any Container
may register HTTP routes. Nothing about it is collective: each node answers from
its own state, or forwards an explicitly-addressed query, so N nodes serve the
same UI without coordinating.

This replaces the need to run the separate Python `context-visualizer` process,
which is still present and still works — it talks to the runtime as an ordinary
client.

## Running it

```bash
clio_run runtime start                      # dashboard on http://127.0.0.1:8080
clio_run runtime start --viz-port 9000      # elsewhere
clio_run runtime start --viz-bind 0.0.0.0   # reachable off-box (see below)
clio_run runtime start --no-viz             # don't serve it
```

Or in `~/.clio/clio.yaml`, which overrides the CLI's default:

```yaml
viz:
  enabled: true
  port: 8080
  bind: "127.0.0.1"
  max_threads: 4
```

Or in the environment, which overrides the file: `CLIO_VIZ_ENABLE`,
`CLIO_VIZ_PORT` (0 = bind any free port), `CLIO_VIZ_BIND`,
`CLIO_VIZ_MAX_THREADS`, `CLIO_VIZ_PATH`.

Two defaults are deliberate:

- **Loopback only.** The dashboard exposes worker queues, pool layout and device
  inventory. Binding `0.0.0.0` publishes that to the network; prefer an SSH
  tunnel. There is no authentication.
- **Off for embedded runtimes.** A unit test, an adapter, or any process calling
  `CLIO_INIT` gets no listening socket unless it asks. `clio_run runtime start`
  is what turns the dashboard on by default, so a daemon has it and a library
  user does not.

A port that is already taken logs a warning and disables the dashboard; it never
fails the runtime.

## Where the pages come from

A ChiMod's assets are found **relative to the library the runtime actually
loaded**, which is what makes a built-but-not-installed tree and an installed
tree both work with no configuration:

| layout | libraries | assets |
| --- | --- | --- |
| built, not installed | `<build>/bin` | `<build>/bin/viz/<mod_name>` |
| installed | `<prefix>/lib` | `<prefix>/share/clio/viz/<mod_name>` |

`$CLIO_VIZ_PATH` (a `:`-separated list of viz roots, each holding one
subdirectory per ChiMod) is checked first, so you can edit pages against a
running daemon without rebuilding.

CMake puts them in both places from one call in the module's `CMakeLists.txt`:

```cmake
clio_add_viz_assets(MOD_NAME clio_bdev)   # defaults to ./viz
```

Assets mount at `/viz/<mod_name>/`, and `/` redirects to whichever module claimed
the home page (the admin ChiMod's dashboard shell).

## Adding a page to your ChiMod

1. Drop the files in `viz/` next to your module's sources and call
   `clio_add_viz_assets(MOD_NAME <your_mod_name>)`. Your page is now at
   `/viz/<your_mod_name>/index.html`; the admin shell's stylesheet and helpers
   are served from the same origin, so you can `<link>`/`<script>` them:

   ```html
   <link rel="stylesheet" href="/viz/clio_admin/css/style.css">
   <script src="/viz/clio_admin/js/api.js"></script>
   ```

2. If the data your page needs is already a `Monitor()` query your module
   answers, you need no C++ at all — fetch it through the generic forward:

   ```js
   const data = await API.get(`/api/pools/${poolId}/monitor?query=stats`);
   ```

3. For anything else, override `Container::RegisterViz`:

   ```cpp
   void Runtime::RegisterViz(clio::run::viz::VizServer &viz,
                             const std::string &mod_name) {
     viz.AddRoute({"GET", "/api/mod/" + mod_name + "/pools", mod_name,
                   "Block-device pools on this node",
                   [](const clio::run::viz::Request &req,
                      clio::run::viz::Response &resp) {
                     clio::run::viz::JsonWriter w;
                     w.BeginObject();
                     w.Field("hello", req.Param("who", "world"));
                     w.EndObject();
                     resp.Json(w.Str());
                   }});
   }
   ```

   `RegisterViz` is called once per container by
   `PoolManager::RegisterContainer`, right after the container is published and
   before its `Create` runs. Registration is idempotent (first `(method, path)`
   wins), so several containers — or several pools of the same ChiMod — are fine.

   Path segments of the form `{name}` bind into `Request::path_vars`.

   `clio_runtime/viz/viz_json.h` is a dependency-free JSON writer, so a module
   does not inherit the dashboard's HTTP dependencies to answer a route.

`GET /api/routes` lists every registered route and mounted directory, which is
how the Config page discovers which modules shipped a UI.

## What a handler may do

Handlers run on the dashboard's own HTTP thread pool, **not** on a runtime
worker. That means:

- **Blocking is allowed.** Reading manager state (`CLIO_POOL_MANAGER`,
  `CLIO_IPC`, `CLIO_WORK_ORCHESTRATOR`, `CLIO_CONFIG_MANAGER`) is fine, and so is
  submitting a task and waiting on its `Future` — the same thing a FUSE thread or
  a client process does. Always pass a timeout; the admin's own forwards use 5s
  so an unreachable peer cannot pin an HTTP thread.
- **A handler is not a coroutine.** There is no `co_await` here.
- **Handlers must be reentrant.** Several may run at once.

The server is stopped early in `RuntimeManager::ServerFinalize`, before the
workers stop, so no handler can be left waiting on a task that will never run.

## Endpoints the admin ChiMod registers

| route | what it answers |
| --- | --- |
| `GET /api/health` | liveness plus this node's identity |
| `GET /api/topology` | cluster membership and SWIM state, from the local host table |
| `GET /api/pools` | pools composed on this node |
| `GET /api/config` | the settings this daemon actually came up with |
| `GET /api/routes` | every route and asset mount, per ChiMod |
| `GET /api/nodes/{node}/workers` | per-worker queue depth, load, task counts |
| `GET /api/nodes/{node}/system_stats` | CPU/RAM/GPU samples newer than `?min_event_id` |
| `GET /api/nodes/{node}/containers` | containers and their learned task-cost models |
| `GET /api/nodes/{node}/bdevs` | block devices on the node |
| `GET /api/pools/{pool}/monitor` | forward `?query` to a pool's own `Monitor()` (`?routing=local\|broadcast\|…`) |

`{node}` is a node id or `local`. Addressing this node by its own id routes
locally, so the single-node case never touches the network.

All of them are read-only. Nothing here shuts a node down or mutates data.

## Build requirements

The HTTP transport is `Poco::Net`, linked PRIVATE into `clio_run_cxx` (Poco's
interface compile definitions collide with the POSIX interception adapters, so no
Poco type may appear in an installed header — the server lives behind a PIMPL in
`src/viz/viz_server.cc`). Without Poco the router still builds and the runtime
still starts; `VizServer::Start()` warns and the dashboard is simply absent
(`CLIO_RUN_HAS_POCO` is 0).

Tests: `ctest -R cr_viz_tests` covers the router directly (path patterns, MIME
types, JSON emission, asset resolution, the traversal guard, both asset layouts)
and then drives the real server over TCP, including a second ChiMod's page and
route appearing when its pool is composed.
