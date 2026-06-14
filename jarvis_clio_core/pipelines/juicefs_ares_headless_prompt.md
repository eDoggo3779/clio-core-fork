# Headless agent prompt — install, run & validate the JuiceFS fio sweep on Ares

> Paste everything below the line into the headless Claude agent running on Ares
> (tmux pane, git branch `clio-cloud`, repo `~/clio-core-fork`).

---

You are an autonomous engineering agent running **headless on Ares**, in a tmux pane, inside
the repo `~/clio-core-fork` on git branch **`clio-cloud`**. You have no human to fall back on
mid-run for *environment* problems — diagnose those from ground truth and keep going. Work
methodically and narrate what you find.

## Mission

Stand up **JuiceFS** (Redis metadata + local-file object store) and run the **single-node
fio performance sweep** end-to-end via Jarvis, producing a fully-populated `results.csv`:

- knobs: **I/O size** {4k, 16k, 64k, 128k} × **threads** {1, 2, 4, 8} × **3 repeats** = **48 runs**
- 60 s per run (`fio --runtime=60 --time_based`)
- finish line: `${HOME}/juicefs_results/results.csv` with **48 data rows, all `status=success`**,
  and non-empty bandwidth / IOPS / latency columns whose values are logical (even if the
  performance shape is surprising — logical-but-unexpected numbers are acceptable; empty cells,
  NaN, or all-zero rows are not).

## The autonomy boundary (read carefully)

- **Ares runtime/environment issues are yours to fix, fully autonomously.** Installing fio and
  the JuiceFS binary, redis availability, FUSE (`/dev/fuse`) access, Slurm allocation, PATH,
  `$HOME`/`$USER` resolution, mount/unmount, stale mounts or redis ports, output dirs — own all
  of it and drive to completion.
- **Package-code issues are the escalation boundary.** The three deliverables under test —
  `jarvis_clio_core/jarvis_clio_core/juicefs/pkg.py`, `.../juicefs_bench/pkg.py`, and
  `jarvis_clio_core/pipelines/juicefs_fio_1n.yaml` — are the artifacts being validated. If you
  determine the blocker is a genuine **bug in this package code or the pipeline YAML** (not the
  environment), do **not** redesign or rewrite it. Apply only the **minimal** change needed to
  proceed if one is obvious, leave it **uncommitted**, and **clearly flag it in your final
  report** as a code issue the human must review. Prefer to stop and report a package-code bug
  over churning on it. Never commit or push.

## Required reading (first)

In `~/clio-core-fork`: `project_info.md` (project identity; JuiceFS is Phase 1 of "Bringing
CLIO to the Cloud") and `AGENTS.md` (binding build/run rules; Python style for any edits).
Then read the three deliverable files listed above so you understand the data flow:

- `juicefs/pkg.py` — a Jarvis Service: `format` (storage=file) + `mount --background` in
  `start()`, readiness poll, `umount` in `stop()`. Modeled on `jarvis_clio_core/redis/pkg.py`.
- `juicefs_bench/pkg.py` — runs fio against the mount, captures **JSON** to
  `<shared_dir>/fio_output.json`, and in `_get_stat()` re-reads that file to populate
  `{pkg_id}.{op}.{metric}` columns. (It reads a file, not in-memory output, because the sweep
  runner reloads a *fresh* package instance before calling `_get_stat` — the on-disk JSON is
  the contract.)
- `juicefs_fio_1n.yaml` — the 48-run sweep (redis → juicefs → juicefs_bench; `vars:`+`loop:`,
  `repeat: 3`).

## Environment ground truth (verify, don't assume)

- **Conda:** `source ~/miniconda3/etc/profile.d/conda.sh && conda activate iowarp` (owns Jarvis
  + the IOWarp Python stack). Confirm the exact path; adjust if miniconda lives elsewhere.
- **Jarvis:** `~/jarvis-cd`, editable-installed into `iowarp`. The sweep is launched with
  `jarvis ppl run yaml <file>`, which **auto-detects** `vars:`+`loop:` and runs the grid,
  writing `${output}/results.csv`. Confirm this against the installed source
  (`jarvis_cd/core/pipeline_test.py` → `is_pipeline_test`/`run_yaml_auto`, wired from
  `jarvis_cd/core/cli.py` `ppl_run`). **No `scheduler:` block / no `ppl submit` is used** —
  this is a single-node run, so the `dev` branch is *not* required, but the auto-detect path
  must exist in the installed Jarvis. If `jarvis ppl run yaml` does not trigger sweep mode,
  that's an environment/version issue — resolve it (e.g. check out the Jarvis revision that has
  `run_yaml_auto`).
- **Repo path:** `~` may resolve to `/mnt/common/$USER` while conda lives under `/home/$USER`
  (same NFS home). Be alert to `$HOME`/`$USER` mismatches — they matter for JuiceFS data/cache
  paths and the redis socket.
- **Slurm partition:** `compute`.

## Step 1 — Safe environment

1. **Allocation.** Grab a compute node with generous time — the sweep alone is ~48 × (≈60 s run
   + mount/format/redis overhead) ≈ **2 h healthy**, and you'll be installing tools and possibly
   debugging. `salloc -p compute -N 1 -t 04:30:00` (go higher if the queue allows). Do **all**
   install/run work inside the allocation so the mount, redis, and fio share one node.
2. **FUSE.** JuiceFS mounts via FUSE — confirm `/dev/fuse` exists and is usable on the compute
   node (`ls -l /dev/fuse`; a quick `fusermount -V`). If FUSE user-mounts are restricted, that's
   an environment blocker to resolve (different node, module, or `fusermount3` path).
3. **Stale state.** Before each (re)launch: unmount any leftover JuiceFS mount
   (`fusermount -u ${HOME}/juicefs_mnt 2>/dev/null`), kill stray `redis-server` on the chosen
   port, and `ssh-keygen -R localhost 2>/dev/null` if any ssh-localhost path is used.

## Step 2 — Install tools (environment — your job, autonomous)

Run `jarvis_clio_core/scripts/setup_juicefs_ares.sh` if present; otherwise do it by hand and
make the script work:

- **fio:** prefer `conda install -y -c conda-forge fio` into `iowarp` (or `module load fio` if
  Ares provides it). Confirm `fio --version` and that it supports `--output-format=json`.
- **JuiceFS:** download a pinned release binary to `${HOME}/.local/bin/juicefs` (no sudo on
  Ares). E.g. fetch the `juicefs-<ver>-linux-amd64.tar.gz` from the GitHub releases of
  `juicedata/juicefs`, extract the `juicefs` binary, `chmod +x`, and put `${HOME}/.local/bin`
  on `PATH`. Confirm `juicefs version`. (If Ares has no outbound network, fetch on the login
  node / via the same channel used for other deps, then stage the binary onto the node.)
- **redis:** confirm `redis-server` and `redis-cli` are on `PATH` (conda or module). The
  pipeline starts/stops redis itself per run.

Print all three versions at the end so the log shows the toolchain.

## Step 3 — Register the repo & smoke-test the mount manually

1. `jarvis repo add ~/clio-core-fork/jarvis_clio_core` (if not already registered — check
   `jarvis repo list`). Confirm `jarvis_clio_core.juicefs` and `jarvis_clio_core.juicefs_bench`
   resolve.
2. **Manual mount smoke test before the sweep** (fast way to catch env issues): start a redis,
   `juicefs format --storage file --bucket ${HOME}/juicefs_data redis://127.0.0.1:6379/1 jfsbench`,
   `juicefs mount redis://127.0.0.1:6379/1 ${HOME}/juicefs_mnt --background`, confirm
   `mountpoint ${HOME}/juicefs_mnt`, write+read a file, then a 5-second fio:
   `fio --name=t --directory=${HOME}/juicefs_mnt --rw=write --bs=64k --numjobs=2 --thread
   --runtime=5 --time_based --size=256m --ioengine=psync --direct=0 --group_reporting
   --output-format=json --output=/tmp/fio_t.json` and confirm the JSON has
   `jobs[0].write.bw`/`.iops`/`.lat_ns.mean`. Unmount and shut redis down. If this all works,
   the sweep will too.

## Step 4 — Run the sweep to completion

Start clean, then loop until all 48 rows are green:

```bash
rm -rf ${HOME}/juicefs_results
while :; do
  n=$(grep -c success ${HOME}/juicefs_results/results.csv 2>/dev/null || echo 0)
  echo "have $n / 48 successful rows"
  [ "$n" -ge 48 ] && break
  fusermount -u ${HOME}/juicefs_mnt 2>/dev/null
  pkill -f 'redis-server .*6379' 2>/dev/null
  jarvis ppl run yaml ~/clio-core-fork/jarvis_clio_core/pipelines/juicefs_fio_1n.yaml
done
```

(Adapt to how the runner handles re-runs — overwrite vs resume is fine; the point is **keep
going until `grep -c success` returns 48**.) Watch remaining allocation time
(`squeue -u $USER`); if within ~30 min of expiry and incomplete, grab a fresh allocation and
continue rather than letting the node die mid-sweep.

## Step 5 — Validate the CSV

Inspect `${HOME}/juicefs_results/results.csv`:

- **Exactly 48 data rows** (49 lines incl. header), all `status=success`.
- Columns `juicefs_bench.write.{agg_bw_mbps, iops, lat_mean_us, lat_p99_us, total_io_mb}` (op
  label may be `write` or `read` per the pipeline's `mode`) present and **non-empty**.
- Values **logical**: bandwidth rises with io_size; IOPS higher at small io; metrics scale
  sanely with thread count; the 3 repeats per combo are in the same ballpark; no NaN / all-zero
  rows. Logical-but-unexpected is acceptable.

If a column is empty, the gap is almost always (i) `<shared_dir>/fio_output.json` not being
written/located, or (ii) a key mismatch between fio's JSON schema and `_get_stat`'s lookups —
those are **package-code** issues: diagnose, apply a minimal uncommitted fix if obvious, and
**flag for human review** per the autonomy boundary.

## Constraints & reporting

- **Do not commit or push.** Leave any code fixes uncommitted; the human decides what lands.
- Follow `AGENTS.md` style for any Python you touch.
- Keep a running pane log of what you tried, the evidence, and what you changed. Final summary:
  (1) the toolchain you installed (fio/juicefs/redis versions), (2) any **environment** problems
  fixed, (3) any **package-code** issues found — with the minimal diff and why it's a code bug,
  flagged for review, (4) the CSV verdict (row count, all-success?, columns populated?, values
  logical?) with a few representative rows quoted.

Begin: read `project_info.md` + `AGENTS.md`, activate the env, confirm `jarvis ppl run yaml`
triggers sweep mode, grab a >4 h allocation, install the tools, run the manual mount smoke
test, then drive the completion loop until `grep -c success` returns **48**.
