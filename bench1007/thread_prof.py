#!/usr/bin/env python3
"""Time-sliced thread profile: per-250ms total CPU, and per-thread CPU +
voluntary context switches (park events) over the run's PEAK window.
Usage: thread_prof.py <cmd...>"""
import os, subprocess, sys, time, collections

HZ = os.sysconf('SC_CLK_TCK')

def snap(pid):
    out = {}
    try:
        for tid in os.listdir(f'/proc/{pid}/task'):
            try:
                with open(f'/proc/{pid}/task/{tid}/stat') as f:
                    s = f.read()
                name = s[s.index('(') + 1:s.rindex(')')]
                rest = s[s.rindex(')') + 2:].split()
                ticks = int(rest[11]) + int(rest[12])
                vcs = nvcs = 0
                with open(f'/proc/{pid}/task/{tid}/status') as f:
                    for line in f:
                        if line.startswith('voluntary_ctxt'):
                            vcs = int(line.split()[-1])
                        elif line.startswith('nonvoluntary_ctxt'):
                            nvcs = int(line.split()[-1])
                out[int(tid)] = (name, ticks, vcs, nvcs)
            except OSError:
                pass
    except OSError:
        pass
    return out

p = subprocess.Popen(sys.argv[1:], stdout=subprocess.PIPE,
                     stderr=subprocess.STDOUT, text=True)
t0 = time.time()
series = []  # (t, {tid: (name, ticks, vcs, nvcs)})
while p.poll() is None:
    series.append((time.time() - t0, snap(p.pid)))
    time.sleep(0.1)
out = p.stdout.read()

# Per-slice total CPU (cores) to locate the benchmark's hot window.
print(f"CPUs {os.cpu_count()}  samples {len(series)}")
slices = []
for i in range(1, len(series)):
    dt = series[i][0] - series[i - 1][0]
    prev, cur = series[i - 1][1], series[i][1]
    d = sum(cur[t][1] - prev[t][1] for t in cur if t in prev) / HZ
    slices.append((series[i][0], d / dt if dt > 0 else 0))
print("UTIL_SLICES " + " ".join(f"{ts:.1f}s:{u:.2f}" for ts, u in slices))

# Peak window = contiguous slices with utilization >= 60% of max.
if slices:
    peak = max(u for _, u in slices)
    hot = [i for i, (_, u) in enumerate(slices) if u >= 0.6 * peak]
    lo, hi = hot[0], hot[-1] + 1
    a, b = series[lo][1], series[min(hi, len(series) - 1)][1]
    span = series[min(hi, len(series) - 1)][0] - series[lo][0]
    print(f"PEAK_WINDOW {series[lo][0]:.1f}s..{series[min(hi,len(series)-1)][0]:.1f}s ({span:.2f}s)")
    rows = []
    for t in b:
        if t not in a:
            continue
        name = b[t][0]
        cpu = (b[t][1] - a[t][1]) / HZ
        vcs = b[t][2] - a[t][2]
        nvcs = b[t][3] - a[t][3]
        rows.append((cpu, vcs, nvcs, t, name))
    rows.sort(reverse=True)
    print(f"{'cpu_s':>7} {'cores':>6} {'vol_cs':>9} {'nonvol':>7}  tid name")
    for cpu, vcs, nvcs, t, name in rows[:20]:
        print(f"{cpu:7.2f} {cpu/span:6.2f} {vcs:9d} {nvcs:7d}  {t} {name}")
    print(f"TOTAL cpu {sum(r[0] for r in rows):.2f}s over {span:.2f}s = "
          f"{sum(r[0] for r in rows)/span:.2f} cores; vol_cs {sum(r[1] for r in rows)}")
for line in out.splitlines():
    if 'Time (avg)' in line or 'Aggregate IOPS' in line:
        print(line.split('PrintResults')[-1].strip())
