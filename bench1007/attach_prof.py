#!/usr/bin/env python3
"""Attach-mode thread profile: sample a RUNNING pid for N seconds; report
per-thread CPU + voluntary context switches. Usage: attach_prof.py <pid> [secs]"""
import os, sys, time

HZ = os.sysconf('SC_CLK_TCK')
pid = sys.argv[1]
secs = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0

def snap():
    out = {}
    try:
        for tid in os.listdir(f'/proc/{pid}/task'):
            try:
                with open(f'/proc/{pid}/task/{tid}/stat') as f:
                    s = f.read()
                name = s[s.index('(') + 1:s.rindex(')')]
                rest = s[s.rindex(')') + 2:].split()
                ticks = int(rest[11]) + int(rest[12])
                vcs = 0
                with open(f'/proc/{pid}/task/{tid}/status') as f2:
                    for line in f2:
                        if line.startswith('voluntary_ctxt'):
                            vcs = int(line.split()[-1])
                            break
                out[tid] = (name, ticks, vcs)
            except OSError:
                pass
    except OSError:
        pass
    return out

a = snap()
time.sleep(secs)
b = snap()
rows = []
for t in b:
    if t in a:
        rows.append(((b[t][1] - a[t][1]) / HZ, b[t][2] - a[t][2], t, b[t][0]))
rows.sort(reverse=True)
print(f"pid {pid} over {secs}s:")
print(f"{'cpu_s':>7} {'cores':>6} {'vol_cs':>8} {'cs/s':>7}  tid name")
tot = 0.0
for cpu, vcs, t, name in rows:
    tot += cpu
    if cpu > 0.02 or vcs > 50:
        print(f"{cpu:7.2f} {cpu/secs:6.2f} {vcs:8d} {vcs/secs:7.0f}  {t} {name}")
print(f"TOTAL {tot:.2f}s = {tot/secs:.2f} cores")
