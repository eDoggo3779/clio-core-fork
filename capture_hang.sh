#!/bin/bash
# Capture diagnostic state when the CTE benchmark sweep hangs.
# Run this in a second terminal the moment progress stalls at iteration 6 (4k x 2t).
# Output goes to /workspace/hang_capture_<timestamp>/

set +e  # don't abort on individual command failures
TS=$(date +%Y%m%d_%H%M%S)
OUT=/workspace/hang_capture_$TS
mkdir -p "$OUT"
cd "$OUT"

echo "=== Capture started at $(date) ===" | tee capture.log
echo "Writing to $OUT" | tee -a capture.log

# --- Process inventory ---
echo "=== ps: runtime/cte/chimaera processes ===" > ps.txt
ps auxf | grep -E "wrp_runtime|wrp_cte|chimaera|jarvis" | grep -v grep >> ps.txt

echo "=== All processes owned by $USER ===" > ps_all.txt
ps -u "$USER" -o pid,ppid,state,stat,etime,time,comm,args >> ps_all.txt

# --- Find runtime PIDs ---
# The actual runtime binary is `chimaera` (launched via `chimaera runtime start`),
# not `wrp_runtime`. Match both to be safe, and also `chi_` workers.
RUNTIME_PIDS=$(pgrep -f "chimaera runtime|wrp_runtime|chi_worker")
BENCH_PIDS=$(pgrep -f wrp_cte_bench)
echo "Runtime PIDs: $RUNTIME_PIDS" | tee -a capture.log
echo "Benchmark PIDs: $BENCH_PIDS" | tee -a capture.log

# --- Per-process kernel state ---
for PID in $RUNTIME_PIDS $BENCH_PIDS; do
  if [ -d "/proc/$PID" ]; then
    echo "--- PID $PID ---" >> proc_state.txt
    cat /proc/$PID/status  >> proc_state.txt 2>&1
    echo "--- /proc/$PID/stack ---" >> proc_state.txt
    cat /proc/$PID/stack   >> proc_state.txt 2>&1
    echo "--- /proc/$PID/wchan ---" >> proc_state.txt
    cat /proc/$PID/wchan   >> proc_state.txt 2>&1; echo "" >> proc_state.txt
    echo "--- /proc/$PID/syscall ---" >> proc_state.txt
    cat /proc/$PID/syscall >> proc_state.txt 2>&1

    # Per-thread kernel stacks
    echo "--- per-thread stacks (PID $PID) ---" >> proc_state.txt
    for TID in /proc/$PID/task/*; do
      T=$(basename "$TID")
      echo "  TID $T state=$(cat $TID/status 2>/dev/null | grep ^State)" >> proc_state.txt
      echo "  stack:" >> proc_state.txt
      cat $TID/stack 2>/dev/null | sed 's/^/    /' >> proc_state.txt
    done
    echo "" >> proc_state.txt
  fi
done

# --- Userspace backtraces via gdb (most informative) ---
for PID in $RUNTIME_PIDS $BENCH_PIDS; do
  if [ -d "/proc/$PID" ]; then
    echo "=== gdb backtrace for PID $PID ===" > gdb_$PID.txt
    gdb -p "$PID" -batch \
        -ex "set pagination off" \
        -ex "thread apply all bt" \
        -ex "detach" \
        -ex "quit" >> gdb_$PID.txt 2>&1
  fi
done

# --- Shared memory ---
echo "=== /dev/shm contents ===" > shm.txt
ls -la /dev/shm/ >> shm.txt 2>&1
echo "" >> shm.txt
echo "=== /dev/shm entries matching chi/cte/wrp/hermes ===" >> shm.txt
ls -la /dev/shm/ | grep -iE "chi|cte|wrp|hermes|chimaera" >> shm.txt 2>&1

# --- SysV IPC (semaphores, shared memory, message queues) ---
echo "=== ipcs -a ===" > ipc.txt
ipcs -a >> ipc.txt 2>&1

# --- Open file descriptors on the runtime ---
for PID in $RUNTIME_PIDS; do
  if [ -d "/proc/$PID" ]; then
    echo "=== lsof for PID $PID ===" > lsof_$PID.txt
    lsof -p "$PID" >> lsof_$PID.txt 2>&1
  fi
done

# --- Network sockets in use (in case TCP/UDS state is stuck) ---
echo "=== ss -tnp (TCP) ===" > sockets.txt
ss -tnp 2>&1 | grep -E "wrp|chi|cte" >> sockets.txt
echo "=== ss -xp (Unix domain) ===" >> sockets.txt
ss -xp 2>&1 | grep -E "wrp|chi|cte" >> sockets.txt

# --- System load ---
echo "=== uptime / loadavg ===" > system.txt
uptime >> system.txt
cat /proc/loadavg >> system.txt
echo "" >> system.txt
echo "=== memory ===" >> system.txt
free -h >> system.txt
echo "" >> system.txt
echo "=== disk usage /workspace & /tmp ===" >> system.txt
df -h /workspace /tmp 2>&1 >> system.txt

# --- Jarvis state ---
echo "=== jarvis pipeline status ===" > jarvis.txt
jarvis ppl list >> jarvis.txt 2>&1

# --- Recent runtime logs (if accessible) ---
echo "=== tail of jarvis logs ===" > logs.txt
find /tmp -maxdepth 3 -name "*.log" -newer /proc/1 2>/dev/null | while read L; do
  echo "--- $L ---" >> logs.txt
  tail -200 "$L" >> logs.txt 2>&1
done

echo "=== Capture complete: $(date) ===" | tee -a capture.log
echo "Files in $OUT:" | tee -a capture.log
ls -la "$OUT" | tee -a capture.log
