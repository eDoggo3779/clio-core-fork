from jarvis_cd.core.pkg import Application
from jarvis_cd.shell import Exec, PsshExecInfo
import os
import re
import time as time_mod


class WrpCteBench(Application):
    """
    CTE Core Benchmark Application

    Runs Put, Get, and PutGet benchmarks against a running CTE instance
    and collects throughput and latency statistics.

    The benchmark measures:
    - Put operations: Write data to CTE
    - Get operations: Read data from CTE
    - PutGet operations: Combined write and read operations
    """

    def _init(self):
        self.benchmark_executable = 'wrp_cte_bench'
        self.output_file = None
        self.start_time = None

    def _configure_menu(self):
        return [
            {
                'name': 'test_case',
                'msg': 'Benchmark test case to run',
                'type': str,
                'choices': ['Put', 'Get', 'PutGet'],
                'default': 'Put',
            },
            {
                'name': 'num_threads',
                'msg': 'Number of worker threads',
                'type': int,
                'default': 4,
            },
            {
                'name': 'depth',
                'msg': 'Number of async requests per thread',
                'type': int,
                'default': 4,
            },
            {
                'name': 'io_size',
                'msg': 'Size of I/O operations',
                'type': str,
                'default': '1m',
            },
            {
                'name': 'io_count',
                'msg': 'Number of I/O operations per thread',
                'type': int,
                'default': 100,
            },
            {
                'name': 'nprocs',
                'msg': 'Number of MPI processes',
                'type': int,
                'default': 1,
            },
            {
                'name': 'ppn',
                'msg': 'Processes per node',
                'type': int,
                'default': 1,
            },
            {
                'name': 'init_runtime',
                'msg': 'Initialize Chimaera runtime',
                'type': bool,
                'default': False,
            },
        ]

    def _configure(self, **kwargs):
        self.log("Configuring CTE benchmark application...")

        if self.config['init_runtime']:
            self.setenv('CHI_WITH_RUNTIME', '1')
        else:
            self.setenv('CHI_WITH_RUNTIME', '0')

        self.log("CTE benchmark configuration completed successfully")

    def start(self):
        t0 = time_mod.time()

        self.log(f"Starting CTE benchmark: {self.config['test_case']}")

        cmd = [
            self.benchmark_executable,
            self.config['test_case'],
            str(self.config['num_threads']),
            str(self.config['depth']),
            str(self.config['io_size']),
            str(self.config['io_count'])
        ]

        cmd_str = ' '.join(cmd)
        output_path = os.path.join(self.shared_dir, 'bench_output.txt')
        self.log(f"Executing: {cmd_str}")

        if self.config['nprocs'] > 1:
            exec_info = PsshExecInfo(
                env=self.mod_env,
                hostfile=self.hostfile,
                nprocs=self.config['nprocs'],
                ppn=self.config['ppn']
            )
        else:
            from jarvis_cd.shell import LocalExecInfo
            exec_info = LocalExecInfo(
                env=self.mod_env,
                pipe_stdout=output_path,
            )

        Exec(cmd_str, exec_info).run()

        self.start_time = time_mod.time() - t0
        self.log(f"Benchmark completed in {self.start_time:.2f}s")

    def stop(self):
        pass

    def clean(self):
        path = os.path.join(self.shared_dir, 'bench_output.txt')
        if os.path.exists(path):
            os.remove(path)

    # ------------------------------------------------------------------
    # Statistics collection
    # ------------------------------------------------------------------

    def _get_stat(self, stat_dict):
        """Parse benchmark output and populate stat_dict."""
        output_path = os.path.join(self.shared_dir, 'bench_output.txt')
        if not os.path.exists(output_path):
            self.log(f'No output file found at {output_path}')
            return

        with open(output_path, 'r') as f:
            output = f.read()

        self._parse_output(output, stat_dict)

    def _parse_output(self, output, stat_dict):
        """Extract metrics from wrp_cte_bench stdout.

        The C++ binary outputs via HLOG which prefixes each line with
        ``<file>:<line> INFO <tid> <func> <message>``.
        We match against the message portion.
        """
        # Detect which test case header appeared (Put, Get, or PutGet)
        header_match = re.search(
            r'=== (\w+) Benchmark Results ===', output)
        operation = header_match.group(1).lower() if header_match else 'unknown'

        patterns = {
            'time_min_us': r'Time \(min\):\s+([\d.]+)\s+us',
            'time_max_us': r'Time \(max\):\s+([\d.]+)\s+us',
            'time_avg_us': r'Time \(avg\):\s+([\d.e+\-]+)\s+us',
            'bw_per_thread_min_mbps': r'Bandwidth per thread \(min\):\s+([\d.e+\-]+)\s+MB/s',
            'bw_per_thread_max_mbps': r'Bandwidth per thread \(max\):\s+([\d.e+\-]+)\s+MB/s',
            'bw_per_thread_avg_mbps': r'Bandwidth per thread \(avg\):\s+([\d.e+\-]+)\s+MB/s',
            'agg_bw_mbps': r'Aggregate bandwidth:\s+([\d.e+\-]+)\s+MB/s',
        }

        for metric, pattern in patterns.items():
            match = re.search(pattern, output)
            if match:
                stat_dict[f'{self.pkg_id}.{operation}.{metric}'] = float(
                    match.group(1))

    # ------------------------------------------------------------------
    # Plotting
    # ------------------------------------------------------------------

    def _plot(self, results_dir):
        """Generate performance plots from pipeline results.csv.

        Produces four PNG figures:
          1. Aggregate Bandwidth (MB/s) vs I/O Size — grouped by test case
          2. Average Latency (us) vs I/O Size — grouped by test case
          3. Aggregate Bandwidth (MB/s) vs Num Threads
          4. Average Latency (us) vs Num Threads
        """
        import csv
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
        except ImportError:
            self.log('matplotlib not available — skipping plots')
            return

        csv_path = os.path.join(results_dir, 'results.csv')
        if not os.path.exists(csv_path):
            self.log(f'No results.csv found at {csv_path}')
            return

        rows = []
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)

        if not rows:
            self.log('results.csv is empty')
            return

        # Detect the pkg_prefix used in stat column names.
        # Columns look like: <prefix>.<operation>.<metric>
        pkg_prefix = None
        for op in ('put', 'get', 'putget'):
            for key in rows[0]:
                if key.endswith(f'.{op}.agg_bw_mbps'):
                    pkg_prefix = key.rsplit(f'.{op}.agg_bw_mbps', 1)[0]
                    break
            if pkg_prefix:
                break

        if pkg_prefix is None:
            self.log('Could not identify stat columns in results.csv')
            return

        def _col(op, metric):
            return f'{pkg_prefix}.{op}.{metric}'

        def _safe_float(row, col):
            val = row.get(col, '')
            if val == '':
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # Detect sweep-variable column names
        io_size_col = None
        threads_col = None
        for key in rows[0]:
            if key.endswith('.io_size'):
                io_size_col = key
            if key.endswith('.num_threads'):
                threads_col = key

        # Determine which operations actually have data
        operations = []
        op_labels = {'put': 'Put', 'get': 'Get', 'putget': 'PutGet'}
        op_colours = {'put': '#2196F3', 'get': '#FF9800', 'putget': '#4CAF50'}
        op_markers = {'put': 'o', 'get': 's', 'putget': '^'}
        for op in ('put', 'get', 'putget'):
            if any(_safe_float(r, _col(op, 'agg_bw_mbps')) is not None
                   for r in rows):
                operations.append(op)

        if not operations:
            self.log('No benchmark data found in results.csv')
            return

        # --- Helper: aggregate rows by a sweep variable ---
        def _aggregate(rows, sweep_col, op, metric):
            from collections import defaultdict
            buckets = defaultdict(list)
            for row in rows:
                x = row.get(sweep_col, '')
                y = _safe_float(row, _col(op, metric))
                if x != '' and y is not None:
                    buckets[x].append(y)
            # Sort numerically if possible, otherwise lexicographically
            def sort_key(k):
                try:
                    return float(k)
                except ValueError:
                    return k
            xs = sorted(buckets.keys(), key=sort_key)
            ys = [sum(buckets[x]) / len(buckets[x]) for x in xs]
            return xs, ys

        # --- Figure 1: Aggregate BW vs I/O Size ---
        if io_size_col:
            fig, ax = plt.subplots(figsize=(8, 5))
            for op in operations:
                xs, ys = _aggregate(rows, io_size_col, op, 'agg_bw_mbps')
                if xs:
                    ax.bar(xs, ys, alpha=0.7, color=op_colours[op],
                           label=op_labels[op], width=0.25)
            ax.set_xlabel('I/O Size')
            ax.set_ylabel('Aggregate Bandwidth (MB/s)')
            ax.set_title('CTE Bandwidth vs I/O Size')
            ax.legend()
            fig.tight_layout()
            fig.savefig(os.path.join(results_dir, 'bw_vs_io_size.png'),
                        dpi=150)
            plt.close(fig)

        # --- Figure 2: Avg Latency vs I/O Size ---
        if io_size_col:
            fig, ax = plt.subplots(figsize=(8, 5))
            for op in operations:
                xs, ys = _aggregate(rows, io_size_col, op, 'time_avg_us')
                if xs:
                    ax.bar(xs, ys, alpha=0.7, color=op_colours[op],
                           label=op_labels[op], width=0.25)
            ax.set_xlabel('I/O Size')
            ax.set_ylabel('Average Latency (us)')
            ax.set_title('CTE Latency vs I/O Size')
            ax.legend()
            fig.tight_layout()
            fig.savefig(os.path.join(results_dir, 'latency_vs_io_size.png'),
                        dpi=150)
            plt.close(fig)

        # --- Figure 3: Aggregate BW vs Num Threads ---
        if threads_col:
            fig, ax = plt.subplots(figsize=(8, 5))
            for op in operations:
                xs, ys = _aggregate(rows, threads_col, op, 'agg_bw_mbps')
                if xs:
                    try:
                        x_nums = [float(x) for x in xs]
                    except ValueError:
                        x_nums = list(range(len(xs)))
                    ax.plot(x_nums, ys, marker=op_markers[op],
                            color=op_colours[op], label=op_labels[op],
                            linewidth=2)
            ax.set_xlabel('Number of Threads')
            ax.set_ylabel('Aggregate Bandwidth (MB/s)')
            ax.set_title('CTE Bandwidth vs Thread Count')
            ax.legend()
            fig.tight_layout()
            fig.savefig(os.path.join(results_dir, 'bw_vs_threads.png'),
                        dpi=150)
            plt.close(fig)

        # --- Figure 4: Avg Latency vs Num Threads ---
        if threads_col:
            fig, ax = plt.subplots(figsize=(8, 5))
            for op in operations:
                xs, ys = _aggregate(rows, threads_col, op, 'time_avg_us')
                if xs:
                    try:
                        x_nums = [float(x) for x in xs]
                    except ValueError:
                        x_nums = list(range(len(xs)))
                    ax.plot(x_nums, ys, marker=op_markers[op],
                            color=op_colours[op], label=op_labels[op],
                            linewidth=2)
            ax.set_xlabel('Number of Threads')
            ax.set_ylabel('Average Latency (us)')
            ax.set_title('CTE Latency vs Thread Count')
            ax.legend()
            fig.tight_layout()
            fig.savefig(os.path.join(results_dir, 'latency_vs_threads.png'),
                        dpi=150)
            plt.close(fig)

        self.log(f'Plots saved to {results_dir}')
