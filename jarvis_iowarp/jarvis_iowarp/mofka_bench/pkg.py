"""
Mofka Benchmark Application — runs producer and/or consumer benchmarks
against a running Mofka server and collects performance metrics.
"""
from jarvis_cd.core.pkg import Application
from jarvis_cd.shell import Exec, LocalExecInfo
import os
import re
import time as time_mod


class MofkaBench(Application):
    """
    Runs Mofka producer/consumer benchmarks and collects throughput,
    event-rate, and latency statistics.
    """

    def _init(self):
        self.start_time = None

    def _configure_menu(self):
        return [
            {
                'name': 'mode',
                'msg': 'Benchmark mode',
                'type': str,
                'default': 'both',
                'choices': ['producer', 'consumer', 'both'],
            },
            {
                'name': 'num_events',
                'msg': 'Number of events to produce/consume',
                'type': int,
                'default': 1000,
            },
            {
                'name': 'data_size',
                'msg': 'Data payload size in bytes',
                'type': int,
                'default': 1024,
            },
            {
                'name': 'metadata_size',
                'msg': 'Metadata size in bytes',
                'type': int,
                'default': 64,
            },
            {
                'name': 'batch_size',
                'msg': 'Producer/consumer batch size',
                'type': int,
                'default': 16,
            },
            {
                'name': 'num_threads',
                'msg': 'Number of threads',
                'type': int,
                'default': 1,
            },
            {
                'name': 'data_selectivity',
                'msg': 'Consumer data selectivity (0.0-1.0)',
                'type': float,
                'default': 1.0,
            },
            {
                'name': 'use_progress_thread',
                'msg': 'Use Mercury progress thread',
                'type': bool,
                'default': True,
            },
        ]

    def _configure(self, **kwargs):
        # Resolve group file and topic from upstream mofka_server env
        group_file = self.env.get(
            'MOFKA_GROUP_FILE', '/tmp/mofka-bench/mofka.json'
        )
        topic = self.env.get('MOFKA_TOPIC', 'benchmark_topic')

        self.setenv('MOFKA_GROUP_FILE', group_file)
        self.setenv('MOFKA_TOPIC', topic)
        self.setenv('MOFKA_NUM_EVENTS', str(self.config['num_events']))
        self.setenv('MOFKA_DATA_SIZE', str(self.config['data_size']))
        self.setenv('MOFKA_METADATA_SIZE', str(self.config['metadata_size']))
        self.setenv('MOFKA_BATCH_SIZE', str(self.config['batch_size']))
        self.setenv('MOFKA_NUM_THREADS', str(self.config['num_threads']))
        self.setenv('MOFKA_DATA_SELECTIVITY',
                     str(self.config['data_selectivity']))

        self.log('Mofka benchmark configured')

    def start(self):
        t0 = time_mod.time()

        group_file = self.env.get(
            'MOFKA_GROUP_FILE', '/tmp/mofka-bench/mofka.json'
        )
        topic = self.env.get('MOFKA_TOPIC', 'benchmark_topic')
        scripts_dir = os.path.join(self.pkg_dir, 'scripts')

        # Clear previous output files to prevent stale data
        # from being matched by _parse_output (pipe_stdout appends).
        for fname in ('producer_output.txt', 'consumer_output.txt'):
            path = os.path.join(self.shared_dir, fname)
            if os.path.exists(path):
                os.remove(path)

        progress_flag = ('--use-progress-thread'
                         if self.config['use_progress_thread'] else '')

        # --- Producer ---
        if self.config['mode'] in ('producer', 'both'):
            cmd = (
                f'python3 {scripts_dir}/producer.py'
                f' --group-file {group_file}'
                f' --topic {topic}'
                f' --num-events {self.config["num_events"]}'
                f' --data-size {self.config["data_size"]}'
                f' --metadata-size {self.config["metadata_size"]}'
                f' --batch-size {self.config["batch_size"]}'
                f' --num-threads {self.config["num_threads"]}'
                f' {progress_flag}'
            )
            producer_out = os.path.join(self.shared_dir, 'producer_output.txt')
            self.log(f'Running producer benchmark')
            Exec(cmd, LocalExecInfo(
                env=self.mod_env,
                pipe_stdout=producer_out,
            )).run()

        # --- Consumer ---
        if self.config['mode'] in ('consumer', 'both'):
            cmd = (
                f'python3 {scripts_dir}/consumer.py'
                f' --group-file {group_file}'
                f' --topic {topic}'
                f' --num-events {self.config["num_events"]}'
                f' --data-size {self.config["data_size"]}'
                f' --data-selectivity {self.config["data_selectivity"]}'
                f' --batch-size {self.config["batch_size"]}'
                f' --num-threads {self.config["num_threads"]}'
                f' {progress_flag}'
            )
            consumer_out = os.path.join(
                self.shared_dir, 'consumer_output.txt'
            )
            self.log(f'Running consumer benchmark')
            Exec(cmd, LocalExecInfo(
                env=self.mod_env,
                pipe_stdout=consumer_out,
            )).run()

        self.start_time = time_mod.time() - t0
        self.log(f'Benchmark completed in {self.start_time:.2f}s')

    def stop(self):
        pass

    def clean(self):
        for fname in ('producer_output.txt', 'consumer_output.txt'):
            path = os.path.join(self.shared_dir, fname)
            if os.path.exists(path):
                os.remove(path)

    # ------------------------------------------------------------------
    # Statistics collection
    # ------------------------------------------------------------------

    def _get_stat(self, stat_dict):
        """Parse benchmark output files and populate stat_dict."""
        for role in ('producer', 'consumer'):
            output_path = os.path.join(
                self.shared_dir, f'{role}_output.txt'
            )
            if os.path.exists(output_path):
                with open(output_path, 'r') as f:
                    output = f.read()
                self._parse_output(output, role, stat_dict)

    def _parse_output(self, output, role, stat_dict):
        """Extract metrics from producer or consumer stdout.

        Uses findall and takes the last match to be resilient against
        output files that contain data from multiple appended runs.
        """
        patterns = {
            'throughput_mbps': r'Throughput:\s+([\d.]+)\s+MB/s',
            'events_per_sec': r'Events/sec:\s+([\d.]+)',
            'elapsed_ms': r'Elapsed time:\s+([\d.]+)\s+ms',
            'total_data_mb': r'Total data:\s+([\d.]+)\s+MB',
            'events_count': r'Events (?:produced|consumed):\s+(\d+)',
        }
        for metric, pattern in patterns.items():
            matches = re.findall(pattern, output)
            if matches:
                value = float(matches[-1])
                if metric == 'events_count':
                    value = int(value)
                stat_dict[f'{self.pkg_id}.{role}.{metric}'] = value

    # ------------------------------------------------------------------
    # Plotting
    # ------------------------------------------------------------------

    def _plot(self, results_dir):
        """
        Generate performance visualisation plots from pipeline test
        results stored in *results_dir*/results.csv.

        Produces four PNG figures:
          1. Throughput (MB/s) vs Data Size
          2. Events/sec vs Data Size
          3. Throughput (MB/s) vs Num Threads
          4. Events/sec vs Num Threads
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

        # Identify the stat column names (they use the pkg_name prefix)
        # Try to find any column matching *.producer.throughput_mbps
        pkg_prefix = None
        for key in rows[0]:
            if key.endswith('.producer.throughput_mbps'):
                pkg_prefix = key.rsplit('.producer.throughput_mbps', 1)[0]
                break
        if pkg_prefix is None:
            # Fall back: try consumer
            for key in rows[0]:
                if key.endswith('.consumer.throughput_mbps'):
                    pkg_prefix = key.rsplit('.consumer.throughput_mbps', 1)[0]
                    break
        if pkg_prefix is None:
            self.log('Could not identify stat columns in results.csv')
            return

        def _col(role, metric):
            return f'{pkg_prefix}.{role}.{metric}'

        def _safe_float(row, col):
            val = row.get(col, '')
            if val == '':
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # Detect which sweep variable columns exist
        data_size_col = None
        threads_col = None
        for key in rows[0]:
            if key.endswith('.data_size'):
                data_size_col = key
            if key.endswith('.num_threads'):
                threads_col = key

        # --- Helper: aggregate rows by a sweep variable ---
        def _aggregate(rows, sweep_col, role, metric_col):
            """Return sorted (x_values, y_means) averaging over repeats."""
            from collections import defaultdict
            buckets = defaultdict(list)
            for row in rows:
                x = _safe_float(row, sweep_col)
                y = _safe_float(row, _col(role, metric_col))
                if x is not None and y is not None:
                    buckets[x].append(y)
            xs = sorted(buckets.keys())
            ys = [sum(buckets[x]) / len(buckets[x]) for x in xs]
            return xs, ys

        # --- Figure 1: Throughput vs Data Size ---
        if data_size_col:
            fig, ax = plt.subplots(figsize=(8, 5))
            for role, colour, label in [
                ('producer', '#2196F3', 'Producer'),
                ('consumer', '#FF9800', 'Consumer'),
            ]:
                xs, ys = _aggregate(rows, data_size_col, role,
                                    'throughput_mbps')
                if xs:
                    ax.bar(
                        [str(int(x)) for x in xs], ys,
                        alpha=0.7, color=colour, label=label,
                        width=0.35,
                    )
            ax.set_xlabel('Data Size (bytes)')
            ax.set_ylabel('Throughput (MB/s)')
            ax.set_title('Mofka Throughput vs Data Size')
            ax.legend()
            fig.tight_layout()
            fig.savefig(os.path.join(results_dir,
                                     'throughput_vs_data_size.png'),
                        dpi=150)
            plt.close(fig)

        # --- Figure 2: Events/sec vs Data Size ---
        if data_size_col:
            fig, ax = plt.subplots(figsize=(8, 5))
            for role, colour, label in [
                ('producer', '#2196F3', 'Producer'),
                ('consumer', '#FF9800', 'Consumer'),
            ]:
                xs, ys = _aggregate(rows, data_size_col, role,
                                    'events_per_sec')
                if xs:
                    ax.bar(
                        [str(int(x)) for x in xs], ys,
                        alpha=0.7, color=colour, label=label,
                        width=0.35,
                    )
            ax.set_xlabel('Data Size (bytes)')
            ax.set_ylabel('Events / sec')
            ax.set_title('Mofka Event Rate vs Data Size')
            ax.legend()
            fig.tight_layout()
            fig.savefig(os.path.join(results_dir,
                                     'events_vs_data_size.png'),
                        dpi=150)
            plt.close(fig)

        # --- Figure 3: Throughput vs Num Threads ---
        if threads_col:
            fig, ax = plt.subplots(figsize=(8, 5))
            for role, marker, label in [
                ('producer', 'o', 'Producer'),
                ('consumer', 's', 'Consumer'),
            ]:
                xs, ys = _aggregate(rows, threads_col, role,
                                    'throughput_mbps')
                if xs:
                    ax.plot(xs, ys, marker=marker, label=label, linewidth=2)
            ax.set_xlabel('Number of Threads')
            ax.set_ylabel('Throughput (MB/s)')
            ax.set_title('Mofka Throughput vs Thread Count')
            ax.legend()
            fig.tight_layout()
            fig.savefig(os.path.join(results_dir,
                                     'throughput_vs_threads.png'),
                        dpi=150)
            plt.close(fig)

        # --- Figure 4: Events/sec vs Num Threads ---
        if threads_col:
            fig, ax = plt.subplots(figsize=(8, 5))
            for role, marker, label in [
                ('producer', 'o', 'Producer'),
                ('consumer', 's', 'Consumer'),
            ]:
                xs, ys = _aggregate(rows, threads_col, role,
                                    'events_per_sec')
                if xs:
                    ax.plot(xs, ys, marker=marker, label=label, linewidth=2)
            ax.set_xlabel('Number of Threads')
            ax.set_ylabel('Events / sec')
            ax.set_title('Mofka Event Rate vs Thread Count')
            ax.legend()
            fig.tight_layout()
            fig.savefig(os.path.join(results_dir,
                                     'events_vs_threads.png'),
                        dpi=150)
            plt.close(fig)

        self.log(f'Plots saved to {results_dir}')
