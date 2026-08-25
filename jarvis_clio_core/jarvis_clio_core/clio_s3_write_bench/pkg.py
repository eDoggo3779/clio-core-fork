from jarvis_cd.core.pkg import Application
from jarvis_cd.shell import Exec, LocalExecInfo
import os
import re
import sys

# The repo root is on sys.path when jarvis imports this module, so the shared
# parser resolves as a namespace-package import.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jarvis_clio_core.bench_parse import parse_bench_output, parse_time_v  # noqa: E402


class ClioS3WriteBench(Application):
    """
    CLIO S3 Write Benchmark Application.

    Drives `clio_s3_write_bench`, which writes N blobs into CTE with a sliding
    window of K in-flight AsyncPutBlob calls. CTE places them on an S3-backed
    bdev tier whose WriteBlocks issues signed PUTs from the runtime daemon.

    Pairs with `zarr_s3_write_bench` (baseline) and `s3_raw_put_bench` (wire
    floor) in the s3_write_bench sweep; all three emit the same two results
    blocks so one parser serves them.

    IMPORTANT -- environment ownership: the PUT is performed by the RUNTIME
    process, which the clio_runtime package launches with its own environment.
    The AWS_* variables set here cover only this benchmark process, which does
    no S3 I/O at all. The pipeline's pre_cmds MUST export them so the job
    script -- and hence the runtime daemon -- inherits them. This differs from
    the read bench only in which process signs: there it was the forked
    cae_s3_tool, here it is clio_run itself.
    """

    def _init(self):
        """Initialize instance state."""
        self.benchmark_executable = 'clio_s3_write_bench'
        self.output_path = None
        self.rss_path = None

    def _configure_menu(self):
        """
        Configure the application menu.

        Returns:
            List[Dict]: Configuration menu options for the benchmark.
        """
        return [
            {
                'name': 'blob_size',
                'msg': 'Bytes per blob',
                'type': str,
                'default': '4m',
                'help': 'Suffixes k/m/g. Blobs larger than the bdev block '
                        'size become several PUTs.'
            },
            {
                'name': 'num_blobs',
                'msg': 'Number of blobs to write',
                'type': int,
                'default': 64,
                'help': 'num_blobs * blob_size is the total bytes written.'
            },
            {
                'name': 'concurrency',
                'msg': 'In-flight AsyncPutBlob calls (K)',
                'type': int,
                'default': 8,
                'help': 'Effective concurrency is capped by the runtime worker '
                        'count: the S3 PUT blocks a worker for its whole '
                        'duration. Sweep clio_runtime.num_threads with this.'
            },
            {
                'name': 'block_size',
                'msg': 'Bdev block size, for the derived PUT count',
                'type': str,
                'default': '1m',
                'help': 'Must match the bdev tier actually configured, or the '
                        '"PUT count" fairness column will be wrong. It is a '
                        'reporting input only; it does not configure the bdev.'
            },
            {
                'name': 'worker_threads',
                'msg': 'Runtime worker threads, for the fairness report',
                'type': int,
                'default': 0,
                'help': 'Reported verbatim into results.csv. Keep in sync with '
                        'clio_runtime.num_threads; 0 means unknown.'
            },
            {
                'name': 'tag_prefix',
                'msg': 'CTE tag prefix',
                'type': str,
                'default': 's3wb',
                'help': 'All blobs share one tag here -- unlike the read '
                        'bench, blob names are explicit so there is no '
                        'chunk_0 restart hazard.'
            },
            {
                'name': 'verify',
                'msg': 'Read every blob back and compare bytes',
                'type': bool,
                'default': False,
                'help': 'Adds a full read pass. Enable for one-off validation '
                        'runs, not for the sweep. This is what proves bytes '
                        'round-tripped through S3.'
            },
            {
                'name': 'aws_region',
                'msg': 'AWS region',
                'type': str,
                'default': 'us-east-1',
                'help': 'SigV4 is region-scoped: a mismatch against the '
                        "bucket's real region is an HTTP 301, not a 403."
            },
            {
                'name': 'aws_profile',
                'msg': 'AWS profile name (credentials come from pre_cmds)',
                'type': str,
                'default': '',
                'help': 'Recorded for provenance. The Poco signer reads env '
                        'vars only, so the profile must be resolved to raw '
                        'keys by the pipeline pre_cmds.'
            },
        ]

    def _configure(self, **kwargs):
        """
        Validate configuration and record the AWS region.

        Note this only covers THIS process, which performs no S3 I/O. The
        runtime daemon signs and PUTs, so the pipeline pre_cmds must export
        AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION.
        """
        if int(self.config['num_blobs']) <= 0:
            raise ValueError('clio_s3_write_bench: num_blobs must be > 0')
        if int(self.config['concurrency']) <= 0:
            raise ValueError('clio_s3_write_bench: concurrency must be > 0')

        self.output_path = os.path.join(self.shared_dir,
                                        'clio_s3_write_output.txt')
        self.rss_path = os.path.join(self.shared_dir,
                                     'clio_s3_write_time.txt')

        self.setenv('AWS_DEFAULT_REGION', self.config['aws_region'])
        if self.config['aws_profile']:
            self.setenv('AWS_PROFILE', self.config['aws_profile'])
        # Real AWS: an endpoint override flips the signer to path-style
        # addressing against a nonexistent host, so it must stay unset.
        for key in ('S3_ENDPOINT', 'AWS_ENDPOINT_URL'):
            for env in (self.env, self.mod_env):
                if isinstance(env, dict):
                    env.pop(key, None)

        self.log(f"CLIO S3 write benchmark: {self.config['num_blobs']} blobs "
                 f"of {self.config['blob_size']}, "
                 f"K={self.config['concurrency']}")

    def _time_prefix(self):
        """
        Build the GNU time(1) prefix used to capture peak RSS.

        Degrades to no prefix when /usr/bin/time is absent: peak RSS is a
        secondary metric, and hard-requiring the binary would turn a missing
        package into a total failure of every sweep row rather than one blank
        column. The shell builtin `time` cannot substitute -- no -v, no -o.

        Returns:
            list: Command tokens to prepend (possibly empty).
        """
        if os.path.exists('/usr/bin/time'):
            return ['/usr/bin/time', '-v', '-o', self.rss_path]
        self.log('WARNING: /usr/bin/time not found; peak RSS will not be '
                 'recorded (throughput columns are unaffected)')
        return []

    def _build_cmd(self):
        """
        Assemble the clio_s3_write_bench command line.

        Returns:
            str: The command, without any output redirection.
        """
        cmd = self._time_prefix() + [
            self.benchmark_executable,
            '--num-blobs', str(self.config['num_blobs']),
            '--blob-size', str(self.config['blob_size']),
            '--block-size', str(self.config['block_size']),
            '--concurrency', str(self.config['concurrency']),
            '--tag-prefix', str(self.config['tag_prefix']),
            '--worker-threads', str(self.config['worker_threads']),
            '--label', 'Write',
        ]
        if self.config['verify']:
            cmd.append('--verify')
        return ' '.join(cmd)

    def start(self):
        """
        Run the benchmark, capturing stdout+stderr for _get_stat.

        The on-disk output file is the contract between start() and
        _get_stat(): the sweep runner reloads a fresh package instance before
        collecting stats, so an in-memory buffer would be lost.
        """
        # The sweep runner reloads a fresh instance and calls start() WITHOUT
        # re-running _configure(), so the paths it sets are still None here.
        # Resolve them from framework attributes rather than trusting
        # _configure -- otherwise _time_prefix() feeds a None into ' '.join().
        self.output_path = os.path.join(self.shared_dir,
                                        'clio_s3_write_output.txt')
        self.rss_path = os.path.join(self.shared_dir,
                                     'clio_s3_write_time.txt')

        # Stale output from a previous combination is the blank-column failure
        # mode -- a crash here would otherwise be scored with old numbers.
        for path in (self.output_path, self.rss_path):
            if path and os.path.exists(path):
                os.remove(path)

        cmd = self._build_cmd()
        exec_info = LocalExecInfo(
            env=self.mod_env,
            pipe_stdout=self.output_path,
            pipe_stderr=self.output_path,
        )

        self.log(f'Executing: {cmd}')
        result = Exec(cmd, exec_info).run()

        exit_codes = getattr(result, 'exit_code', {}) or {}
        nonzero = {h: c for h, c in exit_codes.items() if c != 0}
        if nonzero:
            self._log_output_tail()
            raise RuntimeError(
                f'clio_s3_write_bench exited with non-zero code(s): {nonzero}')
        self._check_output_freshness()
        self.log(f'Benchmark completed. Output: {self.output_path}')

    def _log_output_tail(self, n_lines=100):
        """
        Emit the tail of the benchmark output into the Jarvis log.

        Args:
            n_lines (int): How many trailing lines to log.
        """
        if not self.output_path or not os.path.exists(self.output_path):
            self.log(f'(no output file at {self.output_path})')
            return
        try:
            with open(self.output_path, 'r') as f:
                lines = f.readlines()
        except Exception as e:
            self.log(f'failed to read {self.output_path}: {e}')
            return
        tail = lines[-n_lines:] if len(lines) > n_lines else lines
        self.log(f'--- clio_s3_write_output.txt tail ({len(tail)} lines) ---')
        for line in tail:
            self.log(line.rstrip())
        self.log('--- end tail ---')

    def _check_output_freshness(self):
        """
        Raise unless the output carries the results banner.

        A crash partway through the write loop leaves a file with the startup
        banner but no results, which is the silent failure that produces a
        green row with a blank throughput column.
        """
        if not os.path.exists(self.output_path):
            raise RuntimeError(
                f'clio_s3_write_bench produced no output at '
                f'{self.output_path}')
        with open(self.output_path, 'r') as f:
            content = f.read()
        if not content.strip():
            raise RuntimeError(
                f'clio_s3_write_bench output is empty: {self.output_path}')
        stripped = re.sub(r'\033\[[0-9;]*m', '', content)
        if not re.search(r'=== \w+ Benchmark Results ===', stripped):
            self._log_output_tail()
            raise RuntimeError(
                'clio_s3_write_bench output lacks the results banner: '
                f'{self.output_path}')

    def stop(self):
        """Nothing to stop: the benchmark runs to completion."""
        return True

    def clean(self):
        """
        Remove benchmark output.

        The S3 objects themselves are NOT purged here: the bucket prefix is
        the pipeline's to manage (post_cmds), and clean() runs per-package
        without knowing whether another row still needs the data.
        """
        for path in (self.output_path, self.rss_path):
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                self.log(f'clean: could not remove {path}: {e}')

    def _get_stat(self, stat_dict):
        """
        Scrape the benchmark output into results.csv columns.

        Keys are `<pkg_id>.<label>.<metric>`. This must never raise: jarvis
        calls it inside a try/except that logs a warning and continues, so an
        exception silently drops every column this package contributes.

        Args:
            stat_dict (dict): Collected statistics, modified in place.
        """
        output_path = os.path.join(self.shared_dir,
                                   'clio_s3_write_output.txt')
        rss_path = os.path.join(self.shared_dir, 'clio_s3_write_time.txt')
        if not os.path.exists(output_path):
            self.log(f'No output file found at {output_path}')
            return
        try:
            with open(output_path, 'r') as f:
                output = f.read()
        except Exception as e:
            self.log(f'Could not read {output_path}: {e}')
            return
        found = parse_bench_output(output, self.pkg_id, stat_dict)
        parse_time_v(rss_path, self.pkg_id, 'write', stat_dict)
        if found == 0:
            self.log(f'Warning: no metrics extracted from {output_path} '
                     f'({len(output)} bytes). A green row with a blank '
                     f'throughput column is a FAILURE.')
