from jarvis_cd.core.pkg import Application
from jarvis_cd.shell import Exec, PsshExecInfo, LocalExecInfo
import os
import re
import sys

# The repo root is on sys.path when jarvis imports this module, so the shared
# parser resolves as a namespace-package import.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jarvis_clio_core.bench_parse import parse_bench_output, parse_time_v  # noqa: E402


class ClioS3ReadBench(Application):
    """
    CLIO S3 Read Benchmark Application.

    Drives `clio_s3_read_bench`, which reads N objects from S3 through the CAE
    assimilator (ParseOmni -> S3FileAssimilator -> fork+exec cae_s3_tool get ->
    CTE PutBlob) with a sliding window of K in-flight transfers.

    Pairs with `zarr_s3_read_bench` in the s3_read_bench sweep; both emit the
    same two results blocks so one parser serves both.

    IMPORTANT -- environment ownership: the S3 download is performed by the
    RUNTIME process (the assimilator forks cae_s3_tool from inside clio_run),
    which the clio_runtime package launches with its own environment. Setting
    CAE_S3_TOOL / TMPDIR / AWS_* here therefore only covers this benchmark
    process; the pipeline's pre_cmds MUST also export them so the whole job
    script -- and hence the runtime daemon -- inherits them.
    """

    def _init(self):
        """Initialize instance state."""
        self.benchmark_executable = 'clio_s3_read_bench'
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
                'name': 'bucket',
                'msg': 'S3 bucket holding the staged objects',
                'type': str,
                'default': '',
                'help': 'Required. Staged by scripts/stage_s3_read_bench_data.py'
            },
            {
                'name': 'key_prefix',
                'msg': 'Key prefix for the object set',
                'type': str,
                'default': '',
                'help': 'Required. e.g. clio-s3-read-bench/raw/33554432'
            },
            {
                'name': 'object_size',
                'msg': 'Bytes per object',
                'type': str,
                'default': '32m',
                'help': 'Suffixes k/m/g. Must match the staged set: '
                        '512k, 4m, 32m, 256m'
            },
            {
                'name': 'num_objects',
                'msg': 'Number of objects to read',
                'type': int,
                'default': 64,
                'help': 'Sized so num_objects * object_size == 2 GiB'
            },
            {
                'name': 'concurrency',
                'msg': 'In-flight AsyncParseOmni calls (K)',
                'type': int,
                'default': 8,
                'help': 'Effective concurrency is capped by the runtime worker '
                        'count: the assimilator blocks a worker on waitpid for '
                        'the whole GET. Sweep clio_runtime.num_threads with this.'
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
                'msg': 'CTE tag prefix (one tag per object)',
                'type': str,
                'default': 's3rb',
                'help': 'Each object needs its own tag: the assimilator '
                        'restarts blob naming at chunk_0 per transfer, so a '
                        'shared tag would overwrite earlier objects.'
            },
            {
                'name': 'verify',
                'msg': 'Re-read tag sizes from CTE after the timed run',
                'type': bool,
                'default': False,
                'help': 'Adds 2 round trips per object; enable for one-off '
                        'validation runs, not for the sweep.'
            },
            {
                'name': 'preflight',
                'msg': 'Do a 1-byte ranged GET before timing starts',
                'type': bool,
                'default': True,
                'help': 'Fails fast on bad credentials/bucket/prefix instead of '
                        'issuing thousands of doomed GETs.'
            },
            {
                'name': 'aws_region',
                'msg': 'AWS region',
                'type': str,
                'default': 'us-east-1',
                'help': 'Sets AWS_DEFAULT_REGION'
            },
            {
                'name': 'aws_profile',
                'msg': 'AWS profile name from ~/.aws/credentials',
                'type': str,
                'default': '',
                'help': 'Empty uses the SDK default credential chain. Never put '
                        'secrets in the pipeline YAML.'
            },
            {
                'name': 'tmpdir',
                'msg': 'Node-local staging directory for downloaded objects',
                'type': str,
                'default': '/tmp/clio_s3_bench',
                'help': 'Peak usage is concurrency * object_size (32 x 256 MiB '
                        '= 8 GiB), so this must be on local disk with headroom.'
            },
            {
                'name': 's3_tool',
                'msg': 'Path to the cae_s3_tool helper',
                'type': str,
                'default': 'cae_s3_tool',
                'help': 'Exported as CAE_S3_TOOL; bare name resolves via PATH.'
            },
            {
                'name': 'nprocs',
                'msg': 'Number of benchmark processes',
                'type': int,
                'default': 1,
                'help': 'Fallback for when one process cannot saturate the '
                        'link: M processes partition the key space via '
                        '--object-stride/--object-offset.'
            },
            {
                'name': 'ppn',
                'msg': 'Processes per node',
                'type': int,
                'default': 1,
                'help': 'Only used when nprocs > 1'
            },
        ]

    def _configure(self, **kwargs):
        """
        Validate configuration and export the S3/AWS environment.

        Note this only covers THIS process. The runtime daemon forks
        cae_s3_tool, so the pipeline pre_cmds must export the same variables.
        """
        if not self.config['bucket']:
            raise ValueError('clio_s3_read_bench: bucket is required')
        if not self.config['key_prefix']:
            raise ValueError('clio_s3_read_bench: key_prefix is required')

        self.output_path = os.path.join(self.shared_dir,
                                        'clio_s3_bench_output.txt')
        self.rss_path = os.path.join(self.shared_dir, 'clio_s3_bench_time.txt')

        self.setenv('AWS_DEFAULT_REGION', self.config['aws_region'])
        if self.config['aws_profile']:
            self.setenv('AWS_PROFILE', self.config['aws_profile'])
        # Real AWS: an endpoint override flips cae_s3_tool to path-style
        # addressing against a nonexistent host, so it must stay unset.
        for key in ('S3_ENDPOINT', 'AWS_ENDPOINT_URL'):
            for env in (self.env, self.mod_env):
                if isinstance(env, dict):
                    env.pop(key, None)
        self.setenv('CAE_S3_TOOL', self.config['s3_tool'])
        self.setenv('TMPDIR', self.config['tmpdir'])
        os.makedirs(self.config['tmpdir'], exist_ok=True)

        self.log(f"CLIO S3 read benchmark: s3://{self.config['bucket']}/"
                 f"{self.config['key_prefix']} x {self.config['num_objects']} "
                 f"objects of {self.config['object_size']}, "
                 f"K={self.config['concurrency']}")

    def _time_prefix(self):
        """
        Build the GNU time(1) prefix used to capture peak RSS.

        Degrades to no prefix when /usr/bin/time is absent: peak RSS is a
        secondary metric, and hard-requiring the binary would turn a missing
        package into a total failure of every sweep row rather than one blank
        column. Note the shell builtin `time` cannot substitute -- it has no
        -v and no -o.

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
        Assemble the clio_s3_read_bench command line.

        Returns:
            str: The command, without any output redirection.
        """
        cmd = self._time_prefix() + [
            self.benchmark_executable,
            '--bucket', str(self.config['bucket']),
            '--key-prefix', str(self.config['key_prefix']),
            '--num-objects', str(self.config['num_objects']),
            '--object-size', str(self.config['object_size']),
            '--concurrency', str(self.config['concurrency']),
            '--tag-prefix', str(self.config['tag_prefix']),
            '--worker-threads', str(self.config['worker_threads']),
            '--label', 'Read',
        ]
        if self.config['verify']:
            cmd.append('--verify')
        if not self.config['preflight']:
            cmd.append('--no-preflight')
        return ' '.join(cmd)

    def start(self):
        """
        Run the benchmark, capturing stdout+stderr for _get_stat.

        The on-disk output file is the contract between start() and
        _get_stat(): the sweep runner reloads a fresh package instance before
        collecting stats, so an in-memory buffer would be lost.
        """
        # Stale output from a previous combination is the blank-column failure
        # mode -- a crash here would otherwise be scored with old numbers.
        for path in (self.output_path, self.rss_path):
            if path and os.path.exists(path):
                os.remove(path)

        cmd = self._build_cmd()
        nprocs = int(self.config['nprocs'])
        if nprocs > 1:
            # PsshExecInfo does not accept pipe_stdout; embed the redirect.
            cmd += f' --object-stride {nprocs}'
            exec_info = PsshExecInfo(
                env=self.mod_env,
                hostfile=self.hostfile,
                nprocs=nprocs,
                ppn=self.config['ppn'],
            )
            cmd += f' > {self.output_path} 2>&1'
        else:
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
                f'clio_s3_read_bench exited with non-zero code(s): {nonzero}')
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
        self.log(f'--- clio_s3_bench_output.txt tail ({len(tail)} lines) ---')
        for line in tail:
            self.log(line.rstrip())
        self.log('--- end tail ---')

    def _check_output_freshness(self):
        """
        Raise unless the output carries the results banner.

        A crash partway through the read loop leaves a file with the startup
        banner but no results, which is the silent failure that produces a
        green row with a blank throughput column.
        """
        if not os.path.exists(self.output_path):
            raise RuntimeError(
                f'clio_s3_read_bench produced no output at {self.output_path}')
        with open(self.output_path, 'r') as f:
            content = f.read()
        if not content.strip():
            raise RuntimeError(
                f'clio_s3_read_bench output is empty: {self.output_path}')
        stripped = re.sub(r'\033\[[0-9;]*m', '', content)
        if not re.search(r'=== \w+ Benchmark Results ===', stripped):
            self._log_output_tail()
            raise RuntimeError(
                'clio_s3_read_bench output lacks the results banner: '
                f'{self.output_path}')

    def stop(self):
        """Nothing to stop: the benchmark runs to completion."""
        return True

    def clean(self):
        """Remove benchmark output and staged temp files."""
        for path in (self.output_path, self.rss_path):
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                self.log(f'clean: could not remove {path}: {e}')
        try:
            Exec(f"rm -rf {self.config['tmpdir']}/cae_s3_*",
                 PsshExecInfo(hostfile=self.hostfile)).run()
        except Exception as e:
            self.log(f'clean: temp sweep failed: {e}')

    def _get_stat(self, stat_dict):
        """
        Scrape the benchmark output into results.csv columns.

        Keys are `<pkg_id>.<label>.<metric>`. This must never raise: jarvis
        calls it inside a try/except that logs a warning and continues, so an
        exception silently drops every column this package contributes.

        Args:
            stat_dict (dict): Collected statistics, modified in place.
        """
        output_path = os.path.join(self.shared_dir, 'clio_s3_bench_output.txt')
        rss_path = os.path.join(self.shared_dir, 'clio_s3_bench_time.txt')
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
        parse_time_v(rss_path, self.pkg_id, 'read', stat_dict)
        if found == 0:
            self.log(f'Warning: no metrics extracted from {output_path} '
                     f'({len(output)} bytes). A green row with a blank '
                     f'throughput column is a FAILURE.')
