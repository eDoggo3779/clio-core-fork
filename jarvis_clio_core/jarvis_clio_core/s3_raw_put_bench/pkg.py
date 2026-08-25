from jarvis_cd.core.pkg import Application
from jarvis_cd.shell import Exec, LocalExecInfo
import os
import re
import sys

# The repo root is on sys.path when jarvis imports this module, so the shared
# parser resolves as a namespace-package import.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jarvis_clio_core.bench_parse import parse_bench_output, parse_time_v  # noqa: E402


class S3RawPutBench(Application):
    """
    Raw S3 PUT wire-speed floor.

    Drives scripts/s3_raw_put.py, which uploads N pre-staged files with K
    concurrent `cae_s3_tool put` processes and reports in the same format as
    clio_s3_write_bench and zarr_s3_write_bench.

    This is the row that makes the other two interpretable. Without a floor,
    a poor CLIO number cannot be attributed: it may be CLIO's block layer, or
    simply what this host can push to this bucket at this concurrency. Read it
    as a bound, not a competitor -- it does no chunking, no metadata, and no
    compression, so nothing in the comparison should beat it.

    NOTE for later: the READ benchmark has no equivalent floor, which is the
    same interpretability gap in the other direction. See S3_WRITE_BENCH.md.
    """

    def _init(self):
        """Initialize instance state."""
        self.script_path = None
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
                'msg': 'S3 bucket to upload into',
                'type': str,
                'default': '',
                'help': 'Required.'
            },
            {
                'name': 'key_prefix',
                'msg': 'Key prefix for the uploaded objects',
                'type': str,
                'default': 'clio-s3-write-bench/rawput',
                'help': 'Keys resolve to <key_prefix>/raw_%06d.bin'
            },
            {
                'name': 'num_objects',
                'msg': 'Number of objects to upload',
                'type': int,
                'default': 64,
                'help': 'Match the CLIO row"s num_blobs.'
            },
            {
                'name': 'object_size',
                'msg': 'Bytes per object',
                'type': int,
                'default': 4194304,
                'help': 'Match the CLIO row"s blob_size.'
            },
            {
                'name': 'concurrency',
                'msg': 'Concurrent cae_s3_tool processes (K)',
                'type': int,
                'default': 8,
                'help': 'One process per in-flight PUT. Match the CLIO row"s '
                        'concurrency.'
            },
            {
                'name': 's3_tool',
                'msg': 'Path to the cae_s3_tool helper',
                'type': str,
                'default': 'cae_s3_tool',
                'help': 'Built under CAE_ENABLE_S3 / spack +s3. Resolved on '
                        'PATH when left as the bare name.'
            },
            {
                'name': 'tmpdir',
                'msg': 'Staging directory for the source files',
                'type': str,
                'default': '/tmp',
                'help': 'Peak usage is concurrency * object_size. Staging '
                        'happens before timing starts.'
            },
            {
                'name': 'aws_region',
                'msg': 'AWS region',
                'type': str,
                'default': 'us-east-1',
                'help': 'Must match the bucket"s real region.'
            },
            {
                'name': 'aws_profile',
                'msg': 'AWS profile name',
                'type': str,
                'default': '',
                'help': 'Resolved by cae_s3_tool through the AWS SDK chain.'
            },
        ]

    def _configure(self, **kwargs):
        """Validate configuration and export the AWS environment."""
        if not self.config['bucket']:
            raise ValueError('s3_raw_put_bench: bucket is required')
        if int(self.config['num_objects']) <= 0:
            raise ValueError('s3_raw_put_bench: num_objects must be > 0')
        if int(self.config['concurrency']) <= 0:
            raise ValueError('s3_raw_put_bench: concurrency must be > 0')

        self.script_path = os.path.join(self.pkg_dir, 'scripts',
                                        's3_raw_put.py')
        if not os.path.exists(self.script_path):
            raise ValueError(
                f's3_raw_put.py not found at {self.script_path}')

        self.output_path = os.path.join(self.shared_dir,
                                        's3_raw_put_output.txt')
        self.rss_path = os.path.join(self.shared_dir, 's3_raw_put_time.txt')

        self.setenv('AWS_DEFAULT_REGION', self.config['aws_region'])
        if self.config['aws_profile']:
            self.setenv('AWS_PROFILE', self.config['aws_profile'])
        self.setenv('CAE_S3_TOOL', self.config['s3_tool'])
        # Real AWS: an endpoint override flips cae_s3_tool to path-style
        # addressing against a nonexistent host, so it must stay unset.
        for key in ('S3_ENDPOINT', 'AWS_ENDPOINT_URL'):
            for env in (self.env, self.mod_env):
                if isinstance(env, dict):
                    env.pop(key, None)
        os.makedirs(self.config['tmpdir'], exist_ok=True)

        self.log(f"Raw S3 PUT floor: {self.config['num_objects']} objects of "
                 f"{self.config['object_size']} B, "
                 f"K={self.config['concurrency']}")

    def _time_prefix(self):
        """
        Build the GNU time(1) prefix used to capture peak RSS.

        Degrades to no prefix when /usr/bin/time is absent: peak RSS is a
        secondary metric, and hard-requiring the binary would turn a missing
        package into a total failure of every sweep row rather than one blank
        column. The shell builtin `time` cannot substitute -- no -v, no -o.

        Returns:
            str: Command prefix, possibly empty.
        """
        if os.path.exists('/usr/bin/time'):
            return f'/usr/bin/time -v -o {self.rss_path} '
        self.log('WARNING: /usr/bin/time not found; peak RSS will not be '
                 'recorded (throughput columns are unaffected)')
        return ''

    def start(self):
        """Run the raw-PUT floor, capturing stdout+stderr for _get_stat."""
        # The sweep runner reloads a fresh instance and calls start() WITHOUT
        # re-running _configure(), so the paths set there are still None here.
        # Resolve them from framework attributes rather than trusting
        # _configure -- otherwise the command becomes "python None ...".
        self.script_path = os.path.join(self.pkg_dir, 'scripts',
                                        's3_raw_put.py')
        self.output_path = os.path.join(self.shared_dir,
                                        's3_raw_put_output.txt')
        self.rss_path = os.path.join(self.shared_dir, 's3_raw_put_time.txt')

        # Stale output from a previous combination is the blank-column failure
        # mode -- a crash here would otherwise be scored with old numbers.
        for path in (self.output_path, self.rss_path):
            if os.path.exists(path):
                os.remove(path)

        cmd = (
            f'{self._time_prefix()}python3 {self.script_path}'
            f" --bucket {self.config['bucket']}"
            f" --key-prefix {self.config['key_prefix']}"
            f" --num-objects {self.config['num_objects']}"
            f" --object-size {self.config['object_size']}"
            f" --concurrency {self.config['concurrency']}"
            f" --s3-tool {self.config['s3_tool']}"
            f" --tmpdir {self.config['tmpdir']}"
            ' --label Rawput'
        )

        self.log(f'Executing: {cmd}')
        result = Exec(cmd, LocalExecInfo(
            env=self.mod_env,
            pipe_stdout=self.output_path,
            pipe_stderr=self.output_path)).run()

        exit_codes = getattr(result, 'exit_code', {}) or {}
        nonzero = {h: c for h, c in exit_codes.items() if c != 0}
        if nonzero:
            # The script exits non-zero when any PUT failed: a partial upload
            # timed fewer bytes than it reports, so the row must fail rather
            # than publish a flattering number.
            self._log_output_tail()
            raise RuntimeError(
                f's3_raw_put.py exited with non-zero code(s): {nonzero}')
        self._check_output_freshness()
        self.log(f'Raw PUT floor completed. Output: {self.output_path}')

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
        self.log(f'--- s3_raw_put_output.txt tail ({len(tail)} lines) ---')
        for line in tail:
            self.log(line.rstrip())
        self.log('--- end tail ---')

    def _check_output_freshness(self):
        """
        Raise unless the output carries the results banner.

        A crash partway through the upload leaves a file with the startup
        banner but no results, which is the silent failure that produces a
        green row with a blank throughput column.
        """
        if not os.path.exists(self.output_path):
            raise RuntimeError(
                f's3_raw_put.py produced no output at {self.output_path}')
        with open(self.output_path, 'r') as f:
            content = f.read()
        if not content.strip():
            raise RuntimeError(
                f's3_raw_put.py output is empty: {self.output_path}')
        if not re.search(r'=== \w+ Benchmark Results ===', content):
            self._log_output_tail()
            raise RuntimeError(
                's3_raw_put.py output lacks the results banner: '
                f'{self.output_path}')

    def stop(self):
        """Nothing to stop: the benchmark runs to completion."""
        return True

    def clean(self):
        """
        Remove benchmark output and any orphaned staging directories.

        The uploaded S3 objects are NOT purged here: the bucket prefix is the
        pipeline's to manage (post_cmds).
        """
        for path in (self.output_path, self.rss_path):
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                self.log(f'clean: could not remove {path}: {e}')
        try:
            Exec(f"rm -rf {self.config['tmpdir']}/s3_raw_put_*",
                 LocalExecInfo()).run()
        except Exception as e:
            self.log(f'clean: temp sweep failed: {e}')

    def _get_stat(self, stat_dict):
        """
        Scrape the benchmark output into results.csv columns.

        Keys are `<pkg_id>.rawput.<metric>`. Must never raise: jarvis calls
        this inside a try/except that logs a warning and continues, so an
        exception silently drops every column this package contributes.

        Args:
            stat_dict (dict): Collected statistics, modified in place.
        """
        output_path = os.path.join(self.shared_dir, 's3_raw_put_output.txt')
        rss_path = os.path.join(self.shared_dir, 's3_raw_put_time.txt')
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
        parse_time_v(rss_path, self.pkg_id, 'rawput', stat_dict)
        if found == 0:
            self.log(f'Warning: no metrics extracted from {output_path} '
                     f'({len(output)} bytes). A green row with a blank '
                     f'throughput column is a FAILURE.')
