from jarvis_cd.core.pkg import Application
from jarvis_cd.shell import Exec, LocalExecInfo
import glob
import os
import re
import sys

# The repo root is on sys.path when jarvis imports this module, so the shared
# parser resolves as a namespace-package import.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jarvis_clio_core.bench_parse import parse_bench_output, parse_time_v  # noqa: E402


class ZarrS3ReadBench(Application):
    """
    Zarr S3 Read Benchmark Application.

    Drives scripts/zarr_s3_read.py, which reads a Zarr v3 store from S3 via
    zarr-python + s3fs and reports throughput in the same format as
    clio_s3_read_bench, so one parser serves both stacks.

    Runs once PER COMPRESSION VARIANT within a single sweep row, emitting
    labels `Read` (uncompressed) and `Readzstd`. Both land in the same
    results.csv row, so the compression comparison costs no extra sweep
    combinations and no extra CLIO re-reads.
    """

    def _init(self):
        """Initialize instance state."""
        self.script_path = None

    def _configure_menu(self):
        """
        Configure the application menu.

        Returns:
            List[Dict]: Configuration menu options for the benchmark.
        """
        return [
            {
                'name': 'bucket',
                'msg': 'S3 bucket holding the staged Zarr stores',
                'type': str,
                'default': '',
                'help': 'Required. Staged by scripts/stage_s3_read_bench_data.py'
            },
            {
                'name': 'store_prefix',
                'msg': 'Key prefix containing the zarr/ directory',
                'type': str,
                'default': 'clio-s3-read-bench',
                'help': 'Stores resolve to '
                        '<store_prefix>/zarr/bench_c<edge>_<variant>.zarr'
            },
            {
                'name': 'chunk_edge',
                'msg': 'Cubic chunk edge selecting the store',
                'type': int,
                'default': 256,
                'help': 'One of 64/128/256/512 -> 512 KiB/4 MiB/32 MiB/256 MiB '
                        'per chunk. Swept in lockstep with the CLIO object size.'
            },
            {
                'name': 'variants',
                'msg': 'Compression variants to read in this row',
                'type': list,
                'default': ['none', 'zstd'],
                'help': "Each runs once; 'none' emits label Read, 'zstd' emits "
                        'label Readzstd, both into the same results row.'
            },
            {
                'name': 'async_concurrency',
                'msg': 'zarr async.concurrency (parallel chunk GETs)',
                'type': int,
                'default': 8,
                'help': "The Zarr-side concurrency knob. The reference suite's "
                        'default of 10 is far too low for WAN S3.'
            },
            {
                'name': 'aws_region',
                'msg': 'AWS region',
                'type': str,
                'default': 'us-east-1',
                'help': 'Passed to the s3fs client'
            },
            {
                'name': 'aws_profile',
                'msg': 'AWS profile name from ~/.aws/credentials',
                'type': str,
                'default': '',
                'help': 'Empty uses the botocore default chain. Never put '
                        'secrets in the pipeline YAML.'
            },
            {
                'name': 'venv',
                'msg': 'Path to the venv providing zarr, s3fs and numpy',
                'type': str,
                'default': '${HOME}/zarr-venv',
                'help': 'Prepended to PATH/PYTHONPATH. Do NOT use conda '
                        'activate or spack load: they install shell functions '
                        'that misbehave under the job script.'
            },
            {
                'name': 'anon',
                'msg': 'Read anonymously (public buckets only)',
                'type': bool,
                'default': False,
                'help': 'Skips credential resolution entirely'
            },
        ]

    def _configure(self, **kwargs):
        """Validate configuration and put the zarr venv on PATH/PYTHONPATH."""
        if not self.config['bucket']:
            raise ValueError('zarr_s3_read_bench: bucket is required')

        self.script_path = os.path.join(self.pkg_dir, 'scripts',
                                        'zarr_s3_read.py')
        if not os.path.exists(self.script_path):
            raise ValueError(
                f'zarr_s3_read.py not found at {self.script_path}')

        venv = os.path.expandvars(self.config['venv'])
        if venv and os.path.isdir(venv):
            self.prepend_env('PATH', os.path.join(venv, 'bin'))
            for site in glob.glob(
                    os.path.join(venv, 'lib', 'python3.*', 'site-packages')):
                self.prepend_env('PYTHONPATH', site)
        else:
            self.log(f'Warning: zarr venv not found at {venv}; relying on '
                     f'python3 already having zarr/s3fs/numpy')

        self.setenv('AWS_DEFAULT_REGION', self.config['aws_region'])
        if self.config['aws_profile']:
            self.setenv('AWS_PROFILE', self.config['aws_profile'])

        self.log(f"Zarr S3 read benchmark: chunk_edge="
                 f"{self.config['chunk_edge']} variants="
                 f"{self.config['variants']} "
                 f"concurrency={self.config['async_concurrency']}")

    def _variant_label(self, variant):
        """
        Results namespace for a compression variant.

        Args:
            variant (str): 'none' or a codec name such as 'zstd'.

        Returns:
            str: 'Read' for uncompressed, else 'Read<Variant>'.
        """
        return 'Read' if variant == 'none' else f'Read{variant.capitalize()}'

    def _output_path(self, variant):
        """
        Per-variant output file.

        Args:
            variant (str): Compression variant.

        Returns:
            str: Path under the shared dir.
        """
        return os.path.join(self.shared_dir,
                            f'zarr_s3_bench_output_{variant}.txt')

    def _rss_path(self, variant):
        """
        Per-variant /usr/bin/time report.

        Args:
            variant (str): Compression variant.

        Returns:
            str: Path under the shared dir.
        """
        return os.path.join(self.shared_dir,
                            f'zarr_s3_bench_time_{variant}.txt')

    def _time_prefix(self, rss_path):
        """
        Build the GNU time(1) prefix used to capture peak RSS.

        Degrades to no prefix when /usr/bin/time is absent: peak RSS is a
        secondary metric, and hard-requiring the binary would turn a missing
        package into a total failure of every sweep row rather than one blank
        column. Note the shell builtin `time` cannot substitute -- it has no
        -v and no -o.

        Args:
            rss_path (str): Where time(1) should write its report.

        Returns:
            str: Command prefix, possibly empty.
        """
        if os.path.exists('/usr/bin/time'):
            return f'/usr/bin/time -v -o {rss_path} '
        self.log('WARNING: /usr/bin/time not found; peak RSS will not be '
                 'recorded (throughput columns are unaffected)')
        return ''

    def start(self):
        """Run the reader once per compression variant."""
        # The sweep runner reloads a fresh instance and calls start() WITHOUT
        # re-running _configure(), so self.script_path set there is still None
        # here. Resolve it from self.pkg_dir (a framework attribute set on
        # every instance) rather than trusting _configure -- otherwise the
        # command becomes "python None ..." and can't open the script.
        self.script_path = os.path.join(self.pkg_dir, 'scripts',
                                        'zarr_s3_read.py')

        venv = os.path.expandvars(self.config['venv'])
        python = os.path.join(venv, 'bin', 'python3')
        if not os.path.exists(python):
            python = 'python3'

        for variant in self.config['variants']:
            out = self._output_path(variant)
            rss = self._rss_path(variant)
            for path in (out, rss):
                if os.path.exists(path):
                    os.remove(path)

            store_key = (f"{self.config['store_prefix']}/zarr/"
                         f"bench_c{self.config['chunk_edge']}_{variant}.zarr")
            cmd = (
                f'{self._time_prefix(rss)}{python} {self.script_path}'
                f" --bucket {self.config['bucket']}"
                f' --store-key {store_key}'
                f" --async-concurrency {self.config['async_concurrency']}"
                f" --region {self.config['aws_region']}"
                f' --label {self._variant_label(variant)}'
            )
            if self.config['anon']:
                cmd += ' --anon'

            self.log(f'Executing: {cmd}')
            result = Exec(cmd, LocalExecInfo(
                env=self.mod_env, pipe_stdout=out, pipe_stderr=out)).run()

            exit_codes = getattr(result, 'exit_code', {}) or {}
            nonzero = {h: c for h, c in exit_codes.items() if c != 0}
            if nonzero:
                self._log_output_tail(out)
                raise RuntimeError(
                    f'zarr_s3_read.py ({variant}) exited with non-zero '
                    f'code(s): {nonzero}')
            self._check_output_freshness(out, variant)

        self.log('Zarr benchmark completed for variants '
                 f"{self.config['variants']}")

    def _log_output_tail(self, path, n_lines=100):
        """
        Emit the tail of an output file into the Jarvis log.

        Args:
            path (str): File to tail.
            n_lines (int): How many trailing lines to log.
        """
        if not os.path.exists(path):
            self.log(f'(no output file at {path})')
            return
        try:
            with open(path, 'r') as f:
                lines = f.readlines()
        except Exception as e:
            self.log(f'failed to read {path}: {e}')
            return
        tail = lines[-n_lines:] if len(lines) > n_lines else lines
        self.log(f'--- {os.path.basename(path)} tail ({len(tail)} lines) ---')
        for line in tail:
            self.log(line.rstrip())
        self.log('--- end tail ---')

    def _check_output_freshness(self, path, variant):
        """
        Raise unless the output carries the results banner.

        Args:
            path (str): Output file to inspect.
            variant (str): Compression variant, for the error message.
        """
        if not os.path.exists(path):
            raise RuntimeError(
                f'zarr_s3_read.py ({variant}) produced no output at {path}')
        with open(path, 'r') as f:
            content = f.read()
        if not content.strip():
            raise RuntimeError(
                f'zarr_s3_read.py ({variant}) output is empty: {path}')
        if not re.search(r'=== \w+ Benchmark Results ===', content):
            self._log_output_tail(path)
            raise RuntimeError(
                f'zarr_s3_read.py ({variant}) output lacks the results '
                f'banner: {path}')

    def stop(self):
        """Nothing to stop: the benchmark runs to completion."""
        return True

    def clean(self):
        """Remove per-variant output and timing files."""
        for pattern in ('zarr_s3_bench_output_*.txt',
                        'zarr_s3_bench_time_*.txt'):
            for path in glob.glob(os.path.join(self.shared_dir, pattern)):
                try:
                    os.remove(path)
                except Exception as e:
                    self.log(f'clean: could not remove {path}: {e}')

    def _get_stat(self, stat_dict):
        """
        Scrape every variant's output into results.csv columns.

        Keys are `<pkg_id>.<label>.<metric>`, with labels `read` and
        `readzstd` sharing one row. Must never raise: jarvis calls this inside
        a try/except that logs a warning and continues, so an exception
        silently drops every column this package contributes.

        Args:
            stat_dict (dict): Collected statistics, modified in place.
        """
        total = 0
        paths = sorted(glob.glob(os.path.join(
            self.shared_dir, 'zarr_s3_bench_output_*.txt')))
        if not paths:
            self.log(f'No zarr output files found in {self.shared_dir}')
            return
        for path in paths:
            try:
                with open(path, 'r') as f:
                    output = f.read()
            except Exception as e:
                self.log(f'Could not read {path}: {e}')
                continue
            total += parse_bench_output(output, self.pkg_id, stat_dict)
            variant = os.path.basename(path)[len('zarr_s3_bench_output_'):-4]
            parse_time_v(self._rss_path(variant), self.pkg_id,
                         self._variant_label(variant).lower(), stat_dict)
        if total == 0:
            self.log(f'Warning: no metrics extracted from {paths}. A green '
                     f'row with a blank throughput column is a FAILURE.')
