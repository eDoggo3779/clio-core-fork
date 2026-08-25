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


class ZarrS3WriteBench(Application):
    """
    Zarr S3 Write Benchmark Application.

    Drives scripts/zarr_s3_write.py, which writes a Zarr v3 store to S3 via
    zarr-python + s3fs and reports throughput in the same format as
    clio_s3_write_bench, so one parser serves all three stacks in the sweep.

    Runs once PER COMPRESSION VARIANT within a single sweep row, emitting
    labels `Write` (uncompressed) and `Writezstd`. Both land in the same
    results.csv row, so the compression comparison costs no extra sweep
    combinations and no extra CLIO re-runs.

    Compression is the biggest confound on the write side and it favours zarr:
    a zstd store sends fewer bytes than CLIO's uncompressed bdev moves. The
    `compressibility` option makes the source entropy an explicit input rather
    than an artifact of the test data -- see the script's module docstring.
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
                'msg': 'S3 bucket to write the Zarr stores into',
                'type': str,
                'default': '',
                'help': 'Required. The stores are OVERWRITTEN on every run.'
            },
            {
                'name': 'store_prefix',
                'msg': 'Key prefix to write the zarr/ directory under',
                'type': str,
                'default': 'clio-s3-write-bench',
                'help': 'Stores resolve to '
                        '<store_prefix>/zarr/wbench_<variant>.zarr'
            },
            {
                'name': 'total_bytes',
                'msg': 'Logical bytes to write',
                'type': int,
                'default': 268435456,
                'help': 'Must equal the CLIO row"s num_blobs * blob_size or '
                        'the two stacks are not moving the same data.'
            },
            {
                'name': 'chunk_bytes',
                'msg': 'Bytes per zarr chunk',
                'type': int,
                'default': 4194304,
                'help': 'Must equal the CLIO row"s blob_size so both stacks '
                        'move the same unit.'
            },
            {
                'name': 'variants',
                'msg': 'Compression variants to write in this row',
                'type': list,
                'default': ['none', 'zstd'],
                'help': 'Each becomes its own label: Write / Writezstd.'
            },
            {
                'name': 'compressibility',
                'msg': 'Source data entropy, 0.0 random .. 1.0 constant',
                'type': float,
                'default': 0.5,
                'help': 'Decides whether zstd sends fewer bytes than CLIO. At '
                        '0.0 zstd cannot compress at all and slightly EXPANDS '
                        'the data, so the zstd row would measure only encode '
                        'overhead; at 1.0 it compresses to nothing. Neither '
                        'resembles real scientific arrays.'
            },
            {
                'name': 'async_concurrency',
                'msg': 'zarr async.concurrency (K)',
                'type': int,
                'default': 32,
                'help': "zarr's own concurrency knob. Its default of 10 is "
                        'far too low for WAN S3.'
            },
            {
                'name': 'venv',
                'msg': 'Path to the venv providing zarr, s3fs and numpy',
                'type': str,
                'default': '${HOME}/zarr-venv',
                'help': 'Same venv the read benchmark uses.'
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
                'help': 'Resolved through the standard botocore chain; unlike '
                        'the CLIO side, zarr can use a profile directly.'
            },
        ]

    def _configure(self, **kwargs):
        """Validate configuration and put the zarr venv on PATH/PYTHONPATH."""
        if not self.config['bucket']:
            raise ValueError('zarr_s3_write_bench: bucket is required')
        if int(self.config['total_bytes']) <= 0:
            raise ValueError('zarr_s3_write_bench: total_bytes must be > 0')
        if int(self.config['chunk_bytes']) <= 0:
            raise ValueError('zarr_s3_write_bench: chunk_bytes must be > 0')

        self.script_path = os.path.join(self.pkg_dir, 'scripts',
                                        'zarr_s3_write.py')
        if not os.path.exists(self.script_path):
            raise ValueError(
                f'zarr_s3_write.py not found at {self.script_path}')

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

        self.log(f"Zarr S3 write benchmark: total_bytes="
                 f"{self.config['total_bytes']} chunk_bytes="
                 f"{self.config['chunk_bytes']} variants="
                 f"{self.config['variants']} "
                 f"concurrency={self.config['async_concurrency']}")

    def _variant_label(self, variant):
        """
        Results namespace for a compression variant.

        Args:
            variant (str): 'none' or a codec name such as 'zstd'.

        Returns:
            str: 'Write' for uncompressed, else 'Write<Variant>'.
        """
        return 'Write' if variant == 'none' else f'Write{variant.capitalize()}'

    def _output_path(self, variant):
        """
        Per-variant output file.

        Args:
            variant (str): Compression variant.

        Returns:
            str: Path under the shared dir.
        """
        return os.path.join(self.shared_dir,
                            f'zarr_s3_write_output_{variant}.txt')

    def _rss_path(self, variant):
        """
        Per-variant /usr/bin/time report.

        Args:
            variant (str): Compression variant.

        Returns:
            str: Path under the shared dir.
        """
        return os.path.join(self.shared_dir,
                            f'zarr_s3_write_time_{variant}.txt')

    def _time_prefix(self, rss_path):
        """
        Build the GNU time(1) prefix used to capture peak RSS.

        Degrades to no prefix when /usr/bin/time is absent: peak RSS is a
        secondary metric, and hard-requiring the binary would turn a missing
        package into a total failure of every sweep row rather than one blank
        column. The shell builtin `time` cannot substitute -- no -v, no -o.

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
        """Run the writer once per compression variant."""
        # The sweep runner reloads a fresh instance and calls start() WITHOUT
        # re-running _configure(), so self.script_path set there is still None
        # here. Resolve it from self.pkg_dir (a framework attribute set on
        # every instance) rather than trusting _configure -- otherwise the
        # command becomes "python None ..." and can't open the script.
        self.script_path = os.path.join(self.pkg_dir, 'scripts',
                                        'zarr_s3_write.py')

        venv = os.path.expandvars(self.config['venv'])
        python = os.path.join(venv, 'bin', 'python3')
        if not os.path.exists(python):
            python = 'python3'

        for variant in self.config['variants']:
            out = self._output_path(variant)
            rss = self._rss_path(variant)
            # Stale output from a previous combination is the blank-column
            # failure mode -- a crash here would otherwise be scored with old
            # numbers.
            for path in (out, rss):
                if os.path.exists(path):
                    os.remove(path)

            store_key = (f"{self.config['store_prefix']}/zarr/"
                         f'wbench_{variant}.zarr')
            cmd = (
                f'{self._time_prefix(rss)}{python} {self.script_path}'
                f" --bucket {self.config['bucket']}"
                f' --store-key {store_key}'
                f" --total-bytes {self.config['total_bytes']}"
                f" --chunk-bytes {self.config['chunk_bytes']}"
                f' --compressor {variant}'
                f" --compressibility {self.config['compressibility']}"
                f" --async-concurrency {self.config['async_concurrency']}"
                f" --region {self.config['aws_region']}"
                f' --label {self._variant_label(variant)}'
            )

            self.log(f'Executing: {cmd}')
            result = Exec(cmd, LocalExecInfo(
                env=self.mod_env, pipe_stdout=out, pipe_stderr=out)).run()

            exit_codes = getattr(result, 'exit_code', {}) or {}
            nonzero = {h: c for h, c in exit_codes.items() if c != 0}
            if nonzero:
                self._log_output_tail(out)
                raise RuntimeError(
                    f'zarr_s3_write.py ({variant}) exited with non-zero '
                    f'code(s): {nonzero}')
            self._check_output_freshness(out, variant)

        self.log('Zarr write benchmark completed for variants '
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
                f'zarr_s3_write.py ({variant}) produced no output at {path}')
        with open(path, 'r') as f:
            content = f.read()
        if not content.strip():
            raise RuntimeError(
                f'zarr_s3_write.py ({variant}) output is empty: {path}')
        if not re.search(r'=== \w+ Benchmark Results ===', content):
            self._log_output_tail(path)
            raise RuntimeError(
                f'zarr_s3_write.py ({variant}) output lacks the results '
                f'banner: {path}')

    def stop(self):
        """Nothing to stop: the benchmark runs to completion."""
        return True

    def clean(self):
        """
        Remove per-variant output and timing files.

        The S3 stores themselves are NOT purged here: the bucket prefix is the
        pipeline's to manage (post_cmds).
        """
        for pattern in ('zarr_s3_write_output_*.txt',
                        'zarr_s3_write_time_*.txt'):
            for path in glob.glob(os.path.join(self.shared_dir, pattern)):
                try:
                    os.remove(path)
                except Exception as e:
                    self.log(f'clean: could not remove {path}: {e}')

    def _get_stat(self, stat_dict):
        """
        Scrape every variant's output into results.csv columns.

        Keys are `<pkg_id>.<label>.<metric>`, with labels `write` and
        `writezstd` sharing one row. Must never raise: jarvis calls this
        inside a try/except that logs a warning and continues, so an exception
        silently drops every column this package contributes.

        Args:
            stat_dict (dict): Collected statistics, modified in place.
        """
        total = 0
        paths = sorted(glob.glob(os.path.join(
            self.shared_dir, 'zarr_s3_write_output_*.txt')))
        if not paths:
            self.log(f'No zarr write output files found in {self.shared_dir}')
            return
        for path in paths:
            try:
                with open(path, 'r') as f:
                    output = f.read()
            except Exception as e:
                self.log(f'Could not read {path}: {e}')
                continue
            total += parse_bench_output(output, self.pkg_id, stat_dict)
            variant = os.path.basename(path)[len('zarr_s3_write_output_'):-4]
            parse_time_v(self._rss_path(variant), self.pkg_id,
                         self._variant_label(variant).lower(), stat_dict)
        if total == 0:
            self.log(f'Warning: no metrics extracted from {paths}. A green '
                     f'row with a blank throughput column is a FAILURE.')
