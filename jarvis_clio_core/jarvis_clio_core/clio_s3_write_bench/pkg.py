from jarvis_cd.core.pkg import Application
from jarvis_cd.shell import Exec, LocalExecInfo
import os
import re
import subprocess
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
    no S3 I/O at all.

    Exporting them in the pipeline's pre_cmds is NOT sufficient either, which
    is the trap this benchmark actually fell into. Jarvis launches the daemon
    as `PsshExecInfo(env=self.env, ...)` with a dict it builds from
    `EnvironmentManager.COMMON_ENV_VARS` -- a fixed list of toolchain
    variables that contains no AWS_* entry -- so the job script's exports
    reach jarvis and every benchmark process but never clio_run. The runtime
    package's `forward_env` option exists to bridge that gap; the pipeline
    must list the AWS names there. See clio_runtime.pkg._forward_env.

    This differs from the read bench only in which process signs: there it was
    the forked cae_s3_tool, here it is clio_run itself.
    """

    def _init(self):
        """Initialize instance state."""
        self.benchmark_executable = 'clio_s3_write_bench'
        self.output_path = None
        self.rss_path = None
        self.objects_path = None
        self.purged_count = None

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
                'help': 'Suffixes k/m/g. One blob becomes one S3 object: '
                        'the bdev issues one PutObject per allocator block '
                        'and an unfragmented request gets a single block.'
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
                'name': 'bucket',
                'msg': 'Bucket to count objects in after the run',
                'type': str,
                'default': '',
                'help': 'Enables the objects_measured column. Empty disables '
                        'the count; the run still succeeds.'
            },
            {
                'name': 'key_prefix',
                'msg': 'Key prefix of the bdev tier, for the object count',
                'type': str,
                'default': '',
                'help': 'The clio_cte device path WITHOUT the s3://bucket/ '
                        'part and without the _node<N> suffix CTE appends -- '
                        'listing that stem covers every node.'
            },
            {
                'name': 'venv',
                'msg': 'Python venv providing botocore, for the object count',
                'type': str,
                'default': '',
                'help': 'Ares has no AWS CLI and no system botocore; reuse '
                        'the zarr venv. Empty falls back to sys.executable.'
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
                'name': 'purge_prefix',
                'msg': 'Delete stale objects under key_prefix before running',
                'type': bool,
                'default': True,
                'help': 'Every row of a sweep shares one key prefix, so '
                        'without this objects_measured also counts what '
                        'earlier rows left behind and stops being an exact '
                        'check. Only the bdev prefix is deleted -- the zarr '
                        "store sits one level up and the raw-PUT floor's keys "
                        'in a sibling, so neither is in range. Needs bucket, '
                        'key_prefix and venv, and is skipped without them.'
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
        self.objects_path = os.path.join(self.shared_dir,
                                         'clio_s3_write_objects.txt')

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
        self.objects_path = os.path.join(self.shared_dir,
                                         'clio_s3_write_objects.txt')
        # Same reasoning as the paths: never trust _configure to have run.
        self.purged_count = None

        # Stale output from a previous combination is the blank-column failure
        # mode -- a crash here would otherwise be scored with old numbers.
        for path in (self.output_path, self.rss_path, self.objects_path):
            if path and os.path.exists(path):
                os.remove(path)

        # Stale objects in the bucket are the same failure mode one level out:
        # objects_measured lists the prefix, so a previous row's leftovers
        # would be counted as this row's work. Must happen before the run.
        self._purge_prefix()

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
        # Count what actually landed in the bucket while the runtime is still
        # up. It must happen here, not in _get_stat: the runtime's teardown
        # frees blocks, and FreeBlocks issues a DeleteObject per block
        # (s3_bdev_transport.cc), so by stat-collection time the prefix may be
        # empty and the count would read zero.
        self._measure_objects()
        self.log(f'Benchmark completed. Output: {self.output_path}')

    # ------------------------------------------------------------------
    # Shared botocore plumbing. Ares has no AWS CLI, so every S3 control-plane
    # call here shells into the zarr venv's botocore. Both the pre-run purge
    # and the post-run count need the same credential resolution, so it lives
    # in one place rather than being duplicated between them.
    # ------------------------------------------------------------------

    def _bucket_target(self, what):
        """
        Resolve (bucket, prefix) for the bdev tier, or (None, None).

        The prefix is required, not defaulted. An empty prefix would widen
        every caller to the whole bucket -- harmless for a listing, but the
        purge would then delete the zarr baseline's store, which sits one
        level up at `clio-s3-write-bench` against the bdev's
        `clio-s3-write-bench/bdev`.

        Args:
            what (str): Label for the skip message.

        Returns:
            tuple: (bucket, prefix), or (None, None) when unset.
        """
        bucket = str(self.config.get('bucket') or '').strip()
        prefix = str(self.config.get('key_prefix') or '').strip().strip('/')
        if not bucket or not prefix:
            self.log(f'{what}: skipped (bucket/key_prefix not set)')
            return None, None
        return bucket, prefix

    def _botocore_env(self):
        """
        Build the environment for a botocore subprocess.

        AWS_PROFILE + HOME resolve through botocore's shared credentials file.
        mod_env carries both (jarvis copies HOME out of COMMON_ENV_VARS), and
        the job script's raw keys are inherited by this process, so those are
        passed through too for the profile-less case.

        Returns:
            dict: Environment for the subprocess.
        """
        env = dict(self.mod_env) if isinstance(self.mod_env, dict) \
            else dict(os.environ)
        for name in ('HOME', 'AWS_PROFILE', 'AWS_ACCESS_KEY_ID',
                     'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN'):
            if not env.get(name) and os.environ.get(name):
                env[name] = os.environ[name]
        env['AWS_DEFAULT_REGION'] = str(self.config.get('aws_region')
                                        or env.get('AWS_DEFAULT_REGION')
                                        or 'us-east-1')
        return env

    def _run_botocore(self, script, args, what, timeout=300):
        """
        Run a botocore snippet in the configured venv.

        Never raises: both callers are diagnostics around a measurement that
        has its own success criteria, and neither should be able to fail a row
        on its own.

        Args:
            script (str): Python source to run with `-c`.
            args (list): Positional arguments appended after the script.
            what (str): Label for log messages.
            timeout (int): Seconds before the subprocess is killed.

        Returns:
            str: stdout on success, or None.
        """
        python = str(self.config.get('venv') or '').strip()
        python = os.path.join(python, 'bin', 'python3') if python \
            else sys.executable
        if not os.path.exists(python):
            self.log(f'{what}: skipped ({python} not found)')
            return None
        try:
            proc = subprocess.run([python, '-c', script] + list(args),
                                  env=self._botocore_env(),
                                  capture_output=True, timeout=timeout,
                                  text=True)
        except Exception as e:
            self.log(f'{what}: failed ({e})')
            return None
        if proc.returncode != 0:
            self.log(f'{what}: failed rc={proc.returncode}: '
                     f'{(proc.stderr or "").strip()[-400:]}')
            return None
        return proc.stdout

    def _purge_prefix(self):
        """
        Delete every object under the bdev key prefix before the run.

        WHY THIS EXISTS. All rows of a sweep share one key prefix, and
        `objects_measured` is a listing of that prefix -- so it counts
        whatever earlier rows left behind alongside what this row wrote. The
        36-row sweep of 2026-08-26 hit exactly that: every 4 MiB row reported
        448 objects against `num_blobs: 256`, the excess being precisely 192 x
        1 MiB orphans that the 1 MiB rows' teardown never deleted. Throughput
        was unaffected -- those columns come from logical_bytes/wall_time_us,
        which the benchmark owns -- but the guard was blunted into a lower
        bound, and a row that wrote nothing would still have listed its
        predecessors' objects and looked plausible.

        Purging here rather than subtracting a baseline in the count is
        deliberate: it restores `objects_measured == num_blobs` as an exact
        equality instead of leaving a comparison against a moving reference.

        WHY DELETING HERE IS SAFE. At start() this row has written nothing, so
        every object under the prefix belongs to a row that has already
        finished. That rests on the runtime tearing down between rows, which
        the sweep evidence supports: the orphan count held at exactly 192
        across all eighteen 4 MiB rows rather than accumulating, so each row
        does free its own blocks. If that ever stops holding, the failure is
        loud rather than silent -- purging live blocks shows up immediately as
        objects_measured < num_blobs, which the gate already catches.

        Only the bdev prefix is in range. The zarr baseline's store sits one
        level up and the raw-PUT floor's keys in a sibling prefix, so neither
        can be reached; `_bucket_target` refuses an empty prefix outright, and
        the snippet asserts it again on the far side.

        NOT A ROOT-CAUSE FIX. Something in the bdev teardown path is still
        orphaning blocks -- FreeBlocks issues a DeleteObject per block, and 192
        of them did not happen. This makes the measurement honest; it does not
        explain the leak. `objects_purged` is recorded per row precisely so
        the leak stays visible: a nonzero value names the row whose
        predecessor leaked, and how much.

        Never raises: a purge problem must not fail a row that can still run.
        """
        if not self.config.get('purge_prefix'):
            return
        bucket, prefix = self._bucket_target('purge_prefix')
        if not bucket:
            return

        # Keys are collected across all pages BEFORE any delete: deleting
        # while the paginator is mid-listing invalidates its continuation
        # token and can silently skip a page.
        script = (
            'import os, sys, botocore.session\n'
            'b, p = sys.argv[1], sys.argv[2]\n'
            'assert p, "refusing to purge an empty prefix"\n'
            'c = botocore.session.get_session().create_client(\n'
            '    "s3", region_name=os.environ.get("AWS_DEFAULT_REGION"))\n'
            'keys = []\n'
            'for page in c.get_paginator("list_objects_v2").paginate(\n'
            '        Bucket=b, Prefix=p):\n'
            '    for o in page.get("Contents", []):\n'
            '        keys.append({"Key": o["Key"]})\n'
            'n = 0\n'
            'for i in range(0, len(keys), 1000):\n'
            '    batch = keys[i:i + 1000]\n'
            '    r = c.delete_objects(Bucket=b, Delete={"Objects": batch})\n'
            '    n += len(r.get("Deleted", []))\n'
            '    for e in r.get("Errors", []):\n'
            '        print("ERROR", e.get("Key"), e.get("Message"),\n'
            '              file=sys.stderr)\n'
            'print(n)\n')
        out = self._run_botocore(script, [bucket, prefix], 'purge_prefix')
        if out is None:
            self.log('purge_prefix: FAILED. objects_measured is now a lower '
                     'bound, not an exact count -- compare it against '
                     'put_count rather than num_blobs for this row.')
            return
        try:
            self.purged_count = int(out.split()[0])
        except Exception:
            self.log(f'purge_prefix: unparseable output: {out!r}')
            return
        if self.purged_count:
            self.log(f'purge_prefix: deleted {self.purged_count} stale '
                     f'object(s) under s3://{bucket}/{prefix} left by an '
                     f'earlier row')
        else:
            self.log(f'purge_prefix: s3://{bucket}/{prefix} was already clean')

    def _measure_objects(self):
        """
        List the bdev's key prefix and record how many objects exist.

        WHY THIS IS NOT DERIVED. The benchmark reports `PUT count` as the blob
        count, which is right only while the allocator satisfies each request
        with one block. It previously derived ceil(blob_size / block_size)
        from a knob that configured nothing, and overstated the count 4x. A
        derived number that cannot be wrong-detected is worse than no number,
        so this lists the bucket and reports ground truth alongside it. When
        objects_measured != put_count the allocator fragmented -- or, if it is
        zero, nothing reached S3 at all and the throughput column is fiction.

        The count is exact only because `_purge_prefix` emptied the prefix
        before the run. With the purge disabled or failed it is a lower bound
        that includes earlier rows' leftovers.

        Never raises: a listing problem must not fail a row whose measurement
        already succeeded.
        """
        bucket, prefix = self._bucket_target('objects_measured')
        if not bucket:
            return
        script = (
            'import os, sys, botocore.session\n'
            'b, p = sys.argv[1], sys.argv[2]\n'
            'c = botocore.session.get_session().create_client(\n'
            '    "s3", region_name=os.environ.get("AWS_DEFAULT_REGION"))\n'
            'n = tot = 0\n'
            'for page in c.get_paginator("list_objects_v2").paginate(\n'
            '        Bucket=b, Prefix=p):\n'
            '    for o in page.get("Contents", []):\n'
            '        n += 1; tot += o["Size"]\n'
            'print(n, tot)\n')
        out = self._run_botocore(script, [bucket, prefix], 'objects_measured',
                                 timeout=180)
        if out is None:
            return
        try:
            count, total = out.split()[:2]
            int(count), int(total)
        except Exception:
            self.log(f'objects_measured: unparseable listing output: {out!r}')
            return
        with open(self.objects_path, 'w') as f:
            f.write(f'objects_measured {count}\nbytes_measured {total}\n')
            # Diagnostic, and the reason the purge is not silent: a nonzero
            # value in results.csv names the row whose predecessor leaked.
            if self.purged_count is not None:
                f.write(f'objects_purged {self.purged_count}\n')
        self.log(f'objects_measured: {count} objects, {total} bytes under '
                 f's3://{bucket}/{prefix}')
        if int(count) == 0:
            self.log('WARNING: the bdev prefix is EMPTY after a run the '
                     'benchmark called successful. Nothing reached S3; treat '
                     'the throughput columns as invalid.')

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
        for path in (self.output_path, self.rss_path, self.objects_path):
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
        # Ground truth from the bucket, written by start(). Compare against
        # <pkg>.write.put_count: equal means one object per blob as expected,
        # zero means nothing reached S3 and the row is fiction.
        objects_path = os.path.join(self.shared_dir,
                                    'clio_s3_write_objects.txt')
        if os.path.exists(objects_path):
            try:
                with open(objects_path, 'r') as f:
                    for line in f:
                        parts = line.split()
                        if len(parts) == 2:
                            stat_dict[f'{self.pkg_id}.write.{parts[0]}'] = \
                                int(parts[1])
            except Exception as e:
                self.log(f'Could not read {objects_path}: {e}')
        if found == 0:
            self.log(f'Warning: no metrics extracted from {output_path} '
                     f'({len(output)} bytes). A green row with a blank '
                     f'throughput column is a FAILURE.')
