"""
This module provides classes and methods to deploy JuiceFS as a Jarvis
service package.

JuiceFS is a POSIX-compatible distributed filesystem that stores file
*data* in an object store (here a local directory via ``--storage file``)
and file *metadata* in a transactional engine (here Redis). This package
formats the filesystem once, mounts it via FUSE, and unmounts it on stop --
mirroring the lifecycle style of the ``redis`` service package.

The expected deployment order is: redis -> juicefs -> <benchmark driver>,
so the Redis metadata engine is already accepting connections before
``juicefs format`` runs.
"""
import os
import time
import shutil
from jarvis_cd.core.pkg import Application
from jarvis_cd.util.logger import Color
from jarvis_cd.shell import Exec, LocalExecInfo


class Juicefs(Application):
    """
    Format and FUSE-mount a JuiceFS filesystem (Redis metadata +
    local-file object backend) for single-node benchmarking.
    """

    def _init(self):
        """
        Initialize paths. Concrete (env-expanded) paths are resolved in
        ``_configure`` and recomputed on demand via ``_paths``.
        """
        pass

    def _configure_menu(self):
        """
        Create a CLI menu for the configurator method.

        :return: List(dict)
        """
        return [
            {
                'name': 'meta_url',
                'msg': 'Metadata engine URL (Redis) for format/mount',
                'type': str,
                'default': 'redis://127.0.0.1:6379/1',
            },
            {
                'name': 'storage',
                'msg': 'Object storage backend type',
                'type': str,
                'default': 'file',
            },
            {
                'name': 'data_dir',
                'msg': 'Object store bucket directory (--bucket for storage=file)',
                'type': str,
                'default': '${HOME}/juicefs_data',
            },
            {
                'name': 'mountpoint',
                'msg': 'FUSE mountpoint directory',
                'type': str,
                'default': '${HOME}/juicefs_mnt',
            },
            {
                'name': 'name',
                'msg': 'JuiceFS volume name (passed to juicefs format)',
                'type': str,
                'default': 'jfsbench',
            },
            {
                'name': 'cache_dir',
                'msg': 'Local cache directory for the mount',
                'type': str,
                'default': '${HOME}/juicefs_cache',
            },
            {
                'name': 'juicefs_bin',
                'msg': 'Path to the juicefs binary',
                'type': str,
                'default': 'juicefs',
            },
            {
                'name': 'format_fresh',
                'msg': 'Wipe the bucket directory before formatting each run',
                'type': bool,
                'default': True,
            },
            {
                'name': 'mount_wait',
                'msg': 'Seconds to wait for the mount to become ready',
                'type': int,
                'default': 20,
            },
            {
                'name': 'extra_mount_opts',
                'msg': 'Extra options appended to juicefs mount',
                'type': str,
                'default': '',
            },
        ]

    def _paths(self):
        """
        Resolve env-expanded, user-expanded absolute paths from config.

        :return: tuple(data_dir, mountpoint, cache_dir)
        """
        def fix(p):
            return os.path.expanduser(os.path.expandvars(str(p)))
        return (fix(self.config['data_dir']),
                fix(self.config['mountpoint']),
                fix(self.config['cache_dir']))

    def _configure(self, **kwargs):
        """
        Validate config and ensure the data/mount/cache directories exist.

        :param kwargs: Configuration parameters for this pkg.
        :return: None
        """
        data_dir, mountpoint, cache_dir = self._paths()
        if not self.config['meta_url']:
            raise ValueError('juicefs: meta_url must be set')
        if self.config['storage'] != 'file':
            self.log(f"juicefs: storage='{self.config['storage']}' "
                     f"(only 'file' is exercised by this package)",
                     color=Color.YELLOW)
        for d in (data_dir, mountpoint, cache_dir):
            os.makedirs(d, exist_ok=True)

    def _is_mounted(self, mountpoint):
        """
        Report whether ``mountpoint`` currently has a filesystem mounted.

        :param mountpoint: Absolute mountpoint path.
        :return: bool
        """
        return os.path.ismount(mountpoint)

    def start(self):
        """
        Format (idempotent) and FUSE-mount the JuiceFS filesystem, then
        block until the mount is ready.

        :return: None
        """
        jfs = self.config['juicefs_bin']
        data_dir, mountpoint, cache_dir = self._paths()
        meta_url = self.config['meta_url']

        # Fresh slate: drop any orphaned chunks from a previous run. The
        # Redis metadata is wiped per-run by the redis package, so stale
        # chunks here would only be dead bytes.
        if self.config['format_fresh']:
            self.log(f'Clearing bucket dir {data_dir}', color=Color.YELLOW)
            shutil.rmtree(data_dir, ignore_errors=True)
            os.makedirs(data_dir, exist_ok=True)

        # Format the volume (safe to re-run against fresh metadata).
        fmt = [
            jfs, 'format',
            '--storage', self.config['storage'],
            '--bucket', data_dir,
            meta_url,
            self.config['name'],
        ]
        self.log(f"Formatting JuiceFS: {' '.join(fmt)}", color=Color.YELLOW)
        Exec(' '.join(fmt), LocalExecInfo(env=self.mod_env)).run()

        # Mount via FUSE. --background daemonizes and returns once the
        # mount is registered.
        mnt = [
            jfs, 'mount',
            meta_url,
            mountpoint,
            '--cache-dir', cache_dir,
            '--background',
        ]
        if self.config['extra_mount_opts']:
            mnt.append(self.config['extra_mount_opts'])
        self.log(f"Mounting JuiceFS: {' '.join(mnt)}", color=Color.YELLOW)
        Exec(' '.join(mnt), LocalExecInfo(env=self.mod_env)).run()

        # Wait for the mount to actually be ready before downstream I/O.
        deadline = int(self.config['mount_wait'])
        for _ in range(deadline):
            if self._is_mounted(mountpoint):
                self.log(f'JuiceFS mounted at {mountpoint}', color=Color.GREEN)
                return
            time.sleep(1)
        raise RuntimeError(
            f'juicefs: mountpoint {mountpoint} not ready after '
            f'{deadline}s (check redis connectivity and /dev/fuse access)')

    def stop(self):
        """
        Unmount the JuiceFS filesystem.

        :return: None
        """
        _, mountpoint, _ = self._paths()
        jfs = self.config['juicefs_bin']
        Exec(f'{jfs} umount {mountpoint}',
             LocalExecInfo(env=self.mod_env)).run()
        # Fallback for a wedged mount; ignore failure if already gone.
        if self._is_mounted(mountpoint):
            Exec(f'fusermount -u {mountpoint}',
                 LocalExecInfo(env=self.mod_env)).run()

    def clean(self):
        """
        Unmount (if needed) and delete the bucket and cache directories.

        :return: None
        """
        data_dir, mountpoint, cache_dir = self._paths()
        if self._is_mounted(mountpoint):
            self.stop()
        for d in (data_dir, cache_dir):
            shutil.rmtree(d, ignore_errors=True)
