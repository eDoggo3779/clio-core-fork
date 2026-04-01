"""
Mofka Server Service — starts and stops the Bedrock/Mofka daemon,
creates a topic and partition so that downstream benchmark packages
can produce and consume events.
"""
from jarvis_cd.core.pkg import Service
from jarvis_cd.shell import Exec, LocalExecInfo
from jarvis_cd.shell.process import Kill
import os
import json
import time
import shutil


class MofkaServer(Service):
    """Manages the Mofka/Bedrock daemon lifecycle."""

    def _init(self):
        self.bedrock_pid_file = None

    def _configure_menu(self):
        return [
            {
                'name': 'protocol',
                'msg': 'Mercury transport protocol',
                'type': str,
                'default': 'tcp',
                'choices': ['tcp', 'na+sm', 'ofi+tcp', 'ofi+verbs', 'ofi+cxi'],
            },
            {
                'name': 'workdir',
                'msg': 'Working directory for Mofka runtime files',
                'type': str,
                'default': '/tmp/mofka-bench',
            },
            {
                'name': 'topic',
                'msg': 'Mofka topic name to create',
                'type': str,
                'default': 'benchmark_topic',
            },
            {
                'name': 'partition_type',
                'msg': 'Partition storage type',
                'type': str,
                'default': 'memory',
                'choices': ['memory'],
            },
        ]

    def _configure(self, **kwargs):
        workdir = self.config['workdir']
        group_file = os.path.join(workdir, 'mofka.json')
        driver_config = os.path.join(workdir, 'driver_config.json')
        self.bedrock_pid_file = os.path.join(workdir, 'mofka.pid')

        self.setenv('MOFKA_PROTOCOL', self.config['protocol'])
        self.setenv('MOFKA_WORKDIR', workdir)
        self.setenv('MOFKA_GROUP_FILE', group_file)
        self.setenv('MOFKA_TOPIC', self.config['topic'])
        self.setenv('MOFKA_DRIVER_CONFIG', driver_config)

        # Activate spack environment paths if available
        spack_env_view = '/opt/spack-env/.spack-env/view'
        if os.path.isdir(spack_env_view):
            self.prepend_env('PATH', os.path.join(spack_env_view, 'bin'))
            self.prepend_env(
                'LD_LIBRARY_PATH',
                os.path.join(spack_env_view, 'lib'),
            )
            self.prepend_env(
                'LD_LIBRARY_PATH',
                os.path.join(spack_env_view, 'lib64'),
            )
            # Auto-detect Python version for site-packages
            lib_dir = os.path.join(spack_env_view, 'lib')
            for entry in os.listdir(lib_dir):
                if entry.startswith('python3.'):
                    python_site = os.path.join(
                        lib_dir, entry, 'site-packages'
                    )
                    if os.path.isdir(python_site):
                        self.prepend_env('PYTHONPATH', python_site)
                        break

        self.log('Mofka server configured')

    def start(self):
        workdir = self.config['workdir']
        group_file = os.path.join(workdir, 'mofka.json')
        config_path = os.path.join(self.pkg_dir, 'config', 'config.json')

        # Step 1: Prepare working directory
        if os.path.exists(workdir):
            shutil.rmtree(workdir)
        os.makedirs(workdir, exist_ok=True)

        # Step 2: Start bedrock daemon
        self.log(f'Starting bedrock: protocol={self.config["protocol"]}')
        cmd = f'bedrock {self.config["protocol"]} -c {config_path}'
        Exec(cmd, LocalExecInfo(
            env=self.mod_env,
            cwd=workdir,
            exec_async=True,
        )).run()

        # Wait for group file
        self.log('Waiting for mofka.json...')
        for i in range(30):
            if os.path.exists(group_file):
                break
            time.sleep(1)
        else:
            raise RuntimeError(
                f'mofka.json did not appear after 30s in {workdir}'
            )
        self.log('Bedrock is ready')

        # Step 3: Create topic
        topic = self.config['topic']
        self.log(f'Creating topic: {topic}')
        Exec(
            f'python -m mochi.mofka.mofkactl topic create {topic}'
            f' -g {group_file}',
            LocalExecInfo(env=self.mod_env),
        ).run()

        # Step 4: Add partition
        self.log('Adding memory partition (rank 0)')
        Exec(
            f'python -m mochi.mofka.mofkactl partition add {topic}'
            f' -r 0 -t {self.config["partition_type"]}'
            f' -g {group_file}',
            LocalExecInfo(env=self.mod_env),
        ).run()

        # Step 5: Write driver config
        driver_config_path = os.path.join(workdir, 'driver_config.json')
        driver_cfg = {
            'group_file': group_file,
            'margo': {'use_progress_thread': True},
        }
        with open(driver_config_path, 'w') as f:
            json.dump(driver_cfg, f, indent=4)
        self.log(f'Driver config written to {driver_config_path}')

        self.log('Mofka server is ready for benchmarks')

    def stop(self):
        self.log('Stopping bedrock daemon')
        Kill('bedrock', LocalExecInfo(env=self.mod_env), partial=True).run()
        time.sleep(1)

    def clean(self):
        workdir = self.config.get('workdir', '/tmp/mofka-bench')
        if os.path.exists(workdir):
            shutil.rmtree(workdir)
            self.log(f'Removed working directory: {workdir}')
