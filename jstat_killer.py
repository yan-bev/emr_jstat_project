import os
import signal

import paramiko
import time
import shutil

from config import user
from config import csv_save
from config import master_key_path
from config import graph_dir
from config import search_term

from jstat_capture import node_ips
from jstat_capture import jps_command


def jstat_kill():
    """
    kills all running jstat processes from jstat_capture
    also removes the tmp/jstat_output directory
    :return:
    """
    k = paramiko.RSAKey.from_private_key_file(master_key_path)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


    ips = node_ips()
    pids = jps_command()

    for i, ip in enumerate(ips):
        ssh.connect(
            hostname=ip,
            username=user,
            pkey=k,
            allow_agent=False,
            look_for_keys=False
        )
        ssh.exec_command(f'sudo rm -r {csv_save}', timeout=1)
        print(f'removing {csv_save} in {ip}')
        # print(stderr.read(), file=sys.stderr)
        for pid in pids[i]:
            stdin, stdout, stderr = ssh.exec_command(f'sudo kill -9 {pid}', timeout=1)
            print(f'killing: {pid}')
            # print(stderr.read(), file=sys.stderr)
            time.sleep(5)
    shutil.rmtree(csv_save)
    shutil.rmtree(graph_dir)


def process_kill():
    try:
        for ps_output in os.popen(f"ps ax | grep -i 'main.py' | grep -v grep"):
            fields = ps_output.split()

            kill_pid = fields[0]

            os.kill(int(kill_pid), signal.SIGTERM)
    except:
        print('Error while killing main.py')

if __name__ == '__main__':
    jstat_kill()
    process_kill()

