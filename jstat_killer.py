import os
import signal

import paramiko
import time
import shutil
import io
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


user = config['cnfg']['User']
csv_save = config['cnfg']['CsvSave']
graph_dir = config['cnfg']['GraphDir']


from jstat_capture import node_ips
from jstat_capture import jps_command
from jstat_capture import get_secret


def jstat_kill(pkey=get_secret()):
    """
    kills all running jstat processes from jstat_capture
    also removes the tmp/jstat_output directory
    :return:
    """

    private_key_str = io.StringIO()
    private_key_str.write(pkey)
    private_key_str.seek(0)

    key = paramiko.RSAKey.from_private_key(private_key_str)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


    ips = node_ips()
    pids = jps_command()

    for i, ip in enumerate(ips):
        ssh.connect(
            hostname=ip,
            username=user,
            pkey=key,
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

