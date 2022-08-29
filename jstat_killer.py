import os
import signal
import concurrent.futures

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
from jstat_capture import get_secret


def jstat_kill(ip):
    """
    kills all running jstat processes worker and removes the tmp/jstat_output directory from worker nodes
    :return: none
    """
    pkey = get_secret()

    private_key_str = io.StringIO()
    private_key_str.write(pkey)
    private_key_str.seek(0)

    key = paramiko.RSAKey.from_private_key(private_key_str)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


    pids = []
    ssh.connect(
        hostname=ip,
        username=user,
        pkey=key,
        allow_agent=False,
        look_for_keys=False
    )
    stdin, stdout, stderr = ssh.exec_command(f'ls /tmp/jstat_output')
    pids.append(stdout.read())

    ssh.exec_command(f'sudo rm -r {csv_save}', timeout=1)
    # print(stderr.read(), file=sys.stderr)
    for pid in pids:
        stdin, stdout, stderr = ssh.exec_command(f'sudo kill -9 {pid[6:10]}', timeout=1) # make sure this works
        # print(stderr.read(), file=sys.stderr)
        time.sleep(2)
    shutil.rmtree(csv_save, ignore_errors=True)
    shutil.rmtree(graph_dir, ignore_errors=True)


def process_kill():
    '''
    kills main.py process
    :return: none
    '''
    try:
        for ps_output in os.popen(f"ps ax | grep -i 'main.py' | grep -v grep"):
            fields = ps_output.split()

            kill_pid = fields[0]

            os.kill(int(kill_pid), signal.SIGTERM)
    except:
        print('Error while killing main.py')


if __name__ == '__main__':
    ip_list = node_ips()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futureSSH = {executor.submit(jstat_kill, ip): ip for ip in ip_list}
        for future in concurrent.futures.as_completed(futureSSH):
            print(f'/tmp/jstat_output removed and jstat stopped on: {futureSSH[future]}')
    process_kill()

