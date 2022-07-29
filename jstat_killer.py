import paramiko
import time
from filepath import KEYPATH
import sys
PEM = KEYPATH

from jstat_capture import nested_instance_ip
from jstat_capture import cluster_id
from jstat_capture import jps_command

def jstat_kill():
    """
    kills all running jstat processes from jstat_capture
    also removes the tmp/jstat_output directory
    :return:
    """
    k = paramiko.RSAKey.from_private_key_file(f'{PEM}')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    USERNAME = 'ec2-user'
    ips = nested_instance_ip()
    clusters = cluster_id()
    PIDs = jps_command()

    for cl in range(len(clusters)):
        for ip in range(len(ips[cl])):
            ssh.connect(
                hostname=f'{ips[cl][ip]}',
                username=USERNAME,
                pkey=k,
                allow_agent=False,
                look_for_keys=False
            )
            stdin, stdout, stderr = ssh.exec_command('sudo rm -r /tmp/jstat_output', timeout=1)
            print(f'removing /tmp/jstat_output in {ips[cl][ip]}')
            # print(stderr.read(), file=sys.stderr)
            for pid in PIDs[cl][ip]:
                stdin, stdout, stderr = ssh.exec_command(f'sudo kill -9 {pid}', timeout=1)
                print(f'killing: {pid}')
                # print(stderr.read(), file=sys.stderr)
                time.sleep(5)

if __name__ == '__main__':
    jstat_kill()

