# (b'ip-172-31-80-219.ec2.internal\n', None)

import subprocess
import paramiko
import os
import time

from config import search_term
from config import remote_pem_path
from config import bash_script_path

PEM = remote_pem_path


def get_slave_node_ip(path=bash_script_path):
    os.chmod(path=path, mode=0o755)
    subprocess.call(path)
    print('done')
print(get_slave_node_ip())

def jps_command(ips):
    """
    returns the jps output for the requested search-term for each instance in each cluster
    formatted to only present PID.
    :return:
    lists within list [[pid per instance]]
    """

    command = f"sudo jps | grep '{search_term}' | tr -d [:alpha:]"


    k = paramiko.RSAKey.from_private_key_file(f'{PEM}')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


    username = 'ec2-user'

    standard_jps_output = []
    for ip in ips:
        ssh.connect(
            hostname=ip,
            username=username,
            pkey=k,
            allow_agent=False,
            look_for_keys=False
        )
        stdin, stdout, stderr = ssh.exec_command(command)
        standard_jps_output.append(stdout.read())
        ssh.close()
#
#     PIDs = []
#     for j in range(len(pids_by_cluster)):
#         throw = []
#         for i in pids_by_cluster[j]:
#             oof = i.decode("utf-8")
#             oof = oof.replace('\n', '').strip().split()
#             throw.append((oof))
#         PIDs.append(throw)
#         del throw
#
#     return PIDs
#
#
# def jstat_starter():
#     k = paramiko.RSAKey.from_private_key_file(f'{PEM}')
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#
#     USERNAME = 'ec2-user'
#     ips = nested_instance_ip()
#     clusters = cluster_id()
#     PIDs = jps_command()
#
#
#     for cl in range(len(clusters)):
#         for ip in range(len(ips[cl])):
#             ssh.connect(
#                 hostname=f'{ips[cl][ip]}',
#                 username=USERNAME,
#                 pkey=k,
#                 allow_agent=False,
#                 look_for_keys=False
#             )
#
#             for pid in PIDs[cl][ip]:
#                 print(ip, pid)
#                 ssh.exec_command(f'mkdir -p /tmp/jstat_output && sudo jstat -gcutil {pid} 10000 > /tmp/jstat_output/jstat_{pid} &', timeout=1)
#                 time.sleep(5)
#
#
# if __name__ == '__main__':
#     jstat_starter()
#
