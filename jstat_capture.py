# (b'ip-172-31-80-219.ec2.internal\n', None)

import subprocess
import paramiko
import os
import time

from config import search_term
from config import remote_pem_path
from config import bash_script_path
from config import ip_text_path
from config import user


PEM = remote_pem_path
FILEPATH = ip_text_path
USERNAME = user


def populate_node_ip(path=bash_script_path):
    os.chmod(path=path, mode=0o755) #changes mode to exec
    subprocess.call(path)  # runs the exec

def node_ips(filepath=FILEPATH):
    populate_node_ip() # populates the ip_text_path.txt file
    ip_list = open(filepath).read().splitlines()
    ip_list = set(ip_list)
    ip_list = (list(ip_list))
    os.remove(filepath)
    return ip_list

def jps_command(ip_list=node_ips(), username=USERNAME ):
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


    standard_jps_output = []
    for ip in ip_list:
        ssh.connect(
            hostname=ip,
            username=USERNAME,
            pkey=k,
            allow_agent=False,
            look_for_keys=False
        )
        stdin, stdout, stderr = ssh.exec_command(command)
        standard_jps_output.append(stdout.read())
        ssh.close()
        # print(standard_jps_output)

    PIDs = []
    for pid in standard_jps_output:
        # throw = []
        throw = pid.decode("utf-8").replace('\n', '').strip().split()
        PIDs.append(throw)
        del throw
    return PIDs

print(jps_command())


def jstat_starter(ip_list=node_ips(), PIDs=jps_command()):
    k = paramiko.RSAKey.from_private_key_file(f'{PEM}')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for i, ip in enumerate(ip_list):
        ssh.connect(
            hostname=f'{ip}',
            username=USERNAME,
            pkey=k,
            allow_agent=False,
            look_for_keys=False
        )

        for pid in PIDs[i]:
            print(ip, pid)
            ssh.exec_command(f'mkdir -p /tmp/jstat_output && sudo jstat -gcutil {pid} 10000 > /tmp/jstat_output/jstat_{pid} &', timeout=1)
            time.sleep(5)


if __name__ == '__main__':
    jstat_starter()

