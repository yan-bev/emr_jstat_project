import subprocess
import paramiko
import os
import time
import boto3
import io
from botocore.exceptions import ClientError

from config import csv_save
from config import search_term
from config import master_key_path
from config import secret_name
from config import region_name
from config import bash_script_path
from config import ip_text_path
from config import user


def populate_node_ip(path=bash_script_path):
    os.chmod(path=path, mode=0o755) # changes mode to exec
    subprocess.call(path)  # runs the exec


def node_ips(filepath=ip_text_path):
    populate_node_ip() # populates the ip_text_path.txt file
    ip_list = open(filepath).read().splitlines()
    ip_list = set(ip_list)
    ip_list = (list(ip_list))
    os.remove(filepath)
    return ip_list

def get_secret():
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    else:
        secret = get_secret_value_response['SecretString']
    return secret

def jps_command(ip_list=node_ips(), user=user, key=get_secret()):
    """
    returns the jps output for the requested search-term for each instance in each cluster
    formatted to only present PID.
    :return:
    lists within list [[pid per instance]]
    """


    command = f"sudo jps | grep -i '{search_term}' | tr -d [:alpha:]"

    private_key_str = io.StringIO()
    private_key_str.write(key)
    private_key_str.seek(0)

    key = paramiko.RSAKey.from_private_key(private_key_str)
    private_key_str.close()
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


    standard_jps_output = []
    for ip in ip_list:
        ssh.connect(
            hostname=ip,
            username=user,
            pkey=key,
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


def jstat_starter(ip_list=node_ips(), PIDs=jps_command(), key=get_secret()):
    """

    :param ip_list:
    :param PIDs:
    :param key:
    :return:
    """
    private_key_str = io.StringIO()
    private_key_str.write(key)
    private_key_str.seek(0)

    key = paramiko.RSAKey.from_private_key(private_key_str)
    private_key_str.close()
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for i, ip in enumerate(ip_list):
        ssh.connect(
            hostname=f'{ip}',
            username=user,
            pkey=key,
            allow_agent=False,
            look_for_keys=False
        )

        for pid in PIDs[i]:
            print(ip, pid)
            ssh.exec_command(f'mkdir -p {csv_save} && sudo jstat -gcutil {pid} 10000 > {csv_save}/jstat_{pid} &', timeout=1)
            time.sleep(5)


if __name__ == '__main__':
    jstat_starter()

