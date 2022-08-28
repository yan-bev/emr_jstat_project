import subprocess
import paramiko
import os
import time
from itertools import chain
import boto3
import io
import concurrent.futures
from botocore.exceptions import ClientError
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

bash_script_path = config['cnfg']['BashScriptPath']
ip_text_path = config['cnfg']['IpTextPath']
region_name = config['cnfg']['RegionName']
secret_name = config['cnfg']['SecretName']
user = config['cnfg']['User']
search_term = config['cnfg']['SearchTerm']
csv_save = config['cnfg']['CsvSave']



def populate_node_ip(path=bash_script_path):
    '''
    Runs the IP populator bash script
    :param path: path/to/file
    :return: none
    '''
    os.chmod(path=path, mode=0o755) # changes mode to exec
    subprocess.call(path)  # runs the exec


def node_ips(filepath=ip_text_path):
    '''
    creates a variable which contains all the Worker IPs in the cluster, then deletes the text file.
    :param filepath: to created Ip text list
    :return: variable list
    '''
    populate_node_ip() # populates the ip_text_path.txt file
    ip_list = open(filepath).read().splitlines()
    ip_list = set(ip_list)
    ip_list = (list(ip_list))
    os.remove(filepath)
    return ip_list

def get_secret():
    '''
    pulls the secret from Secret Manager
    :return: secret as string
    '''
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

def jps_command_and_starter(ip):
    """
    returns the jps output for the requested search-term for each instance in the cluster
    formatted to only present PID.
    :return:
     [pid per instance][pids per instance]
    """
    key = get_secret()
    get_PIDs = f"sudo jps | grep -i '{search_term}' | tr -d [:alpha:]"
    # jstat_start = f'mkdir -p {csv_save} && sudo jstat -gcutil {pid} 10000 > {csv_save}/jstat_{pid} &'
    private_key_str = io.StringIO()
    private_key_str.write(key)
    private_key_str.seek(0)

    key = paramiko.RSAKey.from_private_key(private_key_str)
    private_key_str.close()
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    standard_jps_output = []
    ssh.connect(
            hostname=ip,
            username=user,
            pkey=key,
            allow_agent=False,
            look_for_keys=False
        )
    print(f'connected to {ip}')
    stdin, stdout, stderr = ssh.exec_command(get_PIDs)
    standard_jps_output.append(stdout.read())
    print(standard_jps_output)
    # TODO: See what this output looks like, and see if i can make the PIDs usable without resorting to python.

    PIDs = []
    # TODO: try and turn this into list comprehension
    for pid in standard_jps_output:
        pid = pid.decode("utf-8").replace('\n', '').strip().split()
        PIDs.append(pid)
        print(pid)

    for pid in PIDs:
        ssh.exec_command(f'mkdir -p {csv_save} && sudo jstat -gcutil {pid} 10000 > {csv_save}/jstat_{pid} &', timeout=1)
        # print(f'testing: jstat has begun on {pid}')
    ssh.close()


if __name__ == '__main__':
    ip_list = node_ips()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futureSSH = {executor.submit(jps_command_and_starter, ip): ip for ip in ip_list}
        for future in concurrent.futures.as_completed(futureSSH):
            print(f'jstat begun on:{futureSSH[future]}')
