import boto3
import paramiko

from config import remote_pem_path
from config import USER
from config import local_pem_path

REMOTE_PATH = remote_pem_path
PEM = local_pem_path
USERNAME = USER

#
# PEM = KEYPATH


def cluster_menu():
    """
    gets all cluster IDs
    :return: list
    """
    emr = boto3.client('emr')  # set the boto3 variable

    # check for all running clusters
    clusters = emr.list_clusters(
        ClusterStates=[
            'RUNNING',
            'STARTING',
            'WAITING'
        ]
    )

    cl_id = []
    for num, cluster in enumerate(clusters["Clusters"]):
        cl_id.append(clusters["Clusters"][num]["Id"])  # every cluster id is logged.
    cl_menu = []
    for i, cl in enumerate(cl_id):
        cl_menu.append(str(f'{i + 1}: {cl}'))
    return cl_menu


def choose_clusters(clusters=cluster_menu()): #TODO : Clean this up
    print(clusters)
    choice =''
    cl_checks = []
    while choice != 0:
        choice = int(input("Enter a Cluster number to upload PEM Key, to exit press 0\n"))
        if choice == 0:
            break
        cl_checks.append(clusters[choice - 1][3::])
        print(f'you chose {cl_checks}')
        if len(clusters) == 1:
            break
    return cl_checks


def master_ip(cluster=choose_clusters()):
    emr = boto3.client('emr')
    master_ip = []
    for cl in cluster:
        master_ips = emr.list_instances(
            ClusterId=cl,
            InstanceGroupTypes=['MASTER']
        )
        master_ip.append(master_ips["Instances"][0]["PublicIpAddress"])
    return master_ip


def sftp_transfer(ip, username, PEM, REMOTE_PATH):
    '''
    Establishes ssh connect to execute commands
    :param ip: list of ips to ssh into
    :param username: username with which to log-in
    :param PEM: path to pem key
    :return: connection object
    '''
    for address in ip:
        k = paramiko.RSAKey.from_private_key_file(PEM)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=address,
            username=username,
            pkey=k,
            allow_agent=False,
            look_for_keys=False
        )
        print(f'connected to master: {address}')
        try:
            sftp = ssh.open_sftp()
            sftp.put(localpath=PEM, remotepath=REMOTE_PATH, confirm=True)
            sftp.chmod(path=REMOTE_PATH, mode=0o400)
            ssh.close()
        except IOError:
            print('Key file already exits on the Master node')
            ssh.close()
            continue


if __name__ == '__main__':
    sftp_transfer(ip=master_ip(), username=USERNAME, PEM=PEM, REMOTE_PATH=REMOTE_PATH)
