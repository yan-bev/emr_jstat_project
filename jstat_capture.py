import boto3
import paramiko
import time
from filepath import KEYPATH
PEM = KEYPATH

def cluster_id():
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
    # for i, cl in enumerate(cl_id):
    # print({k:v for k, v in enumerate(cl_id)})
    cl_menu = []
    for i, cl in enumerate(cl_id):
        cl_menu.append(str(f'{i + 1}: {cl}'))
    return cl_menu

def choose_clusters(clusters=cluster_id()):
    print(clusters)
    cl_checks = []
    choice = int(input("Choose a Cluster number to start Jstat, to exit press 0\n"))
    while choice != 0:
        cl_checks.append(clusters[choice - 1][3::])
        print(f'you chose: {cl_checks}')
        print(clusters)
        choice = int(input("choose an additional cluster? if not desired, enter '0' to exit\n"))
    return cl_checks


def instance_ip():
    """
    gets all instance IPs and sorts them by list per cluster
    []
    :return: list
    """
    emr = boto3.client('emr')  # set the boto3 variable

    ec2s = []
    for cl in choose_clusters():
        instances = emr.list_instances(
            ClusterId=f"{cl}"
        )

        for i, _ in enumerate(instances["Instances"]):
            ec2s.append(instances["Instances"][i]["PublicIpAddress"])
    return ec2s
print(instance_ip())


def nested_instance_ip():
    """
    gets all instance IPs and sorts them by list per cluster
    [[], []]
    :return: list
    """
    emr = boto3.client('emr')  # set the boto3 variable

    ec2s = []
    for cl in cluster_id():
        instances = emr.list_instances(
            ClusterId=f"{cl}"
        )
        throwout = []
        for i in range(len(instances["Instances"])):
            throwout.append(instances["Instances"][i]["PublicIpAddress"])
        ec2s.append(throwout)
        del throwout
    return ec2s


def cluster_label():
    """
    returns the last four chars of every cluster in a list
    """
    last_four_chars_cluster = []
    for id in cluster_id():
        last_four_chars_cluster.append(id[-4:])
    return last_four_chars_cluster


def nested_instance_label():
    """
    gives the instances labels sorted by cluster
    :return: [[], [], []]
    """
    emr = boto3.client('emr')  # set the boto3 variable
    ec2_label_by_cluster = []
    for cl in cluster_id():
        instances = emr.list_instances(
            ClusterId=f"{cl}"
        )
        throwaway = []
        for i in range(len(instances["Instances"])):
            throwaway.append(instances["Instances"][i]["Ec2InstanceId"][-4:])
        ec2_label_by_cluster.append(throwaway)
        del throwaway
    return ec2_label_by_cluster


def jps_command():
    """
    returns the jps output for the requested search-term for each instance in each cluster
    formatted to only present PID.
    :return:
    lists within list [[[pid per instance per cluster]]]
    """
    SEARCH_TERM = 'Main'
    command = f"sudo jps | grep '{SEARCH_TERM}' | tr -d [:alpha:]"
    nested_ips = nested_instance_ip()
    clid = cluster_id()



    k = paramiko.RSAKey.from_private_key_file(f'{PEM}')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


    username = 'ec2-user'


    pids_by_cluster = []
    for cl in range(len(clid)):
        standard_jps_output = []
        for ip in nested_ips[cl]:
            ssh.connect(
                hostname=ip,
                username=f'{username}',
                pkey=k,
                allow_agent=False,
                look_for_keys=False
            )
            stdin, stdout, stderr = ssh.exec_command(command)
            standard_jps_output.append(stdout.read())
            # print(stderr.read(), file=sys.stderr)
            ssh.close()
        pids_by_cluster.append(standard_jps_output)
        del standard_jps_output


    PIDs = []
    for j in range(len(pids_by_cluster)):
        throw = []
        for i in pids_by_cluster[j]:
            oof = i.decode("utf-8")
            oof = oof.replace('\n', '').strip().split()
            throw.append((oof))
        PIDs.append(throw)
        del throw

    return PIDs


def jstat_starter():
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

            for pid in PIDs[cl][ip]:
                print(ip, pid)
                ssh.exec_command(f'mkdir -p /tmp/jstat_output && sudo jstat -gcutil {pid} 10000 > /tmp/jstat_output/jstat_{pid} &', timeout=1)
                time.sleep(5)


if __name__ == '__main__':
    jstat_starter()

