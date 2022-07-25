import boto3
import paramiko
import re
import nums_from_string
import time

from filepath import KEYPATH
PEM = KEYPATH


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
            'STARTING'
        ]
    )

    cl_id = []
    for num_of_clusters in range(len(clusters["Clusters"])):
        cl_id.append(clusters["Clusters"][num_of_clusters]["Id"])  # every cluster id is logged.
    return cl_id


def instance_ip():
    """
    gets all instance IPs and sorts them by list per cluster
    []
    :return: list
    """
    emr = boto3.client('emr')  # set the boto3 variable

    ec2s = []
    for cl in cluster_id():
        global instances
        instances = emr.list_instances(
            ClusterId=f"{cl}"
        )

        for i in range(len(instances["Instances"])):
            ec2s.append(instances["Instances"][i]["PublicIpAddress"])

    return ec2s

def cluster_label():
    """
    returns the last four chars of every cluster in a list
    """
    last_four_chars_cluster = []
    for id in cluster_id():
        last_four_chars_cluster.append(id[-4:])
    return last_four_chars_cluster


def instance_label():
    """
    gives the last four digits of every instance ID.
    :return:
    """
    emr = boto3.client('emr')  # set the boto3 variable
    ec2_instances = []
    for cl in cluster_id():
        instances = emr.list_instances(
            ClusterId=f"{cl}"
        )

        for i in range(len(instances["Instances"])):
            ec2_instances.append(instances["Instances"][i]["Ec2InstanceId"][-4:])

    return ec2_instances


def list_breaker(jps_output):
    """
    this breaks the list up into a list of lists
    """
    res = []
    for ind in jps_output:
        for i in ind:
            sub = i.split(b',')
            res.append(sub)
    return (res)


def jps_command():
    """
    returns the jps output for the requested search-term for each instance in each cluster
    formatted to only present PID.
    :return:
    lists within list [[[pid per instance per cluster]]]
    """
    SEARCH_TERM = 'Main'
    command = f"jps | grep '{SEARCH_TERM}' | tr -d [:alpha:]"
    nested_ips = nested_instance_ip()
    clid = cluster_id()



    k = paramiko.RSAKey.from_private_key_file(f'{PEM}')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


    username = 'root'  # generally this should be ec2-user, but for me proccesses only worked with root.

    ips = instance_ip()

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


k = paramiko.RSAKey.from_private_key_file(f'{PEM}')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
USERNAME = 'root'
ips = nested_instance_ip()
clusters = cluster_id()


# ips = instance_ip()
PIDs = jps_command()
pid = ''
jstat_command = f'mkdir -p /tmp/jstat_output && jstat -gcutil {pid} 250 7'
remote_destination = r'/tmp/jstat_output/jstat_{pid}'

#
if __name__ == '__main__':
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
                stdin, stdout, stderr = ssh.exec_command(f'mkdir -p /tmp/jstat_output && jstat -gcutil {pid} 250 7 > /tmp/jstat_output/jstat_{pid} &', timeout=1)  # can you nest f'strings'?
                print(pid)
                time.sleep(5)
