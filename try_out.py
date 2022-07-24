import boto3
import paramiko
import nums_from_string
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
            'STARTING'
        ]
    )

    cl_id = []
    for num_of_clusters in range(len(clusters["Clusters"])):
        cl_id.append(clusters["Clusters"][num_of_clusters]["Id"])  # every cluster id is logged.
    return cl_id
print(cluster_id())


def instance_ip():
    """
    gets all instance IPs and sorts them by list per cluster
    [[ip1, ip2], [ip1, ip2]]
    :return: lists with list
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
        # ec2_by_cluster.append(ec2s)
        # del ec2s
    return ec2s
print(instance_ip())


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
    gives the last four digits of every instance ID separated by cluster.
    :return: [[],[],[]]
    """
    emr = boto3.client('emr')  # set the boto3 variable
    ec2_sep = []
    for cl in cluster_id():
        instances = emr.list_instances(
            ClusterId=f"{cl}"
        )
        ec2_instances = []
        for i in range(len(instances["Instances"])):
            ec2_instances.append(instances["Instances"][i]["Ec2InstanceId"][-4:])
        ec2_sep.append(ec2_instances)
        del ec2_instances
    return ec2_sep

# TODO: you have to set the ec2 instance to allow you to ssh directly into root.

def listBreaker(jps_output):
    """
    this breaks the list up into a list of lists
    """
    res = []
    for ind in jps_output:
        sub = ind.split(b',')
        res.append(sub)
    return (res)


def jps_command():
    """
    returns the jps output for the requested search-term for each instance in each cluster
    formatted to only present numbers.
    :return:
    list [[instance1(pid1, pid2)][instance2(pid1, pid2]]
    """
    SEARCH_TERM = 'Main'
    command = f"jps | grep '{SEARCH_TERM}' | tr -d [:alpha:]"



    k = paramiko.RSAKey.from_private_key_file(f'{PEM}')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    standard_jps_output = []

    username = 'root'  # generally this should be ec2-user, but for me proccesses only worked with root.
    ips = instance_ip()

    for ip in ips:
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

    split_jps_list = (listBreaker(standard_jps_output))
    PIDs = []
    for i in split_jps_list:
        PIDs.append(nums_from_string.get_nums(str(i)))
    return PIDs
print(jps_command())

k = paramiko.RSAKey.from_private_key_file(f'{PEM}')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
USERNAME = 'root'

ips = instance_ip()
PIDs = jps_command()


if __name__ == '__main__':
    for ip in range(len(ips)):
        ssh.connect(
            hostname=f'{ips[ip]}',
            username=USERNAME,
            pkey=k,
            allow_agent=False,
            look_for_keys=False
        )

        for pid in (PIDs[ip]):
            stdin, stdout, stderr = ssh.exec_command(f'mkdir -p /tmp/jstat_output && jstat -gcutil {pid} 250 7 > /tmp/jstat_output/jstat_{pid} &', timeout=1)
            print(pid)
            time.sleep(5)



                # for testing purposes
                # p = subprocess.Popen(['ps', '-A']), stdout=subprocess.PIPE)
                # out, err = p.communicate()
                #     for line




# # last_four_instanceId.append(instances["Instances"][i]["Ec2InstanceId"][-4:])