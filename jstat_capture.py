import boto3
import paramiko
import nums_from_string
import time

import subprocess, signal
import os
# import sys


def get_aws_info_by_cluster():
    """ uses boto3 to list out cluster and instance information
        0 - returns a list of clusters and ips in the form of
        [cluster1, [ip1, ip2, ipn], [cluster2], [ip1, ip2, ipn]...]
        1 - returns the identifiers for clusters
        2 - returns the identifiers for ec2 Ids
    :return: lists
    """


    emr = boto3.client('emr')  # set the boto3 variable

    # check for all running clusters
    clusters = emr.list_clusters(
        ClusterStates=[
            'RUNNING',
            'STARTING'
            ]
        )
    last_four_clusterId = []

    cl_id = []
    for num_of_clusters in range(len(clusters["Clusters"])):
        cl_id.append(clusters["Clusters"][num_of_clusters]["Id"])  # every cluster id is logged.
    for cl in cl_id:
        last_four_clusterId.append(cl[-4:])

    ec2_instance_ips_by_cl = []
    last_four_instanceId = []
    for cl in cl_id:
        instances = emr.list_instances(
            ClusterId=f"{cl}"
        )
        ec2_instance_ips_by_cl.append(cl)
        ec2_by_cluster = []

        for i in range(len(instances["Instances"])):
            ec2_by_cluster.append(instances["Instances"][i]["PublicIpAddress"])

            last_four_instanceId.append(instances["Instances"][i]["Ec2InstanceId"][-4:])

        ec2_instance_ips_by_cl.append(ec2_by_cluster)
        del ec2_by_cluster

# ask Ido if i am better served by creating a class or creating multiple functions, or just this one function?
    return [ec2_instance_ips_by_cl, last_four_clusterId, last_four_instanceId]

        # TODO: you have to set the ec2 instance to allow you to ssh directly into root.

# what do I need here? I need to return the pids
SEARCH_TERM = 'Main'
command = f"jps | grep '{SEARCH_TERM}' | tr -d [:alpha:]"



k = paramiko.RSAKey.from_private_key_file(r'C:\\Users\yaniv\Documents\get-a-job\USeast1keypair.pem')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
standard_jps_output = []

username = 'root'  # generally this should be ec2-user, but for me proccesses only worked with root.

for InternetAddress in ec2_instance_ip_list:
    ssh.connect(
        hostname=InternetAddress,
        username=f'{username}',
        pkey=k,
        allow_agent=False,
        look_for_keys=False
    )
    stdin, stdout, stderr = ssh.exec_command(command)
    standard_jps_output.append(stdout.read())
    # print(stderr.read(), file=sys.stderr)

ssh.close()  # this should be within the for loop.

# print(standard_jps_output)


def listBreaker(jps_output):  # this breaks the list up into a list of lists
    res = []
    for ind in jps_output:
        sub = ind.split(b',')
        res.append(sub)
    return(res)

split_jps_list = (listBreaker(standard_jps_output))

PIDs = []
for i in split_jps_list:
    PIDs.append(nums_from_string.get_nums(str(i)))

print(PIDs)

OPEN_VAR = 'a'


if __name__ == '__main__':
    for num_of_ip in range(len(ec2_instance_ip_list)):
        ssh.connect(
            hostname=f'{ec2_instance_ip_list[num_of_ip]}',
            username=f'{username}',
            pkey=k,
            allow_agent=False,
            look_for_keys=False
        )
        for pid in PIDs[num_of_ip]:
            stdin, stdout, stderr = ssh.exec_command(f'mkdir -p /tmp/jstat_output && jstat -gcutil {pid} 250 7 > /tmp/jstat_output/jstat_{pid} &', timeout=1)
            print(pid)
            time.sleep(5)


            # for testing purposes
            # p = subprocess.Popen(['ps', '-A']), stdout=subprocess.PIPE)
            # out, err = p.communicate()
            #     for line
        ssh.close()
