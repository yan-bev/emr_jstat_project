import boto3
import paramiko
import nums_from_string
import time

import subprocess, signal
import os
# import sys

# create here function

def get_ips():

    emr = boto3.client('emr')  # set the boto3 variable

    # check for all running clusters
    clusters = emr.list_clusters(
        ClusterStates=[
            'RUNNING'
        ]
    )
    for cluster in clusters["Clusters"]:
        cluster_id = (clusters["Clusters"][0]["Id"])  # for multiple clusters this will need to be changed.
        # print(cluster_id)

        instances = emr.list_instances( # don't use the same var name.
            ClusterId=f"{cluster_id}"
    )

counter = 0
ec2_instance_ip_list = []
try:  # try this as a for loop, for ec2.... response [Instances]
    while True:
        ec2_instance_ip_list.append(response["Instances"][counter]["PublicIpAddress"])
        counter += 1
except:
    pass
print(ec2_instance_ip_list)

counter = 0
ec2_instance_id_list = []  # this should be without _list
try:
    while True:
        ec2_instance_id_list.append(response["Instances"][counter]["Ec2InstanceId"])
        counter += 1
except:
    pass
ec2_instance_id_last_4_chars = []
for i in ec2_instance_id_list:
    ec2_instance_id_last_4_chars.append(i[-4:])



        # TODO: you have to set the ec2 instance to allow you to ssh directly into root.
command = "jps | grep 'Main' | tr -d [:alpha:]"
    # "sudo",   # this depends on if the default user can 'see' the processes.



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
