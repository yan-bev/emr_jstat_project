import boto3
import paramiko
import nums_from_string
import concurrent.futures
import multiprocessing



emr = boto3.client('emr')  # set the boto3 variable

# check for all running clusters
response = emr.list_clusters(
    ClusterStates=[
        'RUNNING'
    ]
)
Cluster_Id = (response["Clusters"][0]["Id"])  # for multiple clusters this will need to be changed.
# print(Cluster_Id)

response = emr.list_instances(
    ClusterId=f"{Cluster_Id}"
)

counter = 0
ec2_instance_ip_list = []
try:
    while True:
        ec2_instance_ip_list.append(response["Instances"][counter]["PublicIpAddress"])
        counter += 1
except:
    pass
print(ec2_instance_ip_list)

# TODO: you have to set the ec2 instance to allow you to ssh directly into root.
commands = [
    # "sudo",   # this depends on if the default user can 'see' the processes.
    "jps | grep 'Main' | tr -d [:alpha:]"

]

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
    for command in commands:
        stdin, stdout, stderr = ssh.exec_command(command)
        standard_jps_output.append(stdout.read())
        # print(stderr.read())
ssh.close()

# print(standard_jps_output)


def listBreaker(jps_output):  # this breaks the list up into a list of lists
    res = []
    for ind in jps_output:
        sub = ind.split(b',')
        res.append(sub)
    return(res)

split_jps_list = (listBreaker(standard_jps_output))
# print(split_jps_list)

PIDs = []
for i in split_jps_list:
    PIDs.append(nums_from_string.get_nums(str(i)))

print(PIDs)
# amount_of_pids = []
# for i in range(len(PIDs)):
#     amount_of_pids.append(len(PIDs[i]))
# for num in amount_of_pids:
#     sum_of_all_pids = sum(amount_of_pids)
# print(sum_of_all_pids)

OPEN_VAR = 'a'


# to here I'm good, right?



def multi_jstat_output(pid):
    stdin, stdout, stderr = ssh.exec_command(f'jstat -gcutil {pid} 10000', get_pty=True)
    for line in iter(stdout.readline, ""):
        # print(line, end="")
        with open(f"C:\\Users\yaniv\Documents\get-a-job\projects\emr_jstat\jstat_outputs\{pid}_jstat_output.txt", OPEN_VAR) as o:
            o.write(line)


multiprocessing.set_start_method('spawn', True)

if __name__ == '__main__':
    for num_of_ip in range(len(ec2_instance_ip_list)):
        ssh.connect(
            hostname=f'{ec2_instance_ip_list[num_of_ip]}',
            username=f'{username}',
            pkey=k,
            allow_agent=False,
            look_for_keys=False
        )
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(multi_jstat_output, PIDs[num_of_ip])
            print(1)

