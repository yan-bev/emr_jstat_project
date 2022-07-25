import paramiko
import boto3
from operator import itemgetter
import pandas as pd
from pathlib import Path
import os

from jstat_capture import cluster_id
from jstat_capture import cluster_label
from jstat_capture import nested_instance_ip
from jstat_capture import jps_command
from filepath import KEYPATH

USERNAME = 'root'
PEM = KEYPATH
CHANGE_PATH = r'C:\\Users\yaniv\Documents\get-a-job\projects\emr_jstat\jstat_outputs'

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

# print(jps_command())
def extract_files():
    """
    the function opens the requested files, reads it, and saves O,FGC, and FGCT
     to a local folder as a csv. the files are saved in the following path
     cluster/instance/jstat_output.csv. following this file is truncated to 0 bytes.
    :return:
    """
    clusters = cluster_id()
    ips = nested_instance_ip()
    last_four_cluster_chars = cluster_label()
    last_four_instance_chars = nested_instance_label()
    PIDs = jps_command()



    k = paramiko.RSAKey.from_private_key_file(f'{PEM}')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for cl in range(len(clusters)):
        for ip in range(len(ips[cl])):
                ssh.connect(
                hostname=f'{ips[cl][ip]}',
                username=f'{USERNAME}',
                pkey=k,
                allow_agent=False,
                look_for_keys=False
            )
                for pid in PIDs[cl][ip]:
                    print(pid)
                    sftp = ssh.open_sftp()
                    readfile = sftp.open(filename=f'/tmp/jstat_output/jstat_{pid}', mode='r', bufsize=32768)
                    readfile.prefetch()
                    for line in readfile:
                        line = line.split()
                        liner = [(itemgetter(3, 8, 9)(line))]

                        jstat = pd.DataFrame(liner)

                        os.chdir(CHANGE_PATH)
                        output_dir = Path(f"cluster_{last_four_cluster_chars[cl]}\\instance_{last_four_instance_chars[cl][ip]}\\")
                        output_dir.mkdir(parents=True, exist_ok=True)
                        final_dest = f"cluster_{last_four_cluster_chars[cl]}\instance_{last_four_instance_chars[cl][ip]}\jstat_{pid}.csv"
                        jstat.to_csv(final_dest, mode='a', index=False, header=False)
                    sftp.truncate(path=f'/tmp/jstat_output/jstat_{pid}', size=0)


# set working directory

# #  TODO: you have to do the necassary computations on each file.
# # test
# df = pd.read_csv("C:\\Users\yaniv\Documents\get-a-job\projects\emr_jstat\jstat_outputs\jstat_Ide7a6_pid10772.csv")
# # print(df.loc[[0], ['O', 'FGC']])  # this prints by the label (the index)
#
# df.drop(df[df['O'] == 'O'].index, inplace=True)
# # this changes the whole file to floats, otherwise, i wouldn't be able to do math
# df_float = df.astype(float)
#
# # def :: from here down, we print out the 'O' values when FGCT has changed.
# # this adds change of FGCT column and sees where there was a difference
# df_float['\u0394FGCT'] = df_float['FGCT'].diff()
#
# # here if the difference is over zero (excluding NAN values) we save the row as df_change
# df_change = df_float[df_float['\u0394FGCT'] > 0]
# print(df_float)
# # this prints out the O values everytime
# print(df_change.O)
