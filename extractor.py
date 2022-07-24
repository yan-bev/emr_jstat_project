import paramiko
from operator import itemgetter
from jstat_capture import ec2_instance_ip_list
from jstat_capture import username
from jstat_capture import PIDs
from jstat_capture import ec2_instance_id_last_4_chars

import pandas as pd
import os


k = paramiko.RSAKey.from_private_key_file(r'C:\\Users\yaniv\Documents\get-a-job\USeast1keypair.pem')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

READVAR = 'r'
APPENDVAR = 'a'
list_of_files = []


for num_of_ip in range(len(ec2_instance_ip_list)):
    ssh.connect(
        hostname=f'{ec2_instance_ip_list[num_of_ip]}',
        username=f'{username}',
        pkey=k,
        allow_agent=False,
        look_for_keys=False
    )
    for pid in PIDs[num_of_ip]:
        sftp = ssh.open_sftp()
        REMOTE_FILE_PATH = f'/tmp/jstat_output/jstat_{pid}'
        DEST_FILES_PATH = f"C:\\Users\yaniv\Documents\get-a-job\projects\emr_jstat\jstat_outputs\jstat_Id{ec2_instance_id_last_4_chars[num_of_ip]}_pid{pid}"
        FINAL_DEST_FILES_PATH = f"C:\\Users\yaniv\Documents\get-a-job\projects\emr_jstat\jstat_outputs\jstat_Id{ec2_instance_id_last_4_chars[num_of_ip]}_pid{pid}.csv"
        #  I'm not sure if i should use sftp, or read the file line-by-line in order
        # and then simply add that as a DataFrame
        sftp.get(remotepath=REMOTE_FILE_PATH, localpath=DEST_FILES_PATH, prefetch=True)

        with open(DEST_FILES_PATH, READVAR, newline='') as readfile:
            for line in readfile:  # TODO: have this go straight to being a csv with all the info. no need to preselect for O, FGC, and FGCT; though it seems like this is actually easier than dealing with lists
                line = line.split()
                liner = [(itemgetter(3, 8, 9)(line))]

                jstat = pd.DataFrame(liner)

                jstat.to_csv(FINAL_DEST_FILES_PATH, mode=APPENDVAR, index=False, header=False)
                list_of_files.append(FINAL_DEST_FILES_PATH)
        os.remove(DEST_FILES_PATH)
# set working directory

#  TODO: you have to do the necassary computations on each file.
# test
df = pd.read_csv("C:\\Users\yaniv\Documents\get-a-job\projects\emr_jstat\jstat_outputs\jstat_Ide7a6_pid10772.csv")
# print(df.loc[[0], ['O', 'FGC']])  # this prints by the label (the index)

df.drop(df[df['O'] == 'O'].index, inplace=True)
# this changes the whole file to floats, otherwise, i wouldn't be able to do math
df_float = df.astype(float)

# def :: from here down, we print out the 'O' values when FGCT has changed.
# this adds change of FGCT column and sees where there was a difference
df_float['\u0394FGCT'] = df_float['FGCT'].diff()

# here if the difference is over zero (excluding NAN values) we save the row as df_change
df_change = df_float[df_float['\u0394FGCT'] > 0]
print(df_float)
# this prints out the O values everytime
print(df_change.O)
