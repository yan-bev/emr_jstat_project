import paramiko
from operator import itemgetter
from jstat_capture import ec2_instance_ip_list
from jstat_capture import username
from jstat_capture import PIDs
import pandas as pd



k = paramiko.RSAKey.from_private_key_file(r'C:\\Users\yaniv\Documents\get-a-job\USeast1keypair.pem')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())



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
        REMOTE_FILE_PATH = f'/tmp/jstat_output/jstat_{pid}'  # noqa
        DEST_FILES_PATH = f"C:\\Users\yaniv\Documents\get-a-job\projects\emr_jstat\jstat_outputs\jstat_{pid}"  # noqa
        remote_file = sftp.get(remotepath=REMOTE_FILE_PATH, localpath=DEST_FILES_PATH, prefetch=True)
#         try:
#             for line in remote_file:
#                 liner = " ".join(line.split()).split()
#                 all_ec2_o_fgc_fgct.append(itemgetter(3, 8, 9)(liner))
#         finally:
#             remote_file.close()
# FILE_PATH = "C:\\Users\yaniv\Documents\get-a-job\projects\emr_jstat\jstat_outputs"
# df = pd.DataFrame(all_ec2_o_fgc_fgct)
# df.to_csv(FILE_PATH, index=False, header=False)




