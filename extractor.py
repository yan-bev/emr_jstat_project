import datetime
import paramiko
from operator import itemgetter
import pandas as pd
from pathlib import Path
import os
from datetime import datetime, timedelta

from config import master_directory_path_for_df_save
from config import remote_pem_path
from config import user

from jstat_capture import node_ips
from jstat_capture import jps_command

USERNAME = user
PEM = remote_pem_path

pids = jps_command()

ips = node_ips()
CHANGE_PATH = master_directory_path_for_df_save


def ec2_worker_label(ips=ips):
    s = []
    for ip in ips:
        throwaway = []
        for ch in ip:
            if ch.isdigit():
                throwaway.append(ch)
        s.append(throwaway)
    ec2_label = []
    for i, _ in enumerate(s):
        ec2_label.append(''.join(s[i][-5:-1]))
    return ec2_label


def extract_files(ips=ips, username=USERNAME, key_file=PEM, pids=pids, ec2_label=ec2_worker_label()):
    """
    the function opens the requested files, reads it, and saves O,FGC, and FGCT
     to a local folder as a csv. the files are saved in the following path
     cluster/instance/jstat_output.csv. following write, the remote file is truncated to 0 bytes.
    :return:
    """

    # last_four_instance_chars = nested_instance_label()


    k = paramiko.RSAKey.from_private_key_file(f'{key_file}')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


    for i, ip in enumerate(ips):
        ssh.connect(
            hostname=ip,
            username=username,
            pkey=k,
            allow_agent=False,
            look_for_keys=False
            )
        for pid in pids[i]:
            print(ip, pid)
            sftp = ssh.open_sftp()
            readfile = sftp.open(filename=f'/tmp/jstat_output/jstat_{pid}', mode='r', bufsize=32768)
            readfile.prefetch()
            for line in readfile:
                line = line.split()
                liner = [(itemgetter(3, 8, 9)(line))]

                jstat = pd.DataFrame(liner)

                d1 = datetime.now() - timedelta(hours=1)
                dtime = pd.to_datetime(d1)
                jstat.insert(0, "DateTime", dtime, allow_duplicates=True)

                if not os.path.exists(CHANGE_PATH):
                    os.makedirs(CHANGE_PATH)
                    print('path changed to:')
                output_dir = Path(f"instance_{ec2_label[i]}")
                output_dir.mkdir(parents=True, exist_ok=True)
                final_dest = f"instance_{ec2_label[i]}/jstat_{pid}.csv"
                jstat.to_csv(final_dest, mode='a', index=False, header=False)
            sftp.truncate(path=f'/tmp/jstat_output/jstat_{pid}', size=0)



if '__name__' == '__main__':
    extract_files()

