import datetime
import paramiko
from operator import itemgetter
import pandas as pd
from pathlib import Path
import os
from datetime import datetime, timedelta

from config import csv_save
from config import master_key_path
from config import user

from jstat_capture import node_ips
from jstat_capture import jps_command


pids = jps_command()

def ec2_worker_label(ips=node_ips()):
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


def extract_files(ips=node_ips(), username=user, key_file=master_key_path, pids=pids, ec2_label=ec2_worker_label()):
    """
    the function opens the requested files, reads it, and saves O,FGC, and FGCT
     to a local folder as a csv. the files are saved in the following path
     cluster/instance/jstat_output.csv. following write, the remote file is truncated to 0 bytes.
    :return:
    """

    k = paramiko.RSAKey.from_private_key_file(f'{key_file}')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


    for i, ip in enumerate(ips):

        ssh.connect(
            hostname=ip,
            username=username,
            pkey=key_file,
            allow_agent=False,
            look_for_keys=False
            )
        for pid in pids[i]:
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

                output_dir = Path(f"{csv_save}/instance_{ec2_label[i]}")
                output_dir.mkdir(parents=True, exist_ok=True)
                os.chdir(csv_save)
                final_dest = f'jstat_{pid}.csv'
                jstat.to_csv(final_dest, mode='a', index=False, header=False)
            sftp.truncate(path=f'{csv_save}/jstat_{pid}', size=0)


print(extract_files())

