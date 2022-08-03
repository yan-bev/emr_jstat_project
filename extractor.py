import datetime
import paramiko
from operator import itemgetter
import pandas as pd
from pathlib import Path
import os
from datetime import date, datetime, timedelta

from config import remote_pem_path
from config import user

from jstat_capture import node_ips
from jstat_capture import cluster_id
from jstat_capture import cluster_label
from jstat_capture import nested_instance_ip
from jstat_capture import jps_command



PEM = remote_pem_path
USERNAME = user
pids = jps_command()


ips = node_ips()


CHANGE_PATH = r'C:\\Users\yaniv\Documents\get-a-job\projects\emr_jstat\jstat_outputs'
date = date.today()
last_hour = datetime.now() - timedelta(hours=1)

def extract_files(ips=ips, username=USERNAME, key_file=PEM, PIDs=pids ):
    """
    the function opens the requested files, reads it, and saves O,FGC, and FGCT
     to a local folder as a csv. the files are saved in the following path
     cluster/instance/jstat_output.csv. following write, the remote file is truncated to 0 bytes.
    :return:
    """

    last_four_instance_chars = nested_instance_label()


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
        for pid in PIDs[i]:
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

                os.chdir(CHANGE_PATH)
                output_dir = Path(f"cluster_{last_four_cluster_chars[cl]}\\instance_{last_four_instance_chars[cl][ip]}\\")
                output_dir.mkdir(parents=True, exist_ok=True)
                final_dest = f"cluster_{last_four_cluster_chars[cl]}\instance_{last_four_instance_chars[cl][ip]}\jstat_{pid}.csv"
                jstat.to_csv(final_dest, mode='a', index=False, header=False)
            sftp.truncate(path=f'/tmp/jstat_output/jstat_{pid}', size=0)



# if '__name__' == '__main__':
extract_files()

