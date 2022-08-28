import datetime
import concurrent.futures
import paramiko
from operator import itemgetter
import pandas as pd
from pathlib import Path
import os
from datetime import datetime, timedelta
import configparser
import io



from jstat_capture import node_ips
# from jstat_capture import jps_command
from jstat_capture import get_secret


config = configparser.ConfigParser()
config.read('config.ini')


user = config['cnfg']['User']
csv_save = config['cnfg']['CsvSave']
sleeptime = config['cnfg']['Hours']


def ec2_worker_label(ips=node_ips()):
    '''
    creates a label to be used in order to label the Ec2s
    :param ips: the ips to shorten
    :return: a shorted ec2 Ip label
    '''
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


def extract_files(ip):
    """
    the function opens the requested files, reads it, and saves O,FGC, and FGCT
     to a local folder as a csv. the files are saved in the following path
     tmp/jstat_output/instance/jstat_output.csv. following write, the remote file is truncated to 0 bytes.
    :return:
    """
    key = get_secret()
    private_key_str = io.StringIO()
    private_key_str.write(key)
    private_key_str.seek(0)

    key = paramiko.RSAKey.from_private_key(private_key_str)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    files = []
    ssh.connect(
        hostname=ip,
        username=user,
        pkey=key,
        allow_agent=False,
        look_for_keys=False
        )

    stdin, stdout, stderr = ssh.exec_command(f'ls /tmp/jstat_output')
    files.append(stdout.read())
    sftp = ssh.open_sftp()
    # TODO: transfer full directory/open each file following a globsearch/a find files.
    # formatted_files = []
    for formatted_files in files:
        formatted_files = formatted_files.decode("utf-8").replace('\n', ',').strip().split()
    print(formatted_files)
    for filename in formatted_files:
        print(filename)
        readfile = sftp.open(filename=f'/tmp/jstat_output/{filename}', mode='r', bufsize=32768)
        readfile.prefetch()
        for line in readfile:
            line = line.split()
            liner = [(itemgetter(3, 8, 9)(line))]

            jstat = pd.DataFrame(liner)

            d1 = datetime.now() - timedelta(hours=int(sleeptime))
            dtime = pd.to_datetime(d1)
            jstat.insert(0, "DateTime", dtime, allow_duplicates=True)

            output_dir = Path(f"{csv_save}/instance_{ip[10:-13]}") #TODO: try this out
            output_dir.mkdir(parents=True, exist_ok=True)
            os.chdir(csv_save)
            final_dest = f'{output_dir}/{filename}.csv'
            jstat.to_csv(final_dest, mode='a', index=False, header=False)
        sftp.truncate(path=f'{csv_save}/{filename}', size=0)


if __name__ == '__main__':
    ip_list = node_ips()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futureSSH = {executor.submit(extract_files, ip): ip for ip in ip_list}
        for future in concurrent.futures.as_completed(futureSSH):
            print(f'jstat_output from:{futureSSH[future]} transferred to the Master Node ')

