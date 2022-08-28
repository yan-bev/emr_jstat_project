import time
import concurrent.futures
import configparser


from jstat_capture import node_ips
from jstat_capture import jps_command_and_starter
from extractor import extract_files

config = configparser.ConfigParser()

sleeptime = config['cnfg']['Hours']


if __name__ == "__main__":
    while True:
        ip_list = node_ips()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futureSSH = {executor.submit(jps_command_and_starter, ip): ip for ip in ip_list}
            for future in concurrent.futures.as_completed(futureSSH):
                print(f'jstat begun on:{futureSSH[future]}')
        time.sleep(int(sleeptime)*3600)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futureSSH = {executor.submit(extract_files, ip): ip for ip in ip_list}
            for future in concurrent.futures.as_completed(futureSSH):
                print(f'jstat_output from:{futureSSH[future]} transferred to the Master Node ')

