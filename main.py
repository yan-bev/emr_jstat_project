from jstat_capture import jstat_starter
import time
import concurrent.futures

from extractor import extract_files
from jstat_capture import node_ips

if __name__ == "__main__":
    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futureSSH = {executor.submit(jstat_starter()): ip for ip in node_ips()}
            for future in concurrent.futures.as_completed(futureSSH):
                print((futureSSH[future]))
        time.sleep(3600)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futureSSH = {executor.submit(extract_files()): ip for ip in node_ips()}
            for future in concurrent.futures.as_completed(futureSSH):
                print((futureSSH[future]))

