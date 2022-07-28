from jstat_capture import jstat_starter
import time
from extractor import extract_files

if __name__ == "__main__":
    while True:
        jstat_starter()
        time.sleep(3600)
        extract_files()

