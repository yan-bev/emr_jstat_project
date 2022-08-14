#!/bin/bash
source <(grep=config.ini)
User=$User
FILE=/home/!User/emr_jstat_project/worker_ips.txt
yarn node -list 2> /dev/null | grep internal | cut -d' ' -f1 | cut -d: -f1 >> $FILE