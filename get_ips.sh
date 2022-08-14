#!/bin/bash
user=$(awk -F "=" '/User/ {print $2}' config.ini)
FILE=/home/$user/emr_jstat_project/worker_ips.txt
yarn node -list 2> /dev/null | grep internal | cut -d' ' -f1 | cut -d: -f1 >> '$FILE'