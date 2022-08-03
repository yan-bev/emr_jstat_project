#!/bin/bash
FILE=/home/ec2-user/worker_ips.txt
yarn node -list 2> /dev/null | grep internal | cut -d' ' -f1 | cut -d: -f1 >> $FILE