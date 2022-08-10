# Filtered Jstat
Filtered Jstat graphs old storage percentage, full garbage collection occurances, and full garbage collection time for all applicable processes in all instances in the desired EMR Cluster. 

## Description
From the Master node, *Main.py* runs jstat_starter, sleeps for one hour and extracts the jstat output from each instance in an endless loop. In order to graph O%, FGCT, and FGC, *grapher.py* should be run. The graph will be uploaded to s3 to specified bucket. 

If desired, all jstat processes can be killed by running *jstat_killer.py*. 
___
### Getting Started
In order to use Filtered Jstat, there must be at least one running EMR Cluster with both master and worker node(s). the security group of the master node must have ssh/port 22 available.
___
### Executing Program
1. (required): open ssh on the security group of the master node from the local machine.
2. run aws configure on the local machine, ensure that the region name is the same as the EMR Cluster Region:  
    `aws configure`
3. change `local_key_path` in *config.py* to the appropriate key_file_path
2.  run *sftp_transfer.py* to transfer the private key from the local machine to the Master node:  
    `python3 sftp_transfer.py`
3. ssh into the master node of the cluster:  
    `ssh -i <path_to_key> ec2-user@<MasterNode_PublicIP>`   
4. install git:  
    `sudo yum install git`
6. clone emr_project to Master Node:   
    `git clone https://github.com/yan-bev/emr_jstat_project`
7. install python modules:  
    `pip install paramiko pandas matplotlib boto3`
8. (optional): create a s3 bucket to hold the graphs (required): replace `s3_bucket_name` in *config.py* with the desired s3 bucket. 
9. run main.py:  
   `python3 main.py &`
10. run *grapher.py* in the background:  
    `python3 grapher.py`
11. (optional): run *jps_killer.py* to stop all jps processes and jps output files on the worker node(s) as well as remove csv files from the master. this will also kill the *main.py* process.  

___
### Graph Output With Test Data
![expected output](/graph/refined_jstat.png)
