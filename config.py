# s3 bucket to save graphs
s3_bucket_name = 'emr-graph-test-002'

# the path to the pem key on the local machine
local_key_path = '/home/yaniv/get-a-job/USeast1keypair.pem'
# the path to where the pem key should be saved on master
master_key_path = '/home/ec2-user/USeast1keypair.pem'

#user to be used for ssh
user = 'ec2-user'
# process search term to run jstat
search_term = 'Main'
parent_dir = '/home/ec2-user/emr_jstat_project'


# the path to the bash script which pulls worker node ips
bash_script_path = f'{parent_dir}/get_ips.sh'
# the path to the text file containing worker node ips
ip_text_path = f'{parent_dir}/worker_ips.txt'

# path for csv and jstat output for master and worker nodes
csv_save = '/tmp/jstat_output'

# the directory where graph should be saved
graph_dir = f'{parent_dir}/graph'
# full path for graph
graph_path = f'{graph_dir}/refined_jstat.png'


