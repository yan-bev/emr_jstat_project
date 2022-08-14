# -- change: s3 bucket to save graphs
s3_bucket_name = 'emr-graph-test-002'

# -- change: the path to the pem key on the local machine
local_key_path = '/home/yaniv/USeast1keypair.pem'

# -- change: process search term to run jstat
search_term = 'Main'

secret_name = "USeast1keypair.pem"

region_name = "us-east-1"

# user to be used for ssh
user = 'hadoop'

# ----------------------------------------------------

# the path to where the pem key should be saved on master
master_key_path = f'/home/{user}/{secret_name}'

parent_dir = f'/home/{user}/emr_jstat_project'


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


