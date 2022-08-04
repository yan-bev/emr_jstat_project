local_pem_path = r'C:\\Users\yaniv\Documents\get-a-job\USeast1keypair.pem'
remote_pem_path = '/home/ec2-user/USeast1keypair.pem'
user = 'ec2-user'
search_term = 'Main'
bash_script_path = '/home/ec2-user/git/get_ips.sh' # TODO: change name to emr-project
ip_text_path = '/home/ec2-user/worker_ips.txt'
master_directory_path_for_df_save = '/tmp/jstat_output'


graph_parent_save_location = '/home/ec2-user/graph'
full_graph_save_path = '/home/ec2-user/graph/refined_jstat.png'



s3_bucket_name = 'emr-graph-test-002'