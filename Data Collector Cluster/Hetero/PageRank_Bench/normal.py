import random
import sys
import os
import select
import paramiko	
from paramiko import SSHClient
import subprocess
import time

client_master = SSHClient()
client_master.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client_master.load_system_host_keys()
client_master.connect("10.10.1.89", username="ubuntu", password="ubuntu")
client_slave1 = SSHClient()
client_slave1.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client_slave1.load_system_host_keys()
client_slave1.connect("10.10.1.90", username="ubuntu", password="ubuntu")
client_slave2 = SSHClient()
client_slave2.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client_slave2.load_system_host_keys()
client_slave2.connect("10.10.1.51", username="ubuntu", password="ubuntu")

def write_stop(file,diff,misconfig_code,client,write_dir):
	file_lines = []
	ftp = client.open_sftp()
	remote_file = ftp.open("/home/ubuntu/Neha_Shreya/"+file)
	for line in remote_file:
		file_lines.append(line)
	file_lines.append("TIME:"+str(diff)+",CODE:"+str(misconfig_code)+"\n")
	file_lines.append("STOP\n")
	with open(write_dir+file, "a") as dump:
		for i in file_lines:
			dump.write(i)
	ftp.close()
	
def run_script(misconfig_code, command,write_dir):
	start_time = time.time()	
	sleeptime = 0.001
	transport_master = client_master.get_transport()
	transport_slave1 = client_slave1.get_transport()
	transport_slave2 = client_slave2.get_transport()
	channel = transport_master.open_session()
	channel.exec_command('cd /home/ubuntu/Neha_Shreya; source ./myenv; ./jvmtop.sh > Master.txt')
	channel1 = transport_slave1.open_session()
	channel1.exec_command('cd /home/ubuntu/Neha_Shreya; source ./myenv; ./jvmtop.sh > Slave1.txt')
	channel2 = transport_slave2.open_session()
	channel2.exec_command('cd /home/ubuntu/Neha_Shreya; source ./myenv; ./jvmtop.sh > Slave2.txt')
	channel3 = transport_master.open_session()
	channel3.exec_command(command)
	
	while True:  # monitoring process
	# Reading from output streams
		while channel3.recv_ready():
			print channel3.recv(1000)
		while channel3.recv_stderr_ready():
			print channel3.recv_stderr(1000)
		if channel.exit_status_ready():  # If completed
			break
		if channel1.exit_status_ready():  # If completed
			break
		if channel2.exit_status_ready():  # If completed
			break
		if channel3.exit_status_ready():  # If completed
			break
		time.sleep(sleeptime)
	
	end_time = time.time()
	diff = end_time - start_time
	
	client_master.exec_command('pkill -f JvmTop')
	client_slave1.exec_command('pkill -f JvmTop')
	client_slave2.exec_command('pkill -f JvmTop')
	'''
	os.system("scp ubuntu@10.10.1.183:/home/ubuntu/Neha_Shreya/Master.txt /home/neha/Data\ Collector\ Cluster")
	os.system("scp ubuntu@10.10.1.184:/home/ubuntu/Neha_Shreya/Slave1.txt /home/neha/Data\ Collector\ Cluster")
	os.system("scp ubuntu@10.10.1.185:/home/ubuntu/Neha_Shreya/Slave2.txt /home/neha/Data\ Collector\ Cluster")
	os.system("python code.py")
	with open("out.txt", "a") as dump:
		dump.write("TIME:"+str(diff)+",CODE:"+str(misconfig_code)+",NODE:"+str(misconfig_node)+"\n")
		dump.write("STOP\n")
	os.system("python format.py >> data_1.csv")
	'''
	write_stop("Master.txt",diff, 0, client_master,write_dir)
	write_stop("Slave1.txt",diff, 0, client_slave1,write_dir)
	write_stop("Slave2.txt",diff, 0, client_slave2,write_dir)
	
loop = 200
misconfig_code = 0
write_dir = "/home/neha/Data Collector Cluster/Hetro/PageRank_Bench/0_0_0/"
if not os.path.exists(write_dir):
    os.makedirs(write_dir)
while loop:
	print "LOOP ",loop
	run_script(misconfig_code, "cd Neha_Shreya/BigDataBench_V3.2.1_Hadoop_Hive/SearchEngine/PageRank; source ./myenv; ./run_PageRank.sh 10",write_dir)
	loop -= 1

