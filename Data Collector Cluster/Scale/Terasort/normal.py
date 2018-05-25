import random
import sys
import os
import select
import paramiko	
from paramiko import SSHClient
import subprocess
import time

node_ips = ["10.10.1.89","10.10.1.90","10.10.1.51"]
clients = []
for i in range(len(node_ips)):
	clients.append(SSHClient())
	clients[i].set_missing_host_key_policy(paramiko.AutoAddPolicy())
	clients[i].load_system_host_keys()
	clients[i].connect(node_ips[i], username="ubuntu", password="ubuntu")

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
	transports = []
	channels = []
	for i in range(len(node_ips)):
		transports.append(clients[i].get_transport())
		channels.append(transports[i].open_session())
		channels[i].exec_command('cd /home/ubuntu/Neha_Shreya; source ./myenv; ./jvmtop.sh > '+str(i)+'.txt')
	
	channel_exec = transports[1].open_session()
	channel_exec.exec_command(command)
	
	while True:  # monitoring process
	# Reading from output streams
		while channel_exec.recv_ready():
			print channel_exec.recv(1000)
		while channel_exec.recv_stderr_ready():
			print channel_exec.recv_stderr(1000)
		for i in range(len(node_ips)):
			if channels[i].exit_status_ready():  # If completed
				break
		if channel_exec.exit_status_ready():  # If completed
			break
		time.sleep(sleeptime)
	
	end_time = time.time()
	diff = end_time - start_time
	
	for i in range(len(node_ips)):
		clients[i].exec_command('pkill -f JvmTop')
		
	for i in range(len(node_ips)):
		write_stop(str(i)+".txt",diff, 0, clients[i],write_dir)
	
loop = 1
misconfig_code = 0
write_dir = "/home/neha/Data Collector Cluster/Scale/Terasort/Normal/"
if not os.path.exists(write_dir):
    os.makedirs(write_dir)
while loop:
	print "LOOP ",loop

	run_script(misconfig_code, "cd /usr/local/hadoop/share/hadoop/mapreduce; source ./myenv; ./terasort",write_dir)
	loop -= 1

