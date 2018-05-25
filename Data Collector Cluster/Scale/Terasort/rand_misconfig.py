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

def write_stop(file,diff,misconfig_code,misconfig_node,client,write_dir):
	file_lines = []
	ftp = client.open_sftp()
	remote_file = ftp.open("/home/ubuntu/Neha_Shreya/"+file)
	for line in remote_file:
		file_lines.append(line)
	file_lines.append("TIME:"+str(diff)+",CODE:"+str(misconfig_code)+"\n")
	file_lines.append("STOP\n")
	write_dir += str(misconfig_node)
	with open(write_dir+"/"+file, "a") as dump:
		for i in file_lines:
			dump.write(i)
	ftp.close()
	
def create_misconfig(file , misconfig_str, client , misconfig_code):
	file_lines = []
	ftp = client.open_sftp()
	remote_file = ftp.open(file)
	for line in remote_file:
		file_lines.append(line)
	if misconfig_code == 3 or misconfig_code == 4:
		file_lines.append(misconfig_str)
	else:
		file_lines[-1] = misconfig_str
	remote_file = ftp.file(file, "w", -1)
	for i in file_lines:
		remote_file.write(i)
	remote_file.flush()
	remote_file.close()
	ftp.close()

def remove_misconfig(file, client, misconfig_code):
	file_lines = []
	ftp = client.open_sftp()
	remote_file = ftp.open(file)
	for line in remote_file:
		file_lines.append(line)
	if misconfig_code == 3 or misconfig_code == 4:
		file_lines[-1] = ""
	else:
 		file_lines[-1] = "</configuration>"
	remote_file = ftp.file(file, "w", -1)
	for i in file_lines:
		remote_file.write(i)
	remote_file.flush()
	remote_file.close()
	ftp.close()
	
def run_script(misconfig_code,misconfig_node, command, write_dir):
	start_time = time.time()	
	sleeptime = 0.001
	transports = []
	channels = []
	for i in range(len(node_ips)):
		transports.append(clients[i].get_transport())
		channels.append(transports[i].open_session())
		channels[i].exec_command('cd /home/ubuntu/Neha_Shreya; source ./myenv; ./jvmtop.sh > '+str(misconfig_node)+".txt")
	
	channel_exec = transports[misconfig_node].open_session()
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
		if i == misconfig_node:
			write_stop(str(i)+".txt",diff, misconfig_code, misconfig_node, clients[i], write_dir)
		else:
			write_stop(str(i)+".txt",diff, 0, misconfig_node, clients[i], write_dir)

misconfig_list = [
	[
		"/usr/local/hadoop/etc/hadoop/mapred-site.xml",
		"<property><name>mapred.child.java.opts</name><value>-Xmx100M</value></property><property><name>mapred.child.ulimit</name><value>1</value></property><property><name>io.sort.mb</name><value>2000</value></property></configuration>"
	],
	[
		"/usr/local/hadoop/etc/hadoop/mapred-site.xml",
		"<property><name>mapreduce.task.io.sort.mb</name><value>0</value></property></configuration>"
	],
	[
		"/usr/local/hadoop/etc/hadoop/hadoop-env.sh",
		'export HADOOP_HEAPSIZE="0"'
	],
	[
		"/usr/local/hadoop/etc/hadoop/hadoop-env.sh",
		"export HADOOP_CLIENT_OPTS='-Xmx1m $HADOOP_CLIENT_OPTS'"
	],
	[
		"/usr/local/hadoop/etc/hadoop/mapred-site.xml",
		"<property><name>mapreduce.reduce.shuffle.memory.limit.percent</name><value>0.0</value></property></configuration>"
	],
	[
		"/usr/local/hadoop/etc/hadoop/mapred-site.xml",
		"<property><name>io.file.buffer.size</name><value>0</value></property></configuration>"
	],
]

def collect_data(loop, misconfig_node, command, write_dir):
	client = clients[misconfig_node]
	while loop:
		print "LOOP ",loop
		misconfig_code = 1
		for i in misconfig_list:
			create_misconfig(i[0],i[1],client,misconfig_code)
			run_script(misconfig_code,misconfig_node,command, write_dir)
			remove_misconfig(i[0],client,misconfig_code)
			print misconfig_code," DONE"		
			misconfig_code += 1
		#misconfig
		client.exec_command('chmod 000 /usr/local/hadoop/etc/hadoop/mapred-site.xml')
		run_script(misconfig_code,misconfig_node,command, write_dir)
		client.exec_command('chmod 777 /usr/local/hadoop/etc/hadoop/mapred-site.xml')
		misconfig_code += 1
		client.exec_command('chmod 000 /usr/local/hadoop/etc/hadoop/core-site.xml')
		run_script(misconfig_code, misconfig_node, command,write_dir)
		client.exec_command('chmod 777 /usr/local/hadoop/etc/hadoop/core-site.xml')
		loop -= 1

loop = 1
write_dir = "/home/neha/Data Collector Cluster/Scale/Terasort/"

misconfig_node = 1
if not os.path.exists(write_dir+str(misconfig_node)):
    os.makedirs(write_dir+str(misconfig_node))

collect_data(loop, misconfig_node, "cd /usr/local/hadoop/share/hadoop/mapreduce; source ./myenv; ./terasort",write_dir)
