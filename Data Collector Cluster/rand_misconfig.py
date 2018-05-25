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
client_master.connect("10.10.1.183", username="ubuntu", password="ubuntu")
client_slave1 = SSHClient()
client_slave1.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client_slave1.load_system_host_keys()
client_slave1.connect("10.10.1.184", username="ubuntu", password="ubuntu")
client_slave2 = SSHClient()
client_slave2.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client_slave2.load_system_host_keys()
client_slave2.connect("10.10.1.185", username="ubuntu", password="ubuntu")

def write_stop(file,diff,misconfig_code,client):
	file_lines = []
	ftp = client.open_sftp()
	remote_file = ftp.open("/home/ubuntu/Neha_Shreya/"+file)
	for line in remote_file:
		file_lines.append(line)
	file_lines.append("TIME:"+str(diff)+",CODE:"+str(misconfig_code)+"\n")
	file_lines.append("STOP\n")
	with open("/home/neha/Data Collector Cluster/TeraSort/1_0_0/"+file, "a") as dump:
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
	
def run_script(misconfig_code,misconfig_node):
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
	channel3.exec_command('cd /usr/local/hadoop/share/hadoop/mapreduce; source ./myenv; ./terasort')
	
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
	write_stop("Master.txt",diff, misconfig_code, client_master)
	write_stop("Slave1.txt",diff, 0, client_slave1)
	write_stop("Slave2.txt",diff, 0, client_slave2)
	
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
		"/usr/local/hadoop/etc/hadoop/hdfs-site.xml",
		"<property><name>dfs.replication</name><value>0</value></property></configuration>"
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

misconfig_node = 1
loop = 100
while loop:
	print "LOOP ",loop
	misconfig_code = 1
	for i in misconfig_list:
		create_misconfig(i[0],i[1],client_master,misconfig_code)
		run_script(misconfig_code,misconfig_node)
		remove_misconfig(i[0],client_master,misconfig_code)
		print misconfig_code," DONE"		
		misconfig_code += 1
	#misconfig
	client_master.exec_command('chmod 000 /usr/local/hadoop/etc/hadoop/mapred-site.xml')
	run_script(misconfig_code,misconfig_node)
	client_master.exec_command('chmod 777 /usr/local/hadoop/etc/hadoop/mapred-site.xml')
	misconfig_code += 1
	client_master.exec_command('chmod 000 /usr/local/hadoop/etc/hadoop/core-site.xml')
	run_script(misconfig_code,misconfig_node)
	client_master.exec_command('chmod 777 /usr/local/hadoop/etc/hadoop/core-site.xml')
	loop -= 1
