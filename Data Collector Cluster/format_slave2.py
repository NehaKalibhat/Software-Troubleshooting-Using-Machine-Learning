file = open("Grep/0_0_0/Slave2.txt")
head = "DN_heap, DN_cpu, DN_threads,NM_heap, NM_cpu, NM_threads"
#print head

data = file.read()
data = data.split("\n")

i = 0
while i < len(data):
	while i < len(data) and not("STOP" in data[i]):
		l = [0]*6
		while i < len(data) and not("PID" in data[i]) and not("STOP" in data[i]): 
			if("DataNode" in data[i]):
				row = (' '.join(data[i].split())).split(" ")
				l[0], l[1], l[2] = row[2], row[6], row[10]
			
			elif("NodeManager" in data[i]):
				row = (' '.join(data[i].split())).split(" ")
				l[3], l[4], l[5] = row[2], row[6], row[10]
				
			i += 1
		if l != [0]*6:
			l = [str(j) for j in l]
			print ",".join(l)
		
		if i < len(data) and "PID" in data[i]:
			i += 1
		elif i < len(data) and "STOP" in data[i]:
			break
	
	if i < len(data):
		print data[i-1]
		print data[i]
	
	i += 1
