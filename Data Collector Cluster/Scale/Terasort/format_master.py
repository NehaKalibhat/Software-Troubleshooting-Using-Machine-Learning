import sys
file = open(sys.argv[1]+"/0.txt")
head = "NN_heap, NN_cpu, NN_threads,RM_heap, RM_cpu, RM_threads"
#print head

data = file.read()
data = data.split("\n")

i = 0
while i < len(data):
	while i < len(data) and not("STOP" in data[i]):
		l = [0]*6
		while i < len(data) and not("PID" in data[i]) and not("STOP" in data[i]): 
			if(".NameNode" in data[i]):
				row = (' '.join(data[i].split())).split(" ")
				l[0], l[1], l[2] = row[2], row[6], row[10]

			elif("ResourceManager" in data[i]):
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
