# ASSUMPTIONS:
# 1. Master.txt : input file 1
# 2. Slave.txt : input file 2
# 3. out.txt : outupt file


phrase = "alpha"
# Extract Timestamp from Master File
# Timestamp lies between 29-37 chars
f = open("Master.txt")
linesM = f.readlines()
masterTime = [] 
for line in linesM:
    if phrase in line:
        masterTime.append(line[29:37])
#masterTime = set(masterTime)
f.close()

# Extract Timestamp from Slave1 File
# Timestamp lies between 29-37 chars
f = open("Slave1.txt")
linesS1 = f.readlines()
slaveTime1 = []
for line in linesS1:
    if phrase in line:
        slaveTime1.append(line[29:37])
f.close()

# Extract Timestamp from Slave2 File
# Timestamp lies between 29-37 chars
f = open("Slave2.txt")
linesS2 = f.readlines()
slaveTime2 = []
for line in linesS2:
    if phrase in line:
        slaveTime2.append(line[29:37])
#slaveTime2 = set(slaveTime2)
f.close()

linesS1 = [w.replace("tanode.DataNode", "tanode.DataNode1") for w in linesS1]
linesS1 = [w.replace("ger.NodeManager", "ger.NodeManager1") for w in linesS1]
linesS2 = [w.replace("tanode.DataNode", "tanode.DataNode2") for w in linesS2]
linesS2 = [w.replace("ger.NodeManager", "ger.NodeManager2") for w in linesS2]

# Find the common timestamps and sort them
time = list(set(masterTime) & set(slaveTime1) & set(slaveTime2))
sorted_time = sorted([":".join(list(map(str, d.split(":")))) for d in time])

# Write out the data for the common timestamps from Master and Slave files
fO = open("out.txt", "w+")
for time in sorted_time:
	timeFound = False
	for line in linesM:
		if time in line:
			num = linesM.index(line)
			timeFound = True
			continue
		if "Linux" in line and timeFound:
			num2 = linesM.index(line)
			s1 = linesM[num : num2]
			timeFound = False
			break

	for line in linesS1:
		if time in line:
			num = linesS1.index(line)
			timeFound = True
			continue
		if "Linux" in line and timeFound:
			num2 = linesS1.index(line)
			s2 = linesS1[num+4 : num2]
			timeFound = False
			break

	for line in linesS2:
		if time in line:
			num = linesS2.index(line)
			timeFound = True
			continue
		if "Linux" in line and timeFound:
			num2 = linesS2.index(line)
			s3 = linesS2[num+4 : num2]
			timeFound = False
			break
		'''		
		if "TIME" in line and timeFound:
			num = linesS2.index(line)
			s3.append(linesS2[num])
			s3.append(linesS2[num+1])	
		'''
	s = "".join(s1+s2+s3)
	fO.write(s)
	fO.write('\n')
fO.close()


