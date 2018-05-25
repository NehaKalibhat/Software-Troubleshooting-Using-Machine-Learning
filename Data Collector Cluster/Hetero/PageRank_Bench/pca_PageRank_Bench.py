import numpy
from sklearn.decomposition import PCA
from sklearn import linear_model
import csv
# fix random seed for reproducibility
numpy.random.seed(7)

inputs_train = []
inputs_test = []
outputs_train = []
outputs_test = []

num_samples = [0]*9;
pca = PCA(n_components=1)
lr = linear_model.LogisticRegression()

with open("master_all_data.csv") as f:
	file_lines = []
	for i in f:
		file_lines.append(i)
	i = 0
	while i < len(file_lines):
		sample = []
		time = 0
		while i < len(file_lines) and not("TIME" in file_lines[i]):
			l = file_lines[i].split(",")
			for k in range(len(l)):
				l[k] = l[k].replace("\n","")
				l[k] = l[k].replace("m","")
				l[k] = l[k].replace("%","")
				l[k] = float(l[k])	
				sample.append(l[k])
				
			i += 1
		if i < len(file_lines) and "TIME" in file_lines[i]:
			time = (file_lines[i].split(",")[0]).split(":")[1]
			i += 2
			while len(sample) < 45*6:
				sample.append(0)
			if len(sample) > 45*6:
				sample = sample[:45*6]
			#sample.append(time)
		
			code = int(file_lines[i-2].split(",")[-1].split(":")[-1])
			
			sample = numpy.array(sample)
			pca.fit(sample)
			sample = pca.transform(sample)
			
			if num_samples[code] < 160:
				inputs_train.append(sample.tolist())
				outputs_train.append(code)				
			else:
				inputs_test.append(sample.tolist())
				outputs_test.append(code)
			num_samples[code] += 1

inputs = inputs_train + inputs_test
outputs = outputs_train + outputs_test

print inputs
exit()

train_size = int(0.8*len(inputs))
#train_size = 200
inputs_train = numpy.array(inputs[:train_size])
inputs_test = numpy.array(inputs[train_size:])
outputs_train = numpy.array(outputs[:train_size])
outputs_test = numpy.array(outputs[train_size:])		

lr.fit(numpy.array(inputs_train), numpy.array(outputs_train))

correct_group = 0
correct = 0
correct_pred = 0

print "Expected,Predicted,Expected Group,Predicted Group"
i = 0

pred_list = model.predict(inputs_test)
while i < len(inputs_test):
	sample = inputs_test[i]
	sample = numpy.array(sample)
	sample = pca.transform(sample)
	count = numpy.ndarray.tolist(outputs_test[i]).index(max(outputs_test[i]))
	exp_group = [0,"Normal"]
	pred_group = [0,"Normal"]
	
	if count == 0:
		exp_group = [0,"Normal"]
	if count == 1 or count == 2:
		# task/container issues
		exp_group = [1,"Container/Task Out Of Memory"]
	elif count == 5 or count == 6:
		# value misconfig
		exp_group = [2,"Absent/Malformed Value"]
	elif count == 7 or count == 8:
		# permission
		exp_group = [3,"Permission Denied"]
	elif count == 3 or count == 4:
		# client jvm out of memory
		exp_group = [4,"JVM Could Not Start"]
	
	pred = lr.predict(sample.tolist())[0]
	
	if pred == 0:
		pred_group = [0,"Normal"]
	if pred == 1 or pred == 2:
		# task/container issues
		pred_group = [1,"Container/Task Out Of Memory"]
	elif pred == 5 or pred == 6:
		# value misconfig
		pred_group = [2,"Absent/Malformed Value"]
	elif pred == 7 or pred == 8:
		# permission
		pred_group = [3,"Permission Denied"]
	elif pred == 3 or pred == 4:
		# client jvm out of memory
		pred_group = [4,"JVM Could Not Start"]
	
	if count == pred == 0:
		correct_pred += 1
	elif count != 0 and pred != 0:
		correct_pred += 1
		
	if count == pred:
		correct += 1
		correct_group += 1
	elif exp_group == pred_group:
		correct_group += 1
	print  count,",",pred,",",exp_group[1],",",pred_group[1]
	
	i += 1
				
accuracy = (float(correct) / len(inputs_test)) * 100
accuracy_group = (float(correct_group) / len(inputs_test)) * 100
accuracy_pred = (float(correct_pred) / len(inputs_test)) * 100
print "Accuracy,",accuracy,",Accuracy Group,",accuracy_group,"Accuracy Pred,",accuracy_pred

