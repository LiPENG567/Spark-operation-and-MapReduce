#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pyspark
import json
import sys
# define a base RDDs from an external file
# sc.stop()
sc = pyspark.SparkContext()
path = sys.argv[1]
path_stop = sys.argv[3]
y = sys.argv[4]
m = sys.argv[5]
n = sys.argv[6]
# review = sc.textFile('/Users/zailipeng/Desktop/my_research/Important_information/books/CS/Inf553/HW1/review.json')
review = sc.textFile(path)
Myrdd = review.map(json.loads).map(lambda row: (row['date'],row['user_id'],row['text'])).persist()

A = Myrdd.count()
# print(A)

# y = 2015
date_list= Myrdd.map(lambda row:(row[0])).filter(lambda x: x[:4]==str(y))
B = date_list.count()
# print(B)


user_id= Myrdd.map(lambda row:(row[1]))
user_id_distinct= user_id.distinct()
C = user_id_distinct.count()

# print(C)

# D part
# m = 3
ctr = user_id.map(lambda row: (row,1)).reduceByKey(lambda x,y: x+y)
Dr = ctr.sortByKey(ascending=True).map(lambda a:(a[1],a[0])).sortByKey(ascending=False).map(lambda a: (a[1],a[0]))
# print(D.take(m))
D = Dr.take(int(m))
# print(D)

# E part
wordRdd = Myrdd.map(lambda row:(row[2]))
# n = 5
input = open(path_stop,'r')
stoplist = []
for line in input:
    lines = line.strip('\n')
    stoplist.append(lines)
punclist = ['(','[',',','.','!','?',':',';',']',')','']
for a in punclist:
    stoplist.append(a)
word_R = wordRdd.flatMap(lambda row: row.lower().split()).filter(lambda x: x not in stoplist)
# word_R = word_R.map(lambda row: row.replace('(','').replace('[','').replace(',','').replace('!','').replace('?','').replace(':','').replace(';','').replace(']','').replace(')','')).filter(lambda x: x not in stoplist)
ctr_word = word_R.map(lambda row: (row,1)).reduceByKey(lambda x,y: x+y)
Er = ctr_word.sortByKey(ascending=True).map(lambda a:(a[1],a[0])).sortByKey(ascending=False).map(lambda a: (a[1],a[0]))
Err = Er.map(lambda x: x[0])
# print(E.take(5))
E = Err.take(int(n))

# save the result in the json file format
filename = sys.argv[2]
data = {}
data['A']=A
data['B']=B
data['C']=C
data['D']=D
data['E']=E
with open(filename,'w') as outfile:
    json.dump(data, outfile)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




