import pyspark
import json
import sys
# define a base RDDs from an external file
sc = pyspark.SparkContext()
path = sys.argv[1]
outfilename = sys.argv[2]
partition_type = sys.argv[3]
n_partition = sys.argv[4]
n = sys.argv[5]

review = sc.textFile(path)
Myrdd = review.map(json.loads).map(lambda row: (row['business_id'],1)).persist()

def partitioner(business_id):
    return hash(business_id[0])

if partition_type != "default":
    Myrdd = Myrdd.partitionBy(int(n_partition),partitioner).persist()

num_partition = Myrdd.getNumPartitions()
n_item = Myrdd.glom().map(len).collect()
result = Myrdd.reduceByKey(lambda a,b: a+b).filter(lambda a: a[1]>int(n)).collect()

# save the result in the json file format

data = {}
data['n_partitions']=num_partition
data['n_items']=n_item
data['result']=result
with open(outfilename,'w') as outfile:
    json.dump(data, outfile)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




