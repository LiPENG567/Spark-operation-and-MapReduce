#!/usr/bin/env python
# coding: utf-8

# In[33]:


import pyspark
import json
import sys
# sc.stop()
review_path = sys.argv[1]
busi_path = sys.argv[2]
outfilename = sys.argv[3]
if_spark = sys.argv[4]
n = sys.argv[5]
if if_spark == 'spark':
    sc = pyspark.SparkContext()
    review = sc.textFile(review_path)
    RErdd = review.map(json.loads).map(lambda row: (row['business_id'],row['stars'])).persist()
    STrdd = RErdd.groupByKey().map(lambda a: (a[0], (sum(a[1]), len(a[1]))))

    business = sc.textFile(busi_path)
    BUrdd = business.map(json.loads).map(lambda row: (row['business_id'],row['categories'])).persist()
    Catrdd = BUrdd.filter(lambda a: (a[1] is not None) and (a[1] is not "")).mapValues(lambda a: [ai.strip() for ai in a.split(',')])

    JOrdd = Catrdd.leftOuterJoin(STrdd).map(lambda a: a[1]).filter(lambda a: a[1] is not None)
    FFrdd = JOrdd.flatMap(lambda a: [(b, a[1]) for b in a[0]]).reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).mapValues(lambda a: a[0]/a[1])
    sort_rdd = FFrdd.sortByKey(ascending=True).map(lambda a: (a[1],a[0])).sortByKey(ascending=False).map(lambda a: (a[1],a[0]))
    result = sort_rdd.take(int(n))
    
    data = {}
    data['result']=result

    
else:
    review = []
    for line in open(review_path, 'r'):
        review.append(json.loads(line))
    N = len(review)
    buss_id = []
    for i in range(N):
        A = review[i]
        buss_id.append((A['business_id'],A['stars']))

    # group by key for review 
    buss_star={}
    buss_cnt = {}
    for i in range(N):
        if buss_id[i][0] in buss_star: 
            buss_star[buss_id[i][0]] += buss_id[i][1]
            buss_cnt[buss_id[i][0]] += 1
        else:
            buss_star[buss_id[i][0]] = buss_id[i][1]
            buss_cnt[buss_id[i][0]] = 1


    business = []
    for line in open(busi_path, 'r'):
        business.append(json.loads(line))
    M = len(business)
    business_id = []
    for i in range(M):
        A = business[i]
        if (A['categories'] is not None) and (A['categories'] is not ""):
            business_id.append((A['business_id'],[cate.strip() for cate in A['categories'].split(',')]))
    buss_cate={}
    for i in range(len(business_id)):
        buss_cate[business_id[i][0]]=business_id[i][1]

    join_star_dict = {}
    join_cnt_dict = {}
    for k, v in buss_star.items():
        if k in buss_cate:
            for cat in buss_cate[k]:
                if cat in join_star_dict:
                    join_star_dict[cat] += v
                    join_cnt_dict[cat] += buss_cnt[k]
                else:
                    join_star_dict[cat] = v
                    join_cnt_dict[cat] = buss_cnt[k]

    final_dict = {}
    for k, v in join_star_dict.items():
        final_dict[k] = v/join_cnt_dict[k]
    
    sort_final1=sorted(final_dict.items(), key=lambda a: (-a[1], a[0]))[:int(n)]
    data = {}
    data['result']=sort_final1
    
with open(outfilename,'w') as outfile:
json.dump(data, outfile)








