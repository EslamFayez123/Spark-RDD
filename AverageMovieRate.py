import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SparkFilterWordCount")

sc = SparkContext(conf=conf)

tokenized = sc.textFile("/home/bitnami/Labs/Lab2/u.data")

MappedData = tokenized.map(lambda x: x.split('\t')).map(lambda y: (int(y[0]), float(y[2]), int(y[1])))

userRating = MappedData.map(lambda x:(x[0],(x[1],x[2])))

userSumCount = userRating.aggregateByKey((0,0.0), (lambda x, y: (x[0]+y[0],x[1]+1)),(lambda rdd1, rdd2: (rdd1[0]+rdd2[0], rdd1[1]+rdd2[1])))

userAvgRating = userSumCount.mapValues(lambda x:(x[0]/x[1]))
print(userAvgRating.collect())

