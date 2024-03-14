import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SparkFilterWordCount")

sc = SparkContext(conf=conf)


tokenized = sc.textFile("/home/bitnami/Labs/Lab2/Data/AliceInWonderLandPart1.txt").flatMap(lambda line: line.split(" "))
countThreshold=int(sys.argv[1])
wordCounts = tokenized.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2).filter(lambda pair:pair[1] > countThreshold)

list = wordCounts.collect()
print ('[%s]' % ', '.join(map(str, list)))