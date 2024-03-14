from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab words count Example")
sc = SparkContext(conf=conf)
tokenized = sc.textFile("/home/bitnami/Labs/Lab2/Data/AliceInWonderLandPart1.txt").flatMap(lambda line: line.split(" "))

wordCounts = tokenized.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)
list = wordCounts.collect()
print ('[%s]' % ', '.join(map(str, list)))
wordCounts.take(10)