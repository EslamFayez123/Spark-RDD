from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("LongestLine")

sc = SparkContext(conf=conf)

textFile = sc.textFile("/home/bitnami/Labs/Lab2/Data/AliceInWonderLandPart1.txt")

def max(a, b):
    if a > b:
        return a
    else:
        return b
    
textFile.map(lambda line: len(line.split())).reduce(max)


