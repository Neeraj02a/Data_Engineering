from pyspark import SparkConf
from pyspark import SparkContext

if __name__=="__main__":

    conf = SparkConf().setAppName('joins').setMaster('local[3]')
    sc = SparkContext(conf = conf)

