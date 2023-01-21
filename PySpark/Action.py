from pyspark import SparkConf
from pyspark import SparkContext

if __name__=='__main__':

    conf = SparkConf().setAppName('Actions').setMaster('local[3]')
    sc = SparkContext(conf = conf)

    print("\n=========== Actions are the operation that are applied over the rdd as per the requirement but output of action not result intoa new rdd  =============\n")
    data =[1,2,3,4,5,6,7,8,9,10]
    rdd = sc.parallelize(data)
    print(rdd.take(5))
    print(rdd.collect())
