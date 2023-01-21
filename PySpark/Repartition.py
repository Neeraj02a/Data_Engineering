from pyspark import SparkConf
from pyspark import SparkContext

if __name__=="__main__":

    conf = SparkConf().setAppName("Repartition").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    data =[1,2,3,4,5,6,7,8,9,10]
    rdd = sc.parallelize(data)
    print("Number of partitions :",rdd.getNumPartitions())
    rdd_coalesce = rdd.coalesce(2)
    print("After coalesce operation is performed :", rdd_coalesce.getNumPartitions())
    rdd_repartition = rdd.repartition(5)
    print("After repartition operation is performed :", rdd_repartition.getNumPartitions())
