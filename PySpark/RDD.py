from pyspark import SparkConf
from pyspark import SparkContext

if __name__=='__main__':

    conf = SparkConf().setAppName('RDD').setMaster('local[3]')
    sc = SparkContext(conf=conf)

    list = [1,2,4,4,2,1,5,6,7,8,9]
    print(type(list))

    # Parallelize methord to create RDD
    rdd_list = sc.parallelize(list)
    print('Parallelize methord to create RDD : ')
    print(type(rdd_list))

    # External storage system way of creating RDD
    rdd_storage = sc.textFile('file:///home/saif/LFS/datasets/emp.txt')
    print('External storage system way of creating RDD :')
    print(type(rdd_storage))

    # Tranforming one type of partition into other type
    print("Before re-partition :", rdd_list.getNumPartitions())
    print(rdd_list.glom().collect())
    repartition_rdd = rdd_list.repartition(2)
    print("After re-partition :", repartition_rdd.getNumPartitions())
    print(repartition_rdd.glom().collect())
