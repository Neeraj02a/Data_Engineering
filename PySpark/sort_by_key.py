from pyspark import SparkConf
from pyspark import SparkContext

if __name__=="__main__":

    conf = SparkConf().setAppName("sort_by_key").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    data = ["Saif", "Ram", "Mitali", "Aniket", "Ram", "Ram", "Aniket"]

    rdd = sc.parallelize(data)
    rdd_map = rdd.map(lambda word:(word,1))
    rdd_count = rdd_map.reduceByKey(lambda a,b:a+b)
    print(rdd_count.collect())

    rdd_sort = rdd_count.sortByKey()
    print(rdd_sort.collect())

