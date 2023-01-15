from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession


if __name__=="__main__":

    conf = SparkConf().setAppName('transformation').setMaster('local[3]')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('transformation').master('local[3]').getOrCreate()

    #=============================================== Narrow transformation =====================================================================================
    print("\n========================================== Narrow transformation (map(), mapPartition(), flatMap(), filter(), union()) ==============================================================\n")
    print("\n================================================================== Map transformation ==========================================================\n")
    data =[1,2,3,4,5,6]
    rdd_data = sc.parallelize(data)
    rdd_map = rdd_data.map(lambda x:(x,x+2))
    print(rdd_map.collect())

    for element in rdd_map.collect():
        print(element, end="")

    print("\n============================================================= flat map transformation ==========================================================\n")
    data = [3,5,6,8,9,1,0,3,2]
    rdd_data = sc.parallelize(data)
    rdd_flatmap = rdd_data.flatMap(lambda x:(x,x**2))
    print(rdd_flatmap.collect())

    rdd_map = rdd_data.map(lambda x:(x,x**2))
    print(rdd_map.collect())

    print("\n============================================================= Filter transformation ==============================================================\n")
    data =[4,5,6,7,1,9,0,10,9,8,3,2,5,6,8]
    rdd_data = sc.parallelize(data)
    result = rdd_data.filter(lambda val:(val%2==0))
    print(result.collect())

    print("\n=============================================================== union & intersection transformation ============================================================\n")
    a = [7,8,9,4,6,3,2,1,0]
    b = [6,3,8,7,9,5,2,4,1,3,6]

    rdd_a = sc.parallelize(a)
    rdd_b = sc.parallelize(b)

    union_output = rdd_a.union(rdd_b)
    print("After performing union operation : ", end="")
    print(union_output.collect())

    intersection_output = rdd_a.intersection(rdd_b)
    print("\nAfter performing Intersection operation : ", end="")
    print(intersection_output.collect() )




    #======================================================== Wide transformation =============================================================================
    print("\n============================================ Wide transformation (groupByKey(), aggregateByKey(), aggregate(), join(), repartition())============================================================\n")
    print("\n================================================================ Group by key ===================================================================\n")

    data = [('A',2),('B',1),('C',3),('D',1),('E',6),('F',10)]
    rdd_data = sc.parallelize(data)
    result = rdd_data.groupByKey()
    print(result.collect())

    for var in result.collect():
        for digi in var[1]:
            print(digi)

    print("\n============================================================== Reduce by key ====================================================================\n")
    data = [('Project', 1),
            ('Gutenberg’s', 1),
            ('Alice’s', 1),
            ('Adventures', 1),
            ('in', 1),
            ('Wonderland', 1),
            ('Project', 1),
            ('Gutenberg’s', 1),
            ('Adventures', 1),
            ('in', 1),
            ('Wonderland', 1),
            ('Project', 1),
            ('Gutenberg’s', 1)]

    rdd_data = sc.parallelize(data)
    result = rdd_data.reduceByKey(lambda x,b:x+b)
    print(result.collect())

    for var in result.collect():
        print(var)

