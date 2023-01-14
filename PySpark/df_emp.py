from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession


if __name__=="__main__":

    conf = SparkConf().setAppName('df_emp').setMaster("local[3]")
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.appName('df_emp').master('local[3]').getOrCreate()

    print("\nReading file from the external source it will result into rdd :\n")
    rdd_emp = sc.textFile("file:///home/saif/LFS/datasets/emp.txt")
    print(rdd_emp.collect())

    # using toDF()
    rdd_filter = rdd_emp.filter(lambda x:x!= "id,name,city")
    print("\nRdd after removal of header :\n")
    print(rdd_filter.collect())
    rdd_split = rdd_filter.map(lambda x:x.split(","))
    print("\nRdd after split function applied :\n")
    print(rdd_split.collect())
    df = rdd_split.toDF(['id','name','city'])
    print("\nSchema of the data frame after it was been created from the RDD :\n")
    df.printSchema()
    print("\nData frame after it was created from the RDD :\n")
    df.show(truncate=False)