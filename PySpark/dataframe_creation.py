from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__=="__main__":

    conf = SparkConf().setAppName("data_frame").setMaster("local[3]")
    sc =  SparkContext(conf=conf)
    spark = SparkSession.builder.appName("data_frame").master("local[3]").getOrCreate()

    dept = [("Finance",10),
            ("Marketing",20),
            ("Sales",30),
            ("IT",40)]

    rdd = sc.parallelize(dept)

    print("\nCreating dataframe using toDf() methord :")
    df = rdd.toDF(["dept", "deptno"])

    print("\nPrint Schema of dataframe that you have just cretaed :")
    df.printSchema()
    print("\nPrint dataframe that you have just created :")
    df.show(truncate=False)

#=============================================================================================================================================================
    print("\n============================================= Schema Encorporation way of creatoing df =========================================================\n")
    dataSchema = ["dept","deptno"]
    df = spark.createDataFrame(data = dept, schema=dataSchema)
    df.printSchema()
    df.show(truncate=False)




