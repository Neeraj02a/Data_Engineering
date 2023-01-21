from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

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

#======================================== Little more complex example of schema encomposition and creation of Data frame ======================================
    print("\n========================== Little more complex example of schema encomposition and creation of Data frame ========================================\n")
    data = [
        (("Saif", "H", "Shaikh"), "OH", "M"),
        (("Neha", "S", ""), "NY", "F"),
        (("Mitali", "", "Kashiv"), "OH", "F"),
        (("Ram", "S", "Shirali"), "NY", "M"),
        (("Aniket", "M", "Mishra"), "NY", "M"),
        (("Tausif", "M", "Shaikh"), "OH", "M")
    ]

    mySchema = StructType([
                    StructField("name",
                        StructType([
                            StructField('fname', StringType(), True),
                            StructField('mname', StringType(), True),
                            StructField('lname', StringType(), True)
                        ])),
                        StructField("state", StringType(), True),
                        StructField("gender", StringType(), True)

    ])

    df = spark.createDataFrame(data= data, schema=mySchema)
    print("\nSchema of data frame after embedding complex schema with complex data :\n")
    df.printSchema()
    print("\nData frame after embedding complex schema with complex data :\n")
    df.show(truncate=False)

#========================================================= Creating datafram from data read from external source ============================================
    print("\n#========================================================= Creating datafram from data read from external source ============================================\n")

    df_csv = spark.read.format('csv') \
                .option('delimiter','|') \
                .option('header', 'True') \
                .option('inferSchema','True') \
                .load('file:///home/saif/LFS/datasets/emp_all.txt')
    # for HDFS hdfs://localhost:9000/user/saif/HFS/Output/....
    df_csv.printSchema()
    df_csv.show(5,truncate=False)

    #===================================================== save df to a file  =============================================================
    print("\n==============================saving df to a file ====================================================\n")

    df_csv.write.format('csv')\
                .mode('append')\
                .save('file:///home/saif/LFS/datasets/emp_all_write')
    # for HDFS hdfs://localhost:9000/user/saif/HFS/Output/....

