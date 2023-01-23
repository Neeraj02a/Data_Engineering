from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains, when


if __name__=='__main__':

    conf = SparkConf().setAppName('array_function').setMaster('local[3]')
    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName('array_function').master('local[3]').getOrCreate()

    print("============================================================= Filter on an Array Column ========================================================================")
    dept = [('Finance', 10, [1, 2, 3, 4, 5]), ('Marketing', 20, [7, 6, 5, 4, 7]), ('Sales', 30, [17, 26, 65, 84, 97]),
            ('IT', 40, [27, 36, 95, 64, 57])]

    rdd = sc.parallelize(dept)
    df = rdd.toDF(['dept', 'deptno', "locationId's"])
    df.printSchema()
    df.show(truncate=False)

    df.filter(array_contains(df["locationId's"],95)).show(truncate=False)

    print('\n====================================================================== orderby(), sort() ==================================================================\n')
    df.orderBy('dept','deptno').show(truncate=False)
    df.sort('deptno').show(truncate=False)

    print(df.orderBy(['deptno']).count())

    print('\n======================================================================= withColumn() =========================================================================\n')
    df2 = df.withColumn('Salary', df['deptno']*100)
    df2.show(truncate=False)

    df3 = df.withColumn('Salary', df['deptno']*250)

    print('\n======================================================================== Union ===============================================================================\n')
    union_df = df2.union(df3)
    union_df.show(truncate=False)

    print('\n======================================================================== Merge without duplicates ============================================================\n')
    dis_df = df2.union(df3).distinct()
    dis_df.show(truncate=False)

    print('\n======================================================================== dropDuplicate ======================================================================\n')
    drop_df = dis_df.dropDuplicates(['deptno', "locationId's"])
    drop_df.show(truncate=False)

    drop_Sal = dis_df.drop("Salary").show()
    dis_df.drop("dept","Salary").show()

    print('\n================================================================== case - when - other =======================================================================\n')



    rdd = sc.textFile('file:///home/saif/LFS/datasets/sales.txt')
    print(rdd.collect())

    rdd_filter = rdd.filter(lambda x:x!='dept,cadre,costToCompany,state')
    print(rdd_filter.collect())
    rdd_map = rdd_filter.map(lambda x:x.split(','))
    print(rdd_map.collect())
    df = rdd_map.toDF(['dept','cadre','costToCompany','state'])
    df.show(truncate=False)

    df2 = df.withColumn("low_cost",
                        when (df['costToCompany']<30000,'Low')
                        .when (df['costToCompany']>30000, 'High')
                        .otherwise('Unkown'))

    df2.show(truncate=False)


