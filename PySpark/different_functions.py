from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains


if __name__=='__main__':

    conf = SparkConf().setAppName('diff_functions').setMaster('local[3]')
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.appName('diff_functions').master('local[3]').getOrCreate()

    rdd = sc.textFile('file:///home/saif/LFS/datasets/emp_all.txt')

    print(rdd.collect())
    rdd_filter = rdd.filter(lambda line:line!='id,name,sal,country')
    rdd_flattened = rdd_filter.map(lambda word:word.split(','))
    print(rdd_flattened.collect())

    df = rdd_flattened.toDF(['id','name','sal','country'])
    df.printSchema()
    df.show(truncate=False)
    df.select(df['sal'],df['name']).show()
    print('\n========================================================== Filters =====================================================================================\n')
    df_filter = df.filter(df['sal']==1000)
    df_filter.show(truncate=False)

    # OR
    df_filter_col = df.filter(col('sal')==2000)
    df_filter_col.show(truncate=False)

    # OR
    df_where = df.where(df.sal==3000)
    df_where.show(truncate=False)

    print('\n============================================================= multiple condition ============================================================================\n')
    # AND
    df_and = df.filter((df['name'] == 'Saif') & (df['sal'] == 2000))
    df_and.show(truncate=False)

    # OR
    df_or = df.filter((df['name'] == 'Ram') | (df['sal'] == 1000))
    df_or.show(truncate=False)

    # NOT
    df_not = df.filter(~(df['name'] == 'Ram'))
    df_not.show(truncate=False)










