from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,lit, collect_list


if __name__ =='__main__':

    conf = SparkConf().setAppName('Interview_questions').setMaster('local[3]')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('Interview_questions').master('local[3]').getOrCreate()

    tableA = [('Name','Age'),('Azarudeen,Shahul',25),('Michel, Clarke',26),('Virat, Kohli',28)]
    tableB = [('Name','Age','Gender'),('Rabindra, Tagore',32,'Male'),('Madona, Laure',59,'Female'),('Flintoff, David',12,'Male'),('Ammie, James',20,'Female')]

    rddA = sc.parallelize(tableA)
    rdd_filter = rddA.filter(lambda line:line!=('Name','Age'))
    dfA = rdd_filter.toDF(['Name','Age'])
    dfA.printSchema()
    dfA.show(truncate=False)

    rddB = sc.parallelize(tableB)
    rdd_filter =rddB.filter(lambda line:line!=('Name','Age','Gender'))
    dfB = rdd_filter.toDF(['Name','Age','Gender'])
    dfB.printSchema()
    dfB.show(truncate=False)

    dfA2 = dfA.withColumn('Gender',lit('NULL'))
    dfA2.show(truncate=False)

    print('\n===================================================================== Union operation =======================================================================\n')
    df_union = dfA2.union(dfB)
    df_union.show(truncate=False)

    print('\n====================================================================== explode ==============================================================================\n')


    data =[('id','value_list'),(1,['A','B','C']),(2,['D','E']),(3,['X','Y','Z']),(4,['A','A','X','W','A','W','B'])]

    rdd = sc.parallelize(data)
    rdd_filter = rdd.filter(lambda x:x!=('id','value_list'))
    print(rdd_filter.collect())
    df = rdd_filter.toDF(['id', 'value_list'])
    df.show(truncate=False)

    out_df = df.select(df['id'], explode('value_list').alias('value_list'))
    out_df.show(truncate=False)

    print('\n======================================================================== reverse of explode (Aggrigation) ==================================================================\n')
    data =[('id', 'empname', 'sal'),(1, 'a', 10000),(2,'b',10000),(3,'c',20000),(4,'d',30000),(5,'e',20000)]

    rdd = sc.parallelize(data)
    rdd_filter = rdd.filter(lambda x:x!=('id', 'empname', 'sal'))
    df = rdd_filter.toDF(['id', 'empname', 'sal'])
    df.show(truncate=False)

    grouped_transactions = df.groupBy('sal').agg(collect_list('empname').alias('items'))
    grouped_transactions.show(truncate=False)

    data = [('transaction_id','item'),(1,'a'),(1,'b'),(1,'c'),(1,'d'),(2,'a'),(2,'d'),(3,'c'),(4,'b'),(4,'c'),(4,'d')]
    rdd = sc.parallelize(data)
    rdd_filter = rdd.filter(lambda x:x!=('transaction_id','item'))
    df = rdd_filter.toDF(['transaction_id','item'])
    df.show(truncate=False)
    grouped_transactions = df.groupBy('transaction_id').agg(collect_list('item').alias('grouped_items'))
    grouped_transactions.show(truncate=False)








