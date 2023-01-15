from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__=='__main__':

    conf = SparkConf().setAppName('orders').setMaster("local[3]")
    sc =  SparkContext(conf=conf)
    spark = SparkSession.builder.appName('orders').master('local[3]').getOrCreate()

    rdd = sc.textFile("file:///home/saif/LFS/datasets/orders.txt")
    print(rdd.take(5))
    rdd_filter = rdd.filter(lambda x:x!='order_id,order_date,order_customer_id,order_status')
    rdd_map = rdd_filter.map(lambda x:x.split(","))

    df = rdd_map.toDF(['order_id','order_date','order_customer_id','order_status'])
    df.printSchema()
    df.show(truncate=False)

    df_filter = df.filter(df['order_status']=='CLOSED')
    df_filter.show(truncate=False)