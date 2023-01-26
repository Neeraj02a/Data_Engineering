from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

if __name__=='__main__':

    conf = SparkConf().setAppName('Cache_&_Persist').setMaster('local[3]')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('Cache_&_Persist').master('laster[3]').getOrCreate()

    df = spark.read.format('csv') \
                    .option('delimiter', ',') \
                    .option('header', True) \
                    .option('inferSchema', True) \
                    .load('file:///home/saif/LFS/datasets/txns.csv')

    df.show(5,truncate=False)
    print("Is cathced :",df.is_cached)
    cacheDF= df.where(df['state']=='California').cache()
    print("After Catche is performend : ",cacheDF.is_cached)
    print("Count After Catche is performend :", cacheDF.count())
    readingCacheDf = cacheDF.where(cacheDF['spendby']=='credit')
    print("Count After Catche is performend and filteration is performed in spendby column :", readingCacheDf.count())


    persistDF = df.persist(StorageLevel.DISK_ONLY)
    print('\nAfter performing persist we are counting :',persistDF.count())




