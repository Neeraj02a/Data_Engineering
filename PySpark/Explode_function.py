from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, concat, lit, col

if __name__ == '__main__':

    conf = SparkConf().setAppName('Explode').setMaster('local[3]')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('Explode').master('local[3]').getOrCreate()

    explodeData = [('Saif', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
                   ('Mitali', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
                   ('Ram', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
                   ('Wilma', None, None)]
    #  ,('Jatin', ['1', '2'], {})

    df = spark.createDataFrame(data= explodeData, schema= ['name', 'knownLanguages', 'properties'])
    df.show(truncate=False)
    df_extra = df.withColumn('extra', concat(df['name'],lit('NULL')))
    df_extra.show(truncate=False)

    df2= df.select(col("name"),col("properties"), explode('knownLanguages').alias('language'))
    df2.show(truncate=False)

    df3 = df.select(col('*'), explode('knownLanguages').alias('language'))
    df3.show(truncate=False)

    df4 = df.select(df['name'], explode('properties'))
    df4.show(truncate=False)