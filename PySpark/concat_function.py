from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, concat, lit, concat_ws

if __name__ =='__main__':

    conf = SparkConf().setAppName('concat').setMaster('local[3]')
    sc = SparkContext(conf = conf)
    spark = SparkSession.builder.appName('concat').master('local[3]').getOrCreate()

    df = spark.read.format('csv')\
                    .option('delimiter',',')\
                    .option('header',True)\
                    .option('inferSchema',True)\
                    .load('file:///home/saif/LFS/datasets/usdata.csv')
    df.printSchema()
    df.show(5,truncate=False)

    print('\n================================================================ Using & and | operator =====================================================================\n')
    df2 = df.withColumn('slang',
                        when((df['city'] == 'Brighton') | (df['county'] == 'Butler'), 'my_county')
                        .when((df['city']=='Bridgeport') & (df['county']=='Gloucester'),'my_wife_county')
                        .otherwise('foregin')
                        )
    df2.show(5, truncate=False)

    print('\n==================================================================== concat() ============================================================================\n')
    df3 = df.withColumn('Full_Name',
                        concat(df['first_name'],lit('_'), df['last_name'])
                        )
    df3.show(5, truncate=False)
    print('\n====================================================================== concat_ws() =========================================================================\n')

    df4 = df.withColumn('Full_Name',
                        concat_ws(' ', df['first_name'], df['last_name'])
                        )
    df4.show(5,truncate=False)