from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, expr

if __name__ =='__main__':

    conf =  SparkConf().setAppName('case').setMaster('local[3]')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('case').master('local[3]').getOrCreate()

    rdd = sc.textFile('file:///home/saif/LFS/datasets/sales.txt')
    rdd_filter = rdd.filter(lambda line:line!='dept,cadre,costToCompany,state')
    rdd_map = rdd_filter.map(lambda x:x.split(','))
    df = rdd_map.toDF(['dept','cadre','costToCompany','state'])
    df.show(truncate=False)

    print('\n========================================================= case when then ===================================================================================\n')
    df2 = df.withColumn('low_cost',
                        when (df['costToCompany'] <30000, 'LOW')
                        .when(df['costToCompany'] >= 30000, 'High')
                        .otherwise('Unknown'))

    df2.show(5,truncate=False)

    print('\n==================================================================== expr() ================================================================================\n')

    df3 = df.withColumn('low_cost',
                        expr("""
                        case 
                            when costToCompany < 30000 then 'Low'
                            when costToCompany >= 30000 then 'High'
                            else 'Unknown'
                        end
                        """))

    df3.show(5,truncate=False)

    
