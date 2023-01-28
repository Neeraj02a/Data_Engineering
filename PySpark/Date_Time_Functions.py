from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,date_format, dayofweek,datediff,date_add,date_sub, dayofyear, dayofmonth,current_date, \
                                  date_trunc, to_date,last_day, next_day, add_months, weekofyear, months_between, quarter, trunc, year, month

if __name__=='__main__':

    conf = SparkConf().setAppName('Date_Time_Functions').setMaster('local[3]')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('Date_Time_Functions').master('local[3]').getOrCreate()

    date_data = [('2021-01-15', '2021-02-15')]
    date_schema = ['start_dt', 'end_dt']

    df= spark.createDataFrame(data=date_data, schema=date_schema)
    df.printSchema()
    df.show(truncate=False)

    print('\n========================================================= How to create one new column using date format function ===========================================\n')
    df1 = df.select('start_dt', 'end_dt', date_format('start_dt', 'dd/MM/yyyy').alias('dt_format'))
    df1.printSchema()
    df1.show(truncate=False)

    print('\n======================================================= Making use of different date function ================================================================\n')

    df2 = df.select('start_dt', 'end_dt', current_date().alias('curr_dt'))
    df2.show(truncate=False)
    df3 = df.select('start_dt', 'end_dt', date_add('start_dt',2).alias('add_two_days'))
    df3.show(truncate=False)
    df4 = df.select('start_dt', 'end_dt', date_sub('start_dt',2).alias('sub_two_days'))
    df4.show(truncate=False)
    df5 = df.select('start_dt', 'end_dt', datediff('start_dt','end_dt').alias('diff_of_two_dates'))
    df5.show(truncate=False)
    df6 = df.select('start_dt', 'end_dt', add_months('start_dt',2).alias('adding_two_months'))
    df6.show(truncate=False)
    df7 = df.select('start_dt','end_dt', add_months('start_dt',2*12).alias('adding_two_years'))
    df7.show(truncate=False)
    df8 = df.select('start_dt','end_dt',year('start_dt').alias('year')
            , month('start_dt').alias('month')
            , dayofmonth('start_dt').alias('day')
            , weekofyear('start_dt').alias('week_of_year')
            , dayofweek('start_dt').alias('day_of_week')
            , dayofyear('start_dt').alias('Days_of_year')
            )
    df8.show(truncate=False)

    df9 = df.select('start_dt', 'end_dt', last_day('start_dt').alias('Last_day_of_year'))
    df9.show(truncate=False)
    df10 = df.select('start_dt', 'end_dt', months_between('end_dt','start_dt').alias('month_bwt'))
    df10.show(truncate=False)
    df11 = df.select('start_dt','end_dt', next_day('start_dt', 'Mon').alias('Next_month'))
    df11.show(truncate=False)
    df12 = df.select('start_dt','end_dt', quarter('start_dt').alias('Quarter_ofYear'))
    df12.show(truncate=False)

    ###Truncate date of month , year
    df13 =df.select('start_dt','end_dt', trunc('start_dt','month').alias('trunc_month')
            , trunc('start_dt','year').alias('trunc_year'))
    df13.show(truncate=False)











