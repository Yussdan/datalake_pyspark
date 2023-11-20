
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from pyspark.sql.types import StringType






def local_time(message_city):
        return (message_city
            .select(
                F.col('event.message_from').alias('user_id'),
                F.col('city'),
                F.col('event.datetime'),
                F.expr("timezone(city)").alias('timezone')  
            )\
            .withColumn('max_date', F.row_number().over(Window.orderBy(F.desc('datetime')).partitionBy('user_id')))\
            .filter(F.col('max_date')==1)\
            .orderBy('datetime')\
            .withColumn('TIME_UTC', F.date_format('datetime', 'HH:mm:ss'))\
            .withColumn('local_time', F.from_utc_timestamp(F.col('TIME_UTC'), F.col('timezone')))
        ).drop('datetime','max_date','TIME_UTC')
    
def distance(df, geo):
    return df.crossJoin(geo)\
        .withColumn('distance', 2 * 6371 * F.asin(F.sqrt(F.pow(F.sin(((F.col('lat1') - F.col('lat2')) / 2)), 2) + F.cos(F.col('lat1')) * F.cos('lat2') * F.pow(F.sin(((F.col('lon1') - F.col('lon2')) / 2)), 2)))) \
        .withColumn('min_dist', F.rank().over(Window.orderBy('distance').partitionBy('event')))\
        .filter(F.col('min_dist')==1)\
        .drop('min_dist','lat1','lat2','lon1','lon2')


def timezone(city):
        city_utc = {
            'Sydney': 'Australia/Sydney',
            'Melbourne': 'Australia/Melbourne',
            'Brisbane': 'Australia/Brisbane',
            'Perth': 'Australia/Perth',
            'Adelaide': 'Australia/Adelaide',
            'Gold Coast': 'Australia/Brisbane',
            'Cranbourne': 'Australia/Melbourne',
            'Canberra': 'Australia/Canberra',
            'Newcastle': 'Australia/Sydney',
            'Wollongong': 'Australia/Sydney',
            'Geelong': 'Australia/Melbourne',
            'Hobart': 'Australia/Hobart',
            'Townsville': 'Australia/Brisbane',
            'Ipswich': 'Australia/Brisbane',
            'Cairns': 'Australia/Brisbane',
            'Toowoomba': 'Australia/Brisbane',
            'Darwin': 'Australia/Darwin',
            'Ballarat': 'Australia/Melbourne',
            'Bendigo': 'Australia/Melbourne',
            'Launceston': 'Australia/Hobart',
            'Mackay': 'Australia/Brisbane',
            'Rockhampton': 'Australia/Brisbane',
            'Maitland': 'Australia/Sydney',
            'Bunbury': 'Australia/Perth'
        }
        return city_utc.get(city, 'Unknown')


def main():

    date = sys.argv[1]
    events_base_path = sys.argv[2]
    geo_base_path = sys.argv[3]
    fisrt_table_save = sys.argv[4]


    conf = SparkConf().setAppName(f"UsersDatamart-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    date_pivot=F.to_date(F.lit(date), "yyyy-MM-dd")


  
  



    def user_last_city(message_city):
        return message_city\
                    .select(F.col('event.message_from').alias('user_id'),F.col('city').alias('act_city'),F.col('event.message_ts').alias('ts'))\
            .withColumn('last_city', F.row_number().over(Window.orderBy(F.desc('ts')).partitionBy('user_id')))\
            .filter(F.col('last_city')==1)\
            .drop('last_city','ts')

    def user_home(message_city):
        return message_city \
            .select(F.col('event.message_from').alias('user_id'), F.col('city').alias('home_city'), F.col('date')) \
            .orderBy('user_id', 'date') \
            .withColumn('prev_date_message', F.lag('date').over(Window.orderBy('date').partitionBy('user_id', 'home_city'))) \
            .withColumn('date_diff', F.datediff('date', 'prev_date_message')) \
            .filter(F.col('date_diff') == 1) \
            .withColumn('count_day_message', F.row_number().over(Window.orderBy(F.desc('date')).partitionBy('user_id', 'home_city'))) \
            .groupBy('user_id', 'home_city', 'date') \
            .agg(F.max('count_day_message').alias('max_count_date')) \
            .filter(F.col('max_count_date') >= 26) \
            .withColumn('max_date', F.row_number().over(Window.orderBy(F.desc('date')).partitionBy('user_id'))) \
            .filter(F.col('max_date') == 1) \
            .drop('date', 'max_count_date', 'max_date')



    def travel_array(message_city):
        return message_city \
            .select(F.col("event.message_from").alias('user_id'), F.col('city')) \
            .orderBy('user_id') \
            .withColumn('prev_city', F.lag('city').over(Window.partitionBy('user_id').orderBy('city'))) \
            .filter((F.col('prev_city').isNull()) | (F.col('prev_city') != F.col('city'))) \
            .groupBy("user_id") \
            .agg(
                F.collect_list("city").alias("travel_array")
            )

    def travel_count(message_city):
        return travel_array(message_city) \
            .withColumn('travel_count', F.size('travel_array')) \


    timezone_udf = F.udf(timezone, StringType())
    sql.udf.register("timezone", timezone_udf)      


    

    message = sql.read.parquet(events_base_path) \
                .where(F.col('date') <= date_pivot)\
                .withColumn('lat1', F.radians(F.col('lat'))) \
                .withColumn('lon1', F.radians(F.col('lon'))) \
                .drop('lat','lon')


    geo = sql.read.option("delimiter", ";").option("header", True).csv(geo_base_path) \
                .withColumn('lat', F.regexp_replace('lat', ',', '.').cast('double')) \
                .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('double')) \
                .withColumn('lat2', F.radians(F.col('lat'))) \
                .withColumn('lon2', F.radians(F.col('lng'))) \
                .drop('lat', 'lng')





    message_city=distance(message,geo)

    users_datamart=user_last_city(message_city)\
        .join(user_home(message_city),'user_id','left')\
        .join(travel_count(message_city),'user_id')\
        .join(local_time(message_city.filter(F.col('event.datetime').isNotNull())),'user_id')\
        .drop('timezone','city')

    users_datamart.write.mode('append').parquet(f"{fisrt_table_save}/date={date}")

if __name__ == '__main__':
    main()