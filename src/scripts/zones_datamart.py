import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

from users_datamart import distance




def main():

    date = sys.argv[1]
    events_base_path = sys.argv[2]
    geo_base_path = sys.argv[3]
    second_table_save = sys.argv[4]

    conf = SparkConf().setAppName(f"ZonesDatamart-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    date_pivot=F.to_date(F.lit(date), "yyyy-MM-dd")
    def count_event_zone(df,event,slice):
        return df\
            .select(F.col('id').alias('zone_id'),F.col('event_type'))\
            .filter(F.col('event_type')==event)\
            .groupBy('zone_id')\
            .agg(F.count('*').alias(f'{slice}_{event}'))

    def count_registration(df, message, slice, date):
        if slice == 'month':
            messages_without_pivot = message.select('event.message_from').where(
                (F.month("date") < F.month(date_pivot)) & (F.year("date") <= F.year(date_pivot)))
        elif slice == 'week':
            messages_without_pivot = message.select('event.message_from').where(
                (F.weekofyear("date") < F.weekofyear(date_pivot)) & (F.year("date") <= F.year(date_pivot)))

        return df.select(F.col('event.message_from'),F.col('id').alias('zone_id'))\
                .join(messages_without_pivot, 'message_from', 'left_anti') \
                .distinct() \
                .groupBy('zone_id')\
                .agg(F.count('*').alias(f'{slice}_user'))


    date_pivot=F.to_date(F.lit(date), "yyyy-MM-dd")

    message = sql.read.parquet(events_base_path) \
                .where(F.col('date') <= date_pivot)\
                .withColumn('lat1', F.radians(F.col('lat'))) \
                .withColumn('lon1', F.radians(F.col('lon'))) \
                .drop('lat','lon')

    event_week = sql.read.parquet(events_base_path) \
        .where((F.col('date') <= F.lit(date_pivot)) &
            (F.weekofyear(F.col('date')) == F.weekofyear(date_pivot)) &
            (F.year(F.col('date')) == F.year(date_pivot))) \
        .withColumn('lat1', F.radians(F.col('lat'))) \
        .withColumn('lon1', F.radians(F.col('lon'))) \
        .drop('lat','lon')

    event_month = sql.read.parquet(events_base_path) \
                .where((F.col('date') <= F.lit(date_pivot)) &
                    (F.month(F.col('date')) == F.month(date_pivot)) &
                    (F.year(F.col('date')) == F.year(date_pivot)))\
                .withColumn('lat1', F.radians(F.col('lat'))) \
                .withColumn('lon1', F.radians(F.col('lon'))) \
                .drop('lat','lon')



    geo = sql.read.option("delimiter", ";").option("header", True).csv(geo_base_path) \
            .withColumn('lat2', F.radians(F.col('lat'))) \
            .withColumn('lon2', F.radians(F.col('lng'))) \
            .drop('lat', 'lng')



    event_week_city = distance(event_week, geo)
    event_month_city = distance(event_month, geo)

    count_event_week = count_event_zone(event_week_city, 'message', 'week') \
        .join(count_event_zone(event_week_city, 'reaction', 'week'), 'zone_id','full') \
        .join(count_event_zone(event_week_city, 'subscription', 'week'), 'zone_id','full') \
        .join(count_registration(event_week_city, message, 'week', date_pivot),'zone_id','full')


    count_event_month = count_event_zone(event_month_city, 'message', 'month').drop('user_id','event_type') \
        .join(count_event_zone(event_month_city, 'reaction', 'month'), 'zone_id','full') \
        .join(count_event_zone(event_month_city, 'subscription', 'month'), 'zone_id','full') \
        .join(count_registration(event_month_city, message, 'month', date_pivot),'zone_id','full')


    zones_datamart = geo.select(
        F.weekofyear(date_pivot).alias('week'),
        F.month(date_pivot).alias('month'),
        F.col('id').alias('zone_id'))\
        .join(count_event_week, 'zone_id', 'full')\
        .join(count_event_month, 'zone_id', 'full')
    
    zones_datamart.write.parquet(f"{second_table_save}/date={date}")

    
if __name__ == '__main__':
    main()

