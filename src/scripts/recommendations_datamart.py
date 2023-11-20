
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from pyspark.sql.types import StringType

from users_datamart import distance, local_time, timezone


def main():

    date = sys.argv[1]
    events_base_path = sys.argv[2]
    geo_base_path = sys.argv[3]
    recs_dtmrt_base_path = sys.argv[4]

    conf = SparkConf().setAppName(f"RecommendationsDatamart-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    timezone_udf = F.udf(timezone, StringType())
    sql.udf.register("timezone", timezone_udf) 
    date_pivot=F.to_date(F.lit(date), "yyyy-MM-dd")




    def calculate_user_distance(message_city_df):
        window = Window().partitionBy(["message_from", "date"]).orderBy(F.desc("datetime"))
        user_date_lat_lon = (
            message_city_df.select(
                F.col("event.message_from"), F.col("date"), F.col("event.datetime"), F.col("lat1"),F.col('lon1'))
            .withColumn("rn", F.row_number().over(window))
            .where("rn=1")
            .select(F.col("message_from").alias('user_id'), F.col("date"), F.col("lat1"),F.col('lon1'))
        )

        users_joined = user_date_lat_lon.join(
            user_date_lat_lon.select(
                F.col("user_id").alias("user_right"),
                "date",
                F.col("lat1").alias("lat2"),
                F.col("lon1").alias("lon2"),
            ),
            "date",
        )

        users_joined_distance = users_joined.withColumn('distance', 2 * 6371 * F.asin(F.sqrt(F.pow(F.sin(((F.col('lat1') - F.col('lat2')) / 2)), 2) + F.cos(F.col('lat1')) * F.cos('lat2') * F.pow(F.sin(((F.col('lon1') - F.col('lon2')) / 2)), 2))))\
            .where("distance<=1 and user_id<user_right")

        return users_joined_distance



    date_pivot=F.to_date(F.lit(date), "yyyy-MM-dd")


    events_df = sql.read.parquet(events_base_path).filter(
            F.col("date")<date_pivot)\
                .withColumn('lat1', F.radians(F.col('lat'))) \
                .withColumn('lon1', F.radians(F.col('lon'))) \
                .drop('lat','lon')


    geo = sql.read.option("delimiter", ";").option("header", True).csv(geo_base_path) \
            .withColumn('lat', F.regexp_replace('lat', ',', '.').cast('double')) \
            .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('double')) \
            .withColumn('lat2', F.radians(F.col('lat'))) \
            .withColumn('lon2', F.radians(F.col('lng'))) \
            .drop('lat', 'lng')



    subs = (
            events_df.where("event.subscription_channel is not null")
            .select(F.col("event.user").alias("user_id"), "event.subscription_channel")
            .distinct()
    )
        
    subs_pairs = subs.join(
            subs.select(F.col("user_id").alias("user_right"), "subscription_channel"),
            "subscription_channel",
    ).filter(F.col('user_id')!=F.col('user_right'))

    messages = (
            events_df.where(
                "event.message_from is not null and event.message_to is not null"
            )
            .select(
                F.col("event.message_from").alias("user_id"),
                F.col("event.message_to").alias("user_right"),
            )
            .distinct()
    )

    messages_pairs = messages.union(
            messages.select(
                F.col("user_id").alias("user_right"), F.col("user_right").alias("user_id")
            )
    )


    messag_city = distance(events_df.filter(F.col('event.datetime').isNotNull()), geo)

    local_time_df = local_time(messag_city)  

    users_distance_df = calculate_user_distance(events_df)

    recommendations_datamart = (
        users_distance_df
        .join(subs_pairs, "user_id", how="leftsemi")
        .join(messages_pairs, "user_id", how="leftanti")
        .join(local_time_df, on="user_id", how="left")
        .join(geo,'city')
        .select(
            F.col("user_id").alias("user_left"),
            F.col("user_right"),
            F.col("date").alias("processed_dttm"),
            F.col('id')
        )
    )

    recommendations_datamart.write.parquet(f"{recs_dtmrt_base_path}/date={date}")

if __name__ == '__main__':
    main()