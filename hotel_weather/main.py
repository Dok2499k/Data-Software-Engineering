from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config import RESTAURANT_FILE_PATH, WEATHER_FILE_PATH, OUTPUT_PATH
from data_processing import fill_missing_lat_lng, add_geohash_column, save_enriched_data


def initialize_spark():
    return SparkSession.builder \
        .appName("RestaurantWeatherETL") \
        .master("local[*]") \
        .getOrCreate()


def main():
    # Initialize Spark session
    spark = initialize_spark()

    restaurants_df = spark.read.csv(RESTAURANT_FILE_PATH, header=True, inferSchema=True)
    weather_df = spark.read.parquet(WEATHER_FILE_PATH)

    restaurants_df = fill_missing_lat_lng(restaurants_df)

    restaurants_df = add_geohash_column(restaurants_df)
    weather_df = add_geohash_column(weather_df)
    weather_df = weather_df.withColumnRenamed("lat", "weather_lat").withColumnRenamed("lng", "weather_lng")

    enriched_df = restaurants_df.join(weather_df, on="geohash", how="left")

    save_enriched_data(enriched_df, OUTPUT_PATH)

    print("ETL pipeline completed successfully!")

    spark.stop()


if __name__ == "__main__":
    main()
