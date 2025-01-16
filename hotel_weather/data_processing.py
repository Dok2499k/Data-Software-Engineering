from pyspark.sql.functions import col, split
from pyspark.sql.types import DoubleType
from utils import fetch_coordinates_udf, generate_geohash


def fill_missing_lat_lng(restaurants_df):
    missing_lat_lng_df = restaurants_df.filter(col("lat").isNull() | col("lng").isNull())

    updated_missing_df = missing_lat_lng_df.withColumn(
        "filled_coordinates",
        fetch_coordinates_udf(col("country"), col("city"))
    )

    updated_missing_df = updated_missing_df.withColumn(
        "lat",
        split(col("filled_coordinates"), ",").getItem(0).cast(DoubleType())
    ).withColumn(
        "lng",
        split(col("filled_coordinates"), ",").getItem(1).cast(DoubleType())
    ).drop("filled_coordinates")

    existing_lat_lng_df = restaurants_df.filter(col("lat").isNotNull() & col("lng").isNotNull())

    combined_df = existing_lat_lng_df.unionByName(updated_missing_df)

    return combined_df


def add_geohash_column(df):
    return df.withColumn("geohash", generate_geohash(col("lat"), col("lng")))


def save_enriched_data(df, output_path):
    df.write.partitionBy("year", "month", "day").parquet(output_path, mode="overwrite")
