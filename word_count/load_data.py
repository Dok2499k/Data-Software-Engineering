from pyspark.sql import SparkSession

def load_dataset(spark, file_path, column_name):
    reviews_df = spark.read.csv(file_path, header=True, inferSchema=True)
    if column_name not in reviews_df.columns:
        raise ValueError(f"Column '{column_name}' not found in the dataset.")
    return reviews_df.select(column_name)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Amazon Reviews Data Loader") \
        .master("local[*]") \
        .getOrCreate()

    file_path = "amazon_product_reviews.csv"
    column_name = "content"
    dataset = load_dataset(spark, file_path, column_name)

    dataset.show(5)  # Display the first 5 rows for verification
    spark.stop()
