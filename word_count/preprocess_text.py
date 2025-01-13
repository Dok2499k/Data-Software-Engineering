from pyspark.sql.functions import col, lower, regexp_replace, split

def preprocess_text(reviews_df, column_name):
    return reviews_df \
        .withColumn("clean_text", lower(col(column_name))) \
        .withColumn("clean_text", regexp_replace(col("clean_text"), "[^a-zA-Z0-9\\s]", "")) \
        .withColumn("words", split(col("clean_text"), "\\s+"))