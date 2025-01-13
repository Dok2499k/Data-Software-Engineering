from pyspark.sql.functions import explode, col

def count_words(cleaned_df, stop_words):

    return cleaned_df \
        .select(explode(col("words")).alias("word")) \
        .groupBy("word") \
        .count() \
        .filter((col("word") != "") & (~col("word").isin(stop_words))) \
        .orderBy(col("count").desc())
