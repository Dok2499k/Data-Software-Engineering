from pyspark.sql import SparkSession
from load_data import load_dataset
from preprocess_text import preprocess_text
from count_words import count_words

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Amazon Reviews Word Count") \
        .master("local[*]") \
        .getOrCreate()

    # File path and column name
    file_path = "amazon_product_reviews.csv"
    column_name = "content"

    # Stop words list
    stop_words = ["the", "and", "is", "in", "to", "of", "a", "that", "it", "on", "with", "for", "as", "this", "was", "but"]

    # Load dataset
    reviews_df = load_dataset(spark, file_path, column_name)
    print("Dataset loaded successfully:")
    reviews_df.show(5)

    # Preprocess text
    cleaned_df = preprocess_text(reviews_df, column_name)
    print("Text preprocessing completed:")
    cleaned_df.show(5)

    # Count words
    word_counts = count_words(cleaned_df, stop_words)
    print("Top words by frequency:")
    word_counts.show(10)

    # Save results
    output_path = "output"
    word_counts.write.csv(output_path,header=True, mode="overwrite")
    print(f"Results saved to {output_path}")

    spark.stop()
