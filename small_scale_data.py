from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

LOCAL_DATA_SOURCE_PATH = "US_Accidents_March23.csv"
LOCAL_DATA_OUTPUT_PATH = "US_Accidents_March23_small.csv"


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("COMP4651Project").getOrCreate()

    # Read the dataset
    df = spark.read.csv(LOCAL_DATA_SOURCE_PATH, header=True, inferSchema=True)

    # Shuffle the dataset using orderBy with a random column
    shuffled_df = df.orderBy(rand())

    # Select the first 5000 rows
    small_df = shuffled_df.limit(5000)

    # Write the small dataframe to a new CSV file
    small_df.write.csv(LOCAL_DATA_OUTPUT_PATH, header=True, mode="overwrite")

    print("Small scale data saved to: " + LOCAL_DATA_OUTPUT_PATH)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
