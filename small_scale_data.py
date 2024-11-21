from pyspark.sql import SparkSession

LOCAL_DATA_SOURCE_PATH = "US_Accidents_March23.csv"
LOCAL_DATA_OUTPUT_PATH = "US_Accidents_March23_small.csv"

def main():
    spark = SparkSession.builder.appName("COMP4651Project").getOrCreate()
    df = spark.read.csv(LOCAL_DATA_SOURCE_PATH, header=True, inferSchema=True)

    # Select the first 1000 rows
    small_df = df.limit(1000)

    # Write the small dataframe to a new CSV file
    small_df.write.csv(LOCAL_DATA_OUTPUT_PATH, header=True, mode="overwrite")

    print("Small scale data saved to: " + LOCAL_DATA_OUTPUT_PATH)
    spark.stop()

if __name__ == "__main__":
    main()
