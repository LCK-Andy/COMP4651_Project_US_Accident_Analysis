from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import pandas as pd
import os
import boto3

S3_DATA_SOURCE_PATH = "s3://comp4651projectbucket/Data_source/US_Accidents_March23.csv"
S3_DATA_OUTPUT_PATH = "s3://comp4651projectbucket/Data_output/CountByState"
LOCAL_PNG_PATH = "/tmp/top_10_states.png"  # Local path to save the PNG


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("COMP4651Project").getOrCreate()

    # Read the CSV file from S3
    df = spark.read.csv(S3_DATA_SOURCE_PATH, header=True, inferSchema=True)

    # Print total number of records
    print("Total number of records in the source data: " + str(df.count()))

    # Count the number of accident cases per state
    state_df = df.groupBy("State").count().withColumnRenamed("count", "Cases")

    # Get the top 10 states by accident cases
    top_10_states = state_df.orderBy(col("Cases").desc()).limit(10)

    # Convert to Pandas DataFrame for plotting
    top_10_states_pd = top_10_states.toPandas()

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.barh(top_10_states_pd["State"], top_10_states_pd["Cases"], color="skyblue")
    plt.xlabel("Number of Accidents")
    plt.title("Top 10 States by Number of Accidents")
    plt.gca().invert_yaxis()  # Invert y-axis to have the state with the most accidents on top
    plt.tight_layout()

    # Save the plot as PNG
    plt.savefig(LOCAL_PNG_PATH)
    plt.close()

    # Upload the PNG to S3
    s3 = boto3.client("s3")
    s3.upload_file(
        LOCAL_PNG_PATH,
        "comp4651projectbucket",
        "Data_output/CountByState/top_10_states.png",
    )

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
