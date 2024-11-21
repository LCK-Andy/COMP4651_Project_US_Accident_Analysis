from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import pandas as pd
import os

S3_DATA_SOURCE_PATH = "s3://comp4651projectbucket/Data_source/US_Accidents_March23.csv"
S3_DATA_OUTPUT_PATH = "s3://comp4651projectbucket/Data_output/CountByCity"
LOCAL_PNG_PATH = "/tmp/top_10_cities.png"  # Local path to save the PNG


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("COMP4651Project").getOrCreate()

    # Read the CSV file from S3
    df = spark.read.csv(S3_DATA_SOURCE_PATH, header=True, inferSchema=True)

    # Print total number of records
    print("Total number of records in the source data: " + str(df.count()))

    # Count the number of accident cases per city
    city_df = df.groupBy("City").count().withColumnRenamed("count", "Cases")

    # Get the top 10 cities by accident cases
    top_10_cities = city_df.orderBy(col("Cases").desc()).limit(15)

    # Convert to Pandas DataFrame for plotting
    top_10_cities_pd = top_10_cities.toPandas()

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.barh(top_10_cities_pd["City"], top_10_cities_pd["Cases"], color="skyblue")
    plt.xlabel("Number of Accidents")
    plt.title("Top 15 Cities by Number of Accidents")
    plt.gca().invert_yaxis()  # Invert y-axis to have the city with the most accidents on top
    plt.tight_layout()

    # Save the plot as PNG
    plt.savefig(LOCAL_PNG_PATH)
    plt.close()

    # Upload the PNG to S3
    import boto3

    s3 = boto3.client("s3")
    s3.upload_file(
        LOCAL_PNG_PATH,
        "comp4651projectbucket",
        "Data_output/CountByCity/top_10_cities.png",
    )

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
