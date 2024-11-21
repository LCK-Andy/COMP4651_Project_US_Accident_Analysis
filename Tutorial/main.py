from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

S3_DATA_SOURCE_PATH = "s3://comp4651projectbucket/Data_source/US_Accidents_March23.csv"
S3_DATA_OUTPUT_PATH = "s3://comp4651projectbucket/Data_output"


def main():
    spark = SparkSession.builder.appName("COMP4651Peoject").getOrCreate()
    df = spark.read.csv(S3_DATA_SOURCE_PATH, header=True, inferSchema=True)

    print("Total number of records in the source data: " + df.count())
    spark.stop()


if __name__ == "__main__":
    main()
