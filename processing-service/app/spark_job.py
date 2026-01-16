from pyspark.sql import SparkSession
from pyspark.sql.functions import col

RAW_DATA_PATH = "/data/raw"
PROCESSED_DATA_PATH = "/data/processed"


def main():
    spark = (
        SparkSession.builder
        .appName("MiniDataPlatformProcessing")
        .master("local[*]")
        .getOrCreate()
    )

    # Read all CSV files from raw layer
    df = spark.read.option("header", True).csv(f"{RAW_DATA_PATH}/*.csv")

    if df.rdd.isEmpty():
        print("No data found in raw layer.")
        return

    # Basic transformations
    df_clean = (
        df
        .filter(col("event_type").isNotNull())
        .groupBy("event_type")
        .count()
        .orderBy("count", ascending=False)
    )

    # Write processed output
    (
        df_clean
        .coalesce(1)  # keep output readable
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(PROCESSED_DATA_PATH)
    )

    print("Processing completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()