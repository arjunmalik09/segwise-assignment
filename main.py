# Given the data of some of the android apps from playstore, this pyspark job generates insights from it.
# Insights provide count of apps based on various combination of fields provided in input.
import sys
from pyspark.sql import SparkSession

def transform(spark, df):
    """
    1. Bin each integer column of dataframe. Create 10 bin for each integer column
    2. For each k combination out of n columns,
        1. Group the k column combination and find total count
    """
    column_names = df.columns
    rows = df.offset(0).limit(100).collect()
    data = [{col: row[col]} for col in column_names for row in rows]
    return spark.createDataFrame(data)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <csv-file-name>", file=sys.stderr)
        sys.exit(-1)
    filename = sys.argv[1]
    if filename[-4:] != ".csv":
        print("Invalid Input File: Please input csv file", file=sys.stderr)
        sys.exit(-1)
    output_filename = f"output_{filename}"
    spark = SparkSession.builder.appName("Counts").getOrCreate()
    df = spark.read.csv(filename)
    output_df = transform(spark, df)
    output_df.write.csv(output_filename, header=True)
    spark.stop()
