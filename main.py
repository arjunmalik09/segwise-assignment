# Given the data of some of the android apps from playstore, this pyspark job generates insights from it.
# Insights provide count of apps based on various combination of fields provided in input.
import sys
from itertools import combinations
import time
from pyspark.sql import SparkSession

def bin_dataframe(spark, df):
    # from pyspark.ml.feature import QuantileDiscretizer
    # output_column_names = [col + "_bin" for col in column_names]
    # qds = QuantileDiscretizer(relativeError=0.01, handleInvalid="error", numBuckets=10, inputCols=column_names, outputCols=output_column_names)
    # binner = lambda df: qds.setHandleInvalid("keep").fit(df).transform(df)
    return df

def transform(spark, df):
    """
    1. Bin each integer column of dataframe. Create 10 bin for each integer column
    2. For each k combination out of n columns,
        1. Group the k column combination and find total count
    3. Return the dataframes
    """
    tranformed_dfs = []
    column_names = df.columns
    df = bin_dataframe(spark, df)
    for k in range(1, len(column_names) + 1):
        print(f"Tranform dataframe as {k} length")
        for column_combination in combinations(column_names, k):
            tranformed_dfs.append(
                df.select(column_combination).groupBy(column_combination).count()
            )
    return tranformed_dfs

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <csv-file-name>", file=sys.stderr)
        sys.exit(-1)
    filename = sys.argv[1]
    if filename[-4:] != ".csv":
        print("Invalid Input File: Please input csv file", file=sys.stderr)
        sys.exit(-1)
    spark = SparkSession.builder.appName("Counts").getOrCreate()
    df = spark.read.csv(filename)
    output_dfs = transform(spark, df)
    for output_df in output_dfs:
        output_filename = f"output_{int(time.time() * 1000)}_{filename}"
        output_df.write.csv(output_filename, header=True)
    spark.stop()
