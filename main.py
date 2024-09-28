# Given the data of some of the android apps from playstore, this pyspark job generates insights from it.
# Insights provide count of apps based on various combination of fields provided in input.
import sys
from pyspark.sql import SparkSession
import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Bin each column of dataframe. Create 10 bin for each column
    2. For each k combination out of n columns,
        1. Group the k column combination and find total count
    """
    pass

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
    df.applyInPandas(transform, schema=df.schema)
    df.show()
    # counts = lines.flatMap(lambda x: x.split(',')).map(lambda x: (x, 1))
    # output = counts.collect()
    # with open("output.csv", "rw+") as output_file:
    #     for (word, count) in output:
    #         print("%s: %i" % (word, count))
    #         output_file.write(word, count)
    spark.stop()
