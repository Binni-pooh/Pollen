from pyspark.sql import functions as F


def nan_counter(dataframe):
    numerical_cols = [c for c, dtype in dataframe.dtypes if dtype in ('double', 'float', 'int')]

    nan_counts = dataframe.select([
        F.count(F.when(F.col(c).isNaN() | F.col(c).isNull(), c)).alias(c)
        for c in numerical_cols
    ])

    nan_counts.show(truncate=False)