from pyspark.sql.functions import col, when, split, expr
from pyspark.sql.types import FloatType


def parse_altitude_column(coffee):
  """
  Parse the altitude column to be a float.

  If there is a dash in the altitude column, we need to divide the sum of the altitudes by the number of altitudes.
  """
  
  df = coffee.withColumn(
        "altitude",
        when(
            col("altitude").contains("-"), 
            expr( 
                """aggregate(transform(split(altitude, '-'),
                x -> cast(x as int)), 0, (acc, x) -> acc + x) / size(split(altitude, '-'))"""
            ),
        ).otherwise(col("altitude")), 
    )
  return df

