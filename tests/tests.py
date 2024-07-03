def convert_camelcase_to_snakecase(df):
  """
  Convert column names in a DataFrame to snake case.

  Args:
      df (DataFrame): Input DataFrame.

  Returns:
      DataFrame: DataFrame with column names converted to snake case.
  """
  # Function to convert a string to snake case
  def camel_to_snake(name):
    import re
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
      
  columns = df.columns

  # Create a mapping of old column names to new column names
  column_mapping = {col: camel_to_snake(col) for col in columns}
  df_snake_case = df.selectExpr(*[f"`{col}` AS `{column_mapping[col]}`" for col in columns])

  return df_snake_case

def test_rename_columns_to_snake_case():
  # create sample data that has columns with camelCase or CamelCase formatted names
  data = [(1, 2, 3), (4, 5, 6)]
  df = spark.createDataFrame(data, ["camelCase", "CamelCase", "snake_case"])
  expected_df = spark.createDataFrame(data, ["camel_case", "camel_case", "snake_case"])

