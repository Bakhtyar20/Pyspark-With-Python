# Pyspark-With-Python

In the selected cell (Cell 11), the code is using SQL within a Python string to query a Spark DataFrame. This approach is not directly related to functional programming. However, functional programming concepts can be applied in subsequent transformations and actions on the DataFrame.

Functional programming in Spark typically involves using higher-order functions such as map, filter, reduce, and DataFrame methods like select, withColumn, groupBy, and agg. These functions allow you to apply transformations and actions in a declarative manner, which is a key aspect of functional programming.

Here is a brief explanation of how functional programming could be applied to the DataFrame df_fleet_csv:

Transformation with select and withColumn:

You can use select to choose specific columns.
withColumn can be used to create new columns or modify existing ones.
Filtering with filter:

The filter method allows you to filter rows based on a condition.
Aggregation with groupBy and agg:

You can group data using groupBy and perform aggregations with agg.
Here is an example of how you might apply functional programming concepts to df_fleet_csv:

from pyspark.sql.functions import col, max
from pyspark.sql.window import Window

# Create a window partitioned by KEY_CONTRACT and ordered by CAMPAIGN in descending order
window_spec = Window.partitionBy("KEY_CONTRACT").orderBy(col("CAMPAIGN").desc())

# Add a new column MAX_CAMPAIGN that contains the maximum campaign for each KEY_CONTRACT
df_fleet_csv = df_fleet_csv.withColumn("MAX_CAMPAIGN", max(col("CAMPAIGN")).over(window_spec))

# Filter the rows where CAMPAIGN is equal to MAX_CAMPAIGN to get the rows with the maximum campaign for each KEY_CONTRACT
df_fleet_csv = df_fleet_csv.filter(col("CAMPAIGN") == col("MAX_CAMPAIGN"))

# Drop the MAX_CAMPAIGN column
df_fleet_csv = df_fleet_csv.drop("MAX_CAMPAIGN")


In this example:

withColumn is used to add a new column.
filter is used to filter rows based on a condition.
drop is used to remove a column.
These operations are chainable and do not mutate the original DataFrame, which aligns with the principles of functional programming.
