# Databricks notebook source
# MAGIC %md
# MAGIC ### PySpark Schema Comparison UDF

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def compare_schemas(df_schema, delta_schema):

    df_fields = {field.name: field.dataType.simpleString() for field in df_schema.fields}
    delta_fields = {field.name: field.dataType.simpleString() for field in delta_schema.fields}

    all_columns = set(df_fields.keys()).union(delta_fields.keys())
    result = []

    for col in all_columns:
        src_type = df_fields.get(col, "Missing")
        dest_type = delta_fields.get(col, "Missing")
        
        if src_type == dest_type:
            status = "Match"
        elif src_type == "Missing":
            status = "Missing in Source"
        elif dest_type == "Missing":
            status = "Missing in Destination"
        else:
            status = "Mismatch"
 
        result.append((col, src_type, dest_type, status))
 
    return result

# Register the UDF
compare_schemas_udf = F.udf(lambda src, dest: compare_schemas(src, dest), StringType())


# COMMAND ----------

# Example Delta table schema and incoming DataFrame schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

delta_table_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

incoming_df_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Compare schemas
comparison_result = compare_schemas(incoming_df_schema, delta_table_schema)

# Create a DataFrame from the result
comparison_df = spark.createDataFrame(comparison_result, ["Column Name", "Source Data Type", "Destination Data Type", "Status"])

# Show the comparison result
comparison_df.show()


# COMMAND ----------


