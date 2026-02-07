# Databricks notebook source
# DBTITLE 1,settting_notebook_run_env
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
run_env = "dev" if "dev" in notebook_path.lower() else "prod"

print(run_env , notebook_path)

# COMMAND ----------

# DBTITLE 1,creating_metadata_table
spark.sql(f"""
CREATE or replace table silver.processed_files_orders (
  file_name_metadata STRING,
  processed_at TIMESTAMP
)
USING delta
LOCATION 'abfss://ordercontainer@caxnladls.dfs.core.windows.net/{run_env}/metadatatable/'""")


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

source_path = "abfss://ordercontainer@caxnladls.dfs.core.windows.net/orders"

df = (
    spark.read
         .option("header", True)
         .csv(source_path)
         .withColumn("file_name_metadata", col("_metadata.file_path"))
)
df.display()

# COMMAND ----------

processed_files = spark.table("silver.processed_files_orders")

new_files_df = df.join(
    processed_files,
    df["file_name_metadata"] == processed_files["file_name_metadata"],
    how="left_anti"   # <-- removes already processed files
)
new_files_df.display()

# COMMAND ----------

new_files_df \
    .drop("file_name_metadata") \
    .write \
    .mode("append") \
    .option("path", f"abfss://ordercontainer@caxnladls.dfs.core.windows.net/silver/{run_env}/") \
    .saveAsTable(f"silver.silver_orders_jobs")

# COMMAND ----------

(
    new_files_df
      .select("file_name_metadata")
      .distinct()
      .withColumn("processed_at", current_timestamp())
      .write
      .mode("append")
      .saveAsTable("silver.processed_files_orders")
)