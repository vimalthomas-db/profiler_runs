# Databricks notebook source
# MAGIC %md
# MAGIC # Synapse Profiler Data Analyzer
# MAGIC > Version: 0.1.0
# MAGIC
# MAGIC This the main Notebook to analyze the extracted Profiler Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01. Initializations

# COMMAND ----------

dbutils.widgets.text("analyzer_schema", "")

# COMMAND ----------

# DBTITLE 1,Run common settings
# MAGIC %run ../00_settings

# COMMAND ----------

# DBTITLE 1,Get utility functions
# MAGIC %run ../common/02_profiler_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02. Create Schema

# COMMAND ----------

# DBTITLE 1,Create Schema if required
analyzer_schema = dbutils.widgets.get("analyzer_schema")
if not analyzer_schema:
    raise ValueError("ERROR: Missing 'analyzer_schema' value for creating/checking analyzer table definitions!!!")
  
# Create Database if not exists
spark.sql(f"create database if not exists {analyzer_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 03. Create Table Definitions

# COMMAND ----------

# DBTITLE 1,create_tables
from collections import namedtuple

TableCategory = namedtuple("TableCategory", ["name", "folder_name"])

# create_tables
def create_tables(table_category, case_sensitive_schema=False):
  '''
    Utility function to create tables/view definitions
  '''
  category = table_category
  print(f"INFO: Creating table for {category.name} category... ")
  category_data_location = get_staging_dbfs_path(category.folder_name)
  print(f"      category_data_location: {category_data_location}")
  try:
    dbutils.fs.ls(category_data_location)
  except:
    print(f"      SKIPPING '{category.name}' category as above data folder not present (or data extract for these category tables are excluded in run settings)")
    return
  
  # update session settings of required
  session_sql_case_sensitivity = spark.conf.get("spark.sql.caseSensitive")
  if case_sensitive_schema:
    spark.conf.set("spark.sql.caseSensitive", "true")
  # list subfolder
  for subfolder in dbutils.fs.ls(category_data_location):
    table_name = f"{category.name}_{subfolder.name}".strip("/")
    print(f'\t> Createng Table(View): {table_name} ...')
    spark.sql(f"CREATE OR REPLACE VIEW {analyzer_schema}.{table_name} as SELECT * from json.`{subfolder.path}`")
  # restore session settings
  spark.conf.set("spark.sql.caseSensitive", session_sql_case_sensitivity)    

# COMMAND ----------

# DBTITLE 1,Create Views for workspace tables
import os
workspace_tables = TableCategory("workspace", "workspace")
create_tables(workspace_tables, True)

# COMMAND ----------

# DBTITLE 1,Create Views for Metrics
metrics_tables = TableCategory("metrics", "monitoring_metrics")
create_tables(metrics_tables)

# COMMAND ----------

# DBTITLE 1,Create Views for Serverless SQL Pool
serverless_sqlpool_tables = TableCategory("serverless", "sqlpools-serverless")
create_tables(serverless_sqlpool_tables)


# COMMAND ----------

# DBTITLE 1,Create Views for Dedicated SQL Pool
  dedicated_sqlpool_tables = TableCategory("dedicated", "sqlpools-dedicated") 
  create_tables(dedicated_sqlpool_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 04. Check Created Tables

# COMMAND ----------

spark.catalog.setCurrentDatabase(analyzer_schema)

# COMMAND ----------

# DBTITLE 1,Show tables
# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace_sql_pools

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dedicated_tables