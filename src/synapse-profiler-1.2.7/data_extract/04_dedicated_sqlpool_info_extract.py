# Databricks notebook source
# DBTITLE 1,Widgets
dbutils.widgets.text("sqlpool_name", "")
dbutils.widgets.text("workspaceName","workspace name")
wp_name = dbutils.widgets.get("workspaceName")

dbutils.widgets.text("developmentEndpoint","Development Endpoint")
dev_ep = dbutils.widgets.get("developmentEndpoint")

dbutils.widgets.text("dedicatedSQLEndpoint","Dedicated SQL Endpoint")
dedicated_ep = dbutils.widgets.get("dedicatedSQLEndpoint")

dbutils.widgets.text("serverlessSQLEndpoint","Serverless SQL Endpoint")
serverless_ep = dbutils.widgets.get("serverlessSQLEndpoint")

dbutils.widgets.text("serverlessOnly","Serverless Only")
exclude_dedicated_sql_pool = dbutils.widgets.get("serverlessOnly")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 01. Intializations

# COMMAND ----------

# MAGIC %md
# MAGIC #### A) setup

# COMMAND ----------

# DBTITLE 0,setup
# MAGIC %run ../00_settings

# COMMAND ----------

# MAGIC %md
# MAGIC #### B) profiler_functions

# COMMAND ----------

# DBTITLE 0,profiler_functions
# MAGIC %run ../common/02_profiler_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### C) profiler_classes

# COMMAND ----------

# DBTITLE 0,profiler_classes
# MAGIC %run ../common/03_profiler_classes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 01. Warehouse Objects

# COMMAND ----------

# DBTITLE 1,Validate Input SQL Pool Name
sqlpool_name = dbutils.widgets.get("sqlpool_name")
if not sqlpool_name:
  raise ValueError("ERROR: Invalid/Empty sqlpool_name. Expecting a valid for Notebook input paramater 'sqlpool_name'")

# COMMAND ----------

# DBTITLE 1,Data Paths
import os 
dedicated_staging_location= get_staging_dbfs_path("sqlpools-dedicated", True)
print(f"dedicated_staging_location → {dedicated_staging_location}")

# COMMAND ----------

# DBTITLE 1,Read Required Profiler Settings 
profiler_settings = get_synapse_profiler_settings()
redact_sql_text_flag = "redact_sql_pools_sql_text" in profiler_settings and profiler_settings["redact_sql_pools_sql_text"]

# COMMAND ----------

# DBTITLE 1,Create Reader and Writers
sqlpool_reader    = get_dedicated_sqlpool_reader(sqlpool_name)
sqlpool           = SynapseDedicatedSqlPool(sqlpool_name, sqlpool_reader)
dataframe_writer  = ProfilerDataframeWriter(dedicated_staging_location, sqlpool.get_name())

# COMMAND ----------

# MAGIC %md
# MAGIC #### A) Tables

# COMMAND ----------

# DBTITLE 1,List Tables
obj_name = "tables"
sqlpool_tables = sqlpool.list_tables()
dataframe_writer.write(sqlpool_tables, obj_name, "overwrite")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(dedicated_staging_location, obj_name, f"pool_name={sqlpool.get_name()}"))
del obj_name, sqlpool_tables # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC #### B) Columns

# COMMAND ----------

# DBTITLE 1,List Columns
obj_name = "columns"
sqlpool_columns = sqlpool.list_columns()
dataframe_writer.write(sqlpool_columns, obj_name, "overwrite")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(dedicated_staging_location, obj_name, f"pool_name={sqlpool.get_name()}"))
del obj_name, sqlpool_columns # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC #### C) Views

# COMMAND ----------

# DBTITLE 1,List Views
obj_name = "views"
sqlpool_views = sqlpool.list_views(redact_sql_text_flag)
dataframe_writer.write(sqlpool_views, obj_name, "overwrite")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(dedicated_staging_location, obj_name, f"pool_name={sqlpool.get_name()}"))
del obj_name, sqlpool_views # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC #### D) Routines ( Procedures + Functions)

# COMMAND ----------

# DBTITLE 1,List Routines
obj_name = "routines"
sqlpool_routines = sqlpool.list_routines(redact_sql_text_flag)
dataframe_writer.write(sqlpool_routines, obj_name, "overwrite")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(dedicated_staging_location, obj_name, f"pool_name={sqlpool.get_name()}"))
del obj_name, sqlpool_routines # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC #### E) Materlized Views (TBD)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 02. Warehouse Storage Infomation

# COMMAND ----------

# DBTITLE 1,Get Storage Info
obj_name = "storage_info"
sqlpool_sessions = sqlpool.get_db_storage_info()
dataframe_writer.write(sqlpool_sessions, obj_name, "overwrite")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(dedicated_staging_location, obj_name, f"pool_name={sqlpool.get_name()}"))
del obj_name, sqlpool_sessions # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC ### End