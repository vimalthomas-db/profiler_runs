# Databricks notebook source
# MAGIC %md
# MAGIC ### 01. Intializations

# COMMAND ----------

# MAGIC %md
# MAGIC #### A) setup

# COMMAND ----------

# DBTITLE 1,widgets
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
# MAGIC ####  → Set Data Paths

# COMMAND ----------

# DBTITLE 1,Data Paths
serverless_staging_location= get_staging_dbfs_path("sqlpools-serverless", True)
print(f"\nserverless_staging_location → {serverless_staging_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####  → Create Reader and Writers

# COMMAND ----------

# DBTITLE 1,Create Reader and Writers
sqlpool_reader    = get_serverless_sqlpool_reader()
sqlpool           = SynapseServerlessSqlPool(sqlpool_reader)
dataframe_writer  = ProfilerDataframeWriter(serverless_staging_location, sqlpool.get_name())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 02. Warehouse Sessions & Queries
# MAGIC
# MAGIC
# MAGIC Note that these table schema between serverless and dedicated are different

# COMMAND ----------

# MAGIC %md
# MAGIC ##### i) Sessions

# COMMAND ----------

# DBTITLE 1,List Sessions
obj_name = "sessions"
max_last_request_start_time = get_max_column_value("last_request_start_time", os.path.join(serverless_staging_location, obj_name))
sqlpool_sessions = sqlpool.list_sessions(max_last_request_start_time)
dataframe_writer.write(sqlpool_sessions, obj_name, "append")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(serverless_staging_location, obj_name))
del obj_name, sqlpool_sessions # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ii) Session Requests

# COMMAND ----------

# DBTITLE 1,List Session Requests
obj_name = "session_requests"
max_start_time= get_max_column_value("start_time", os.path.join(serverless_staging_location, obj_name))
sqlpool_session_requests = sqlpool.list_requests(max_start_time)
dataframe_writer.write(sqlpool_session_requests, obj_name, "append")

# COMMAND ----------

# DBTITLE 1,Check output
check_target_folder_content(os.path.join(serverless_staging_location, obj_name))
del obj_name, sqlpool_session_requests # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC ##### iii) Query Stats

# COMMAND ----------

# DBTITLE 1,Query Stats
obj_name = "query_stats"
max_last_execution_time= get_max_column_value("last_execution_time", os.path.join(serverless_staging_location, obj_name))
sqlpool_query_stats = sqlpool.list_query_stats(max_last_execution_time)
dataframe_writer.write(sqlpool_query_stats, obj_name, "append")

# COMMAND ----------

# DBTITLE 1,Check output
check_target_folder_content(os.path.join(serverless_staging_location, obj_name))
del obj_name, sqlpool_query_stats # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC ##### iv) Query History

# COMMAND ----------

# DBTITLE 1,Query Requests History
obj_name = "requests_history"
max_end_time= get_max_column_value("end_time", os.path.join(serverless_staging_location, obj_name))
sqlpool_requests_history = sqlpool.query_requests_history(max_end_time)
dataframe_writer.write(sqlpool_requests_history, obj_name, "append")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(serverless_staging_location, obj_name))
del obj_name, sqlpool_requests_history # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC ### End