# Databricks notebook source
# MAGIC %md
# MAGIC ### 01. Intializations

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("sqlpool_names", "", "SQL Pools (Comma Separated List)")
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
# MAGIC ### 01. Initializations

# COMMAND ----------

# DBTITLE 1,Validate Input SQL Pool Names
sqlpool_names_txt = dbutils.widgets.get("sqlpool_names")
sqlpool_names = [ entry for entry in sqlpool_names_txt.strip().split(",") if len(entry.strip()) ]
if not sqlpool_names:
  raise ValueError(f"ERROR: Invalid/Empty sqlpool_names [{sqlpool_names_txt}]. Expecting a valid for Notebook input paramater 'sqlpool_names' as comma separated list")
print(f"INFO: SQL Pools to extract: {sqlpool_names}")

# COMMAND ----------

# DBTITLE 1,Data Paths
import os 
dedicated_staging_location= get_staging_dbfs_path("sqlpools-dedicated", True)
print(f"dedicated_staging_location → {dedicated_staging_location}")

# COMMAND ----------

# DBTITLE 1,get_sqlpool_instance
# get_sqlpool_instance
def get_sqlpool_instance(sqlpool_name):
    sqlpool_reader    = get_dedicated_sqlpool_reader(sqlpool_name)
    sqlpool           = SynapseDedicatedSqlPool(sqlpool_name, sqlpool_reader)
    return sqlpool

# COMMAND ----------

# MAGIC %md
# MAGIC ### 02. Warehouse Sessions & Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ##### i) Sessions funtions

# COMMAND ----------

# DBTITLE 1,Extract Sessions
# extract_sessions
def extract_sessions(sqlpool, staging_location):
  obj_name = "sessions"
  max_login_time = get_max_column_value("login_time", os.path.join(staging_location, obj_name))
  sqlpool_sessions = sqlpool.list_sessions(max_login_time)
  dataframe_writer  = ProfilerDataframeWriter(staging_location, sqlpool.get_name())
  dataframe_writer.write(sqlpool_sessions, obj_name, "append")

# COMMAND ----------

# DBTITLE 1,Check Output
# sessions_data_check
def sessions_data_check(sqlpool, staging_location):
  obj_name = "sessions"
  check_target_folder_content(os.path.join(staging_location, obj_name, f"pool_name={sqlpool.get_name()}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ii) Session Requests funtions

# COMMAND ----------

# DBTITLE 1,Extract Session Requests
# extract_session_requests
def extract_session_requests(sqlpool, staging_location, redact_sql_text=False):
  obj_name = "session_requests"
  max_end_time = get_max_column_value("end_time", os.path.join(staging_location, obj_name))
  sqlpool_session_requests = sqlpool.list_requests(redact_sql_text = redact_sql_text, min_end_time = max_end_time)
  dataframe_writer  = ProfilerDataframeWriter(staging_location, sqlpool.get_name())
  dataframe_writer.write(sqlpool_session_requests, obj_name, "append")

# COMMAND ----------

# DBTITLE 1,Check Output
# session_requests_data_check
def session_requests_data_check(sqlpool, staging_location):
  obj_name = "session_requests"
  check_target_folder_content(os.path.join(staging_location, obj_name, f"pool_name={sqlpool.get_name()}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### iii) Run extract for all Pools

# COMMAND ----------

# DBTITLE 1,run: extract_sessions
profiler_settings = get_synapse_profiler_settings()
redact_sql_text_flag = "redact_sql_pools_sql_text" in profiler_settings and profiler_settings["redact_sql_pools_sql_text"]

for idx, sqlpool_name in enumerate(sqlpool_names):
  print(f"{'*'*60}")
  print(f"INFO: {idx+1}) sqlpool_name → {sqlpool_name}")
  if redact_sql_text_flag:
    print(f"     redact_sql_text set to {redact_sql_text_flag}")
  sqlpool = get_sqlpool_instance(sqlpool_name)
  
  # Extract session Data
  extract_sessions(sqlpool, dedicated_staging_location)
  sessions_data_check(sqlpool, dedicated_staging_location)

# COMMAND ----------

# DBTITLE 1,run: extract_session_requests
profiler_settings = get_synapse_profiler_settings()
redact_sql_text_flag = "redact_sql_pools_sql_text" in profiler_settings and profiler_settings["redact_sql_pools_sql_text"]

for idx, sqlpool_name in enumerate(sqlpool_names):
  print(f"{'*'*60}")
  print(f"INFO: {idx+1}) sqlpool_name → {sqlpool_name}")
  if redact_sql_text_flag:
    print(f"     redact_sql_text set to {redact_sql_text_flag}")
  sqlpool = get_sqlpool_instance(sqlpool_name)
  
  # Extract session requests
  extract_session_requests(sqlpool, dedicated_staging_location, redact_sql_text_flag)  
  session_requests_data_check(sqlpool, dedicated_staging_location)


# COMMAND ----------

# DBTITLE 1,Check the request window
request_win_df = spark.sql("""
with requests as (
  select * from json.`{}`
)
select
  pool_name,
  extract_ts, 
  min(start_time) as min_request_start_time, 
  max(start_time) as max_start_time, 
  datediff(MINUTE, min(start_time), max(start_time)) extract_window_minutes,
  datediff(HOUR, min(start_time), max(start_time)) extract_window_hours, 
  count(*) as num_requests
from requests
group by extract_ts, pool_name
order by extract_ts desc, pool_name
""".format(
  os.path.join(dedicated_staging_location, "session_requests")
))
request_data_extract_info = request_win_df.first()
display(request_win_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### End