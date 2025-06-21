# Databricks notebook source
# MAGIC %md
# MAGIC # Synapse Profiler (Legacy SQL DW Version)
# MAGIC > Version: 1.2.5
# MAGIC
# MAGIC This the main Notebook to Run for Profiler Data extract
# MAGIC
# MAGIC # Changelog
# MAGIC |Version|Last Updated|Contributor|
# MAGIC |-------|------------|-----------|
# MAGIC |1.2.5| 01/15/2024 | Suresh Matlapudi|

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Pool Names
# MAGIC > `Important Note:` 
# MAGIC Make sure the input SQL Pools (Dedicated) are **online at the time of running this Notebook**. 
# MAGIC For legacy SQL DW Service there is no Synapse Workspace attched. So we cannot query the status of the given SQL Pools using Workspace API to check on current state of the given pools.

# COMMAND ----------

# DBTITLE 1,Run this to Enter Dedicated SQL Pool Name(s)
dbutils.widgets.text("sqlpool_names", "", "SQL Pools (Comma Separated List)")

# COMMAND ----------

# DBTITLE 1,Validate Input SQL Pool Names
sqlpool_names_txt = dbutils.widgets.get("sqlpool_names")
sqlpool_names = [ entry.strip() for entry in sqlpool_names_txt.strip().split(",") if len(entry.strip()) ]
if not sqlpool_names:
  raise ValueError(f"ERROR: Invalid/Empty sqlpool_names [{sqlpool_names_txt}]. Expecting a valid for Notebook input paramater 'sqlpool_names' as comma separated list")
print(f"INFO: SQL Pools to extract: {sqlpool_names}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 01. Intializations

# COMMAND ----------

# MAGIC %md
# MAGIC #### A) Settings

# COMMAND ----------

# DBTITLE 1,Source Settings
# MAGIC %run ../00_settings

# COMMAND ----------

# MAGIC %md
# MAGIC #### B) profiler_functions

# COMMAND ----------

# DBTITLE 1,Profiler functions
# MAGIC %run ../common/02_profiler_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### C) profiler_classes

# COMMAND ----------

# DBTITLE 1,Profiler classes
# MAGIC %run ../common/03_profiler_classes

# COMMAND ----------

# DBTITLE 1,profiler_settings
profiler_settings = get_synapse_profiler_settings()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Test Settings and Connectivity

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1) Test the input DBFS location for 'data_staging_root'

# COMMAND ----------

# DBTITLE 1,data_staging_root
data_staging_root = profiler_settings["data_staging_root"]
print(f"INFO: data_staging_root is set to:  {data_staging_root}\n")
# Run dbutils fs.ls to check this location
dbutils.fs.ls(data_staging_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2) Test JDBC connectivity

# COMMAND ----------

# DBTITLE 1,Dedicated SQL Pool connectivity test
first_pool_name = sqlpool_names[0]
sqlpool_reader = get_dedicated_sqlpool_reader(first_pool_name)
sqlpool        = SynapseDedicatedSqlPool(first_pool_name, sqlpool_reader)
sqlpool_databases = sqlpool.list_databases()
display(sqlpool_databases)
del sqlpool_reader, sqlpool, sqlpool_databases  

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Run Profiler Steps
# MAGIC
# MAGIC >Following are the required steps
# MAGIC   1. Run Dedicated SQL Pool info extract (for each given sql pool)
# MAGIC   2. Run Dedicated SQL Pool activity extract (for each given sql pool)
# MAGIC   
# MAGIC - All of these steps can be run independently in parellelly
# MAGIC - Data extracts from above steps will be saved under subfolder of "data_staging_root" likes below
# MAGIC | SNO | Step Type                   | Target Folder |
# MAGIC | --- | --------------------------- | --------------------------- |
# MAGIC | 1   | Dedicated SQL Pool          | ${data_staging_root}/sqlpools-dedicated |
# MAGIC
# MAGIC **Note :** 
# MAGIC   -  Current version of the profiler uses Synapse DMVs for activity data which has limitation of 10K record at any time. 
# MAGIC   -  Which can be filed in few hours of activity depending on your usage volumne.
# MAGIC   -  So after intitial maual run of the profiler main , it is requried to schedule `04_dedicated_sqlpool_activity_extract` notebook as hourly job for a mininum of 48 hours to extract decent data for workload insights.
# MAGIC   -  Notebook `04_dedicated_sqlpool_activity_extract` is light weight (in term of resource needs) and it can be run on a  single node cluster. 
# MAGIC   - It extracts the command session/requests data in incremental manner

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.1) Run Dedicated Sql Pool extract 

# COMMAND ----------

synapse_profiler_settings = get_synapse_profiler_settings()
exclude_dedicated_sql_pools = synapse_profiler_settings.get("exclude_dedicated_sql_pools", None)
dedicated_sql_pools_profiling_list = sqlpool_names

print(f"INFO: exclude_dedicated_sql_pools        →  {exclude_dedicated_sql_pools}")
print(f"INFO: dedicated_sql_pools_profiling_list →  {dedicated_sql_pools_profiling_list}")

# COMMAND ----------

# DBTITLE 1,Run 04_dedicated_sqlpool_info_extract   (for each pool)
for idx, pool_name in enumerate(dedicated_sql_pools_profiling_list):
  print(f"   {idx :02d})  {pool_name} : RUNNING extract...")
  dbutils.notebook.run("./04_dedicated_sqlpool_info_extract", 0, {"sqlpool_name": pool_name} )


# COMMAND ----------

# DBTITLE 1,Run 04_dedicated_sqlpools_activity_extract   (with pool list as notebook parameter)
sqlpool_names_to_profile = ",".join(dedicated_sql_pools_profiling_list)
print(f"INFO: Running 04_dedicated_sqlpools_activity_extract with sqlpool_names  → {sqlpool_names_to_profile} ...")
dbutils.notebook.run("./04_dedicated_sqlpools_activity_extract", 0, {"sqlpool_names": sqlpool_names_to_profile} )

# COMMAND ----------

# MAGIC %md
# MAGIC ### End