# Databricks notebook source
# MAGIC %md
# MAGIC # Synapse Profiler
# MAGIC > Version: 1.2.5
# MAGIC
# MAGIC This the main Notebook to Run for Profiler Data extract
# MAGIC
# MAGIC # Changelog
# MAGIC |Version|Last Updated|Contributor|
# MAGIC |-------|------------|-----------|
# MAGIC |1.2.6| 05/17/2024 | Suresh Matlapudi|
# MAGIC |1.2.5| 01/15/2024 | Suresh Matlapudi|
# MAGIC |1.2.4| 11/21/2023 | Suresh Matlapudi|
# MAGIC |1.2.3| 10/20/2023 | Suresh Matlapudi|
# MAGIC |1.2.2| 10/03/2023 | Suresh Matlapudi|
# MAGIC |1.2.0| 09/28/2023 | Suresh Matlapudi|
# MAGIC |1.1.0| 09/07/2023 | Suresh Matlapudi|

# COMMAND ----------

# MAGIC %md
# MAGIC ### 01. Intializations

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

# MAGIC %md
# MAGIC #### A) setup

# COMMAND ----------

# DBTITLE 1,Azure SDK setup
# MAGIC %run ../common/01_azure_sdk_setup

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

# MAGIC %md
# MAGIC #### → create data_staging_root

# COMMAND ----------

# DBTITLE 1,Create "data_staging_root"  if required
profiler_settings = get_synapse_profiler_settings()
data_staging_root = profiler_settings["data_staging_root"]
print(f"INFO:  data_staging_root →  {data_staging_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Test Settings and Connectivity

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1) Test the input DBFS location for 'data_staging_root'

# COMMAND ----------

# DBTITLE 1,data_staging_root
print(f"INFO: data_staging_root is set to:  {data_staging_root}\n")
# Run dbutils fs.ls to check this location
dbutils.fs.ls(data_staging_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2) Test Azure API connectivity

# COMMAND ----------

# DBTITLE 1,Get Synapse Workspace Info
synapse_workspace_settings = get_synapse_workspace_settings()
workspace_tz = synapse_workspace_settings["tz_info"]

#-----------------------------------------------------
# create workspace instance to run sdk query
#-----------------------------------------------------
artifacts_client = get_synapse_artifacts_client()
workspace_instance = SynapseWorkspace(workspace_tz, artifacts_client)
print(json.dumps(workspace_instance.get_workspace_info(keep=[]), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.3) Test JDBC connectivity

# COMMAND ----------

# DBTITLE 1,Serverless SQL Pool connectivity test
if synapse_profiler["exclude_serverless_sql_pool"] == True:
  print('INFO: "exclude_serverless_pool" configuration is set to True. Skipping Serverless SQL Pool connectivity test')  
else:
  sqlpool_reader    = get_serverless_sqlpool_reader()
  sqlpool           = SynapseServerlessSqlPool(sqlpool_reader)
  sqlpool_databases = sqlpool.list_databases()
  display(sqlpool_databases)
  del sqlpool_reader, sqlpool, sqlpool_databases

# COMMAND ----------

# DBTITLE 1,Dedicated SQL Pool connectivity test
if synapse_profiler["exclude_dedicated_sql_pools"] == True:
  print('INFO: "exclude_dedicated_sql_pools" configuration is set to True. Skipping Dedicated SQL Pool connectivity test')  
else:
  dedicated_sqlpools = workspace_instance.list_sql_pools()
  online_dedicated_sqlpool_names = [pool["name"] for poolPages in dedicated_sqlpools for pool in poolPages if pool["status"]== "Online"]
  if online_dedicated_sqlpool_names:
    first_pool_name = online_dedicated_sqlpool_names[0]
    sqlpool_reader = get_dedicated_sqlpool_reader(first_pool_name)
    sqlpool        = SynapseDedicatedSqlPool(first_pool_name, sqlpool_reader)
    sqlpool_databases = sqlpool.list_databases()
    display(sqlpool_databases)
    del sqlpool_reader, sqlpool, sqlpool_databases  
  else:
    print('INFO: No Online Dedicated SQL pools found!!! Skipping Dedicated SQL Pool connectivity test')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Run Profiler Steps
# MAGIC
# MAGIC >Following are the required steps
# MAGIC   1. Extract workspace level information
# MAGIC   2. Run extract for monitoring metrics
# MAGIC   3. Run extract for each Dedicated SQL Pool ( info + activity* )
# MAGIC   4. Run extract for Serverless SQL Pool
# MAGIC
# MAGIC - All of these steps can be run independently in parellelly
# MAGIC - Data extracts from above steps will be saved under subfolder of "data_staging_root" likes below
# MAGIC | SNO | Step Type                   | Target Folder |
# MAGIC | --- | --------------------------- | --------------------------- |
# MAGIC | 1   | Workspace level information | ${data_staging_root}/workspace |
# MAGIC | 2   | Monitoring Merics           | ${data_staging_root}/monitoring_metrics |
# MAGIC | 3   | Dedicated SQL Pool          | ${data_staging_root}/sqlpools-dedicated |
# MAGIC | 4   | Serverless SQL Pool         | ${data_staging_root}/sqlpools-serverless |
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Note :** 
# MAGIC   -  Current version of the profiler uses Synapse DMVs for activity data which has limitation of 10K record at any time. 
# MAGIC   -  Which can be filed in few hours of activity depending on your usage volumne.
# MAGIC   -  So after intitial maual run of the profiler main , it is requried to schedule `04_dedicated_sqlpool_activity_extract` notebook as hourly job for a mininum of 48 hours to extract decent data for workload insights.
# MAGIC   -  Notebook `04_dedicated_sqlpool_activity_extract` is light weight (in term of resource needs) and it can be run on a  single node cluster. 
# MAGIC   - It extracts the command session/requests data in incremental manner

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.1) Run Workspace extract

# COMMAND ----------

# DBTITLE 1,04_run_synapse_workspace_extract
dbutils.notebook.run("./04_synapse_workspace_extract", 0, {"workspaceName": wp_name, "developmentEndpoint": dev_ep, "dedicatedSQLEndpoint": dedicated_ep, "serverlessSQLEndpoint": serverless_ep,"serverlessOnly":exclude_dedicated_sql_pool})

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.2) Run Metrics Extract

# COMMAND ----------

if not synapse_profiler["exclude_monitoring_metrics"]:
  dbutils.notebook.run("./04_monitoring_metrics_extract", 0 , {"workspaceName": wp_name, "developmentEndpoint": dev_ep, "dedicatedSQLEndpoint": dedicated_ep, "serverlessSQLEndpoint": serverless_ep,"serverlessOnly":exclude_dedicated_sql_pool})
else:
  print('INFO: "exclude_monitoring_metrics" configuration is set to True. Skipping Monitoring Metrics extract ')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.3) Run Dedicated Sql Pool extract 

# COMMAND ----------

synapse_profiler_settings = get_synapse_profiler_settings()
exclude_dedicated_sql_pools = synapse_profiler_settings.get("exclude_dedicated_sql_pools", None)
dedicated_sql_pools_profiling_list = synapse_profiler_settings.get("dedicated_sql_pools_profiling_list", None)

print(f"INFO: exclude_dedicated_sql_pools        →  {exclude_dedicated_sql_pools}")
print(f"INFO: dedicated_sql_pools_profiling_list →  {dedicated_sql_pools_profiling_list}")

# COMMAND ----------

# DBTITLE 1,Get List of Dedicated pools to extract
if exclude_dedicated_sql_pools  == True:
  print(f"INFO: exclude_dedicated_sql_pools is set to {exclude_dedicated_sql_pools}, Skipping metrics extract for Dedicated SQL pools")
else:
  dedicated_sqlpools = workspace_instance.list_sql_pools() 
  all_dedicated_pools_list = [ pool for poolPages in dedicated_sqlpools for pool in poolPages]
  dedicated_pools_to_profile = all_dedicated_pools_list if not dedicated_sql_pools_profiling_list else [pool for pool in all_dedicated_pools_list if pool['name'] in dedicated_sql_pools_profiling_list]
  
  print(f"INFO: Pool names to extract metrics...")
  for idx, entry in enumerate(dedicated_pools_to_profile):
    print(f"\t {idx+1}  {entry['name'].ljust(50, '.')}{entry['status']}")

  live_dedicated_pools_to_profile = [ entry for entry in dedicated_pools_to_profile if entry['status'] == 'Online']
  print()
  print(f"INFO: live_dedicated_pools_to_profile: {[entry['name'] for entry in live_dedicated_pools_to_profile]}")

# COMMAND ----------

# DBTITLE 1,Run 04_dedicated_sqlpool_info_extract   (for each pool)
if exclude_dedicated_sql_pools  == True:
  print(f"INFO: exclude_dedicated_sql_pools is set to {exclude_dedicated_sql_pools}, Skipping metrics extract for Dedicated SQL pools")
else:
  for idx, entry in enumerate(live_dedicated_pools_to_profile):
    entry_info=f"{entry['name']} [{entry['status']}]"
    print(f"   {idx :02d})  {entry_info.ljust(60, '.')} : RUNNING extract...")
    dbutils.notebook.run("./04_dedicated_sqlpool_info_extract", 0, {"workspaceName": wp_name, "developmentEndpoint": dev_ep, "dedicatedSQLEndpoint": dedicated_ep, "serverlessSQLEndpoint": serverless_ep,"serverlessOnly":exclude_dedicated_sql_pool,"sqlpool_name": entry['name']} )


# COMMAND ----------

# DBTITLE 1,Run 04_dedicated_sqlpools_activity_extract   (with pool list as notebook parameter)
if exclude_dedicated_sql_pools  == True:
  print(f"INFO: exclude_dedicated_sql_pools is set to {exclude_dedicated_sql_pools}, Skipping metrics extract for Dedicated SQL pools")
else:
  sqlpool_names_to_profile = ",".join([entry['name'] for entry in live_dedicated_pools_to_profile])
  print(f"INFO: Running 04_dedicated_sqlpools_activity_extract with sqlpool_names  → [{sqlpool_names_to_profile}] ...")
  dbutils.notebook.run("./04_dedicated_sqlpools_activity_extract", 0,  {"workspaceName": wp_name, "developmentEndpoint": dev_ep, "dedicatedSQLEndpoint": dedicated_ep, "serverlessSQLEndpoint": serverless_ep,"serverlessOnly":exclude_dedicated_sql_pool,"sqlpool_names": sqlpool_names_to_profile} )
  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.4) Run Serverless Sql Pool extract 

# COMMAND ----------

# DBTITLE 1,04_serverless_sqlpool_info_extract
if not synapse_profiler["exclude_serverless_sql_pool"]:
  databses_profiled = dbutils.notebook.run("./04_serverless_sqlpool_info_extract", 0, {"workspaceName": wp_name, "developmentEndpoint": dev_ep, "dedicatedSQLEndpoint": dedicated_ep, "serverlessSQLEndpoint": serverless_ep,"serverlessOnly":exclude_dedicated_sql_pool})
  print(f'INFO: Successfully extracted information for databases {databses_profiled}' )
else:
  print('INFO: "exclude_serverless_sql_pool" configuration is set to True. Skipping Serverless Pool Info extract ')

# COMMAND ----------

# DBTITLE 1,04_serverless_sqlpool_activity_extract
if not synapse_profiler["exclude_serverless_sql_pool"]:
  dbutils.notebook.run("./04_serverless_sqlpool_activity_extract", 0 , {"workspaceName": wp_name, "developmentEndpoint": dev_ep, "dedicatedSQLEndpoint": dedicated_ep, "serverlessSQLEndpoint": serverless_ep,"serverlessOnly":exclude_dedicated_sql_pool})
else:
  print('INFO: "exclude_serverless_sql_pool" configuration is set to True. Skipping Serverless Pool Activity extract ')

# COMMAND ----------

# MAGIC %md
# MAGIC ### End