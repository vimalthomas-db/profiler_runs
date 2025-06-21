# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Initialization

# COMMAND ----------

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

# DBTITLE 1,Azure SDK setup
# MAGIC %run ../common/01_azure_sdk_setup

# COMMAND ----------

# DBTITLE 1,Profiler functions
# MAGIC %run ../common/02_profiler_functions

# COMMAND ----------

# DBTITLE 1,Profiler classes
# MAGIC %run ../common/03_profiler_classes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1) Workspace Object

# COMMAND ----------

# DBTITLE 1,Synapse Workspace
synapse_workspace_settings = get_synapse_workspace_settings()
synapse_profiler_settings  = get_synapse_profiler_settings()
workspace_tz = synapse_workspace_settings["tz_info"]

workspace_name = synapse_workspace_settings["name"]
print(f"INFO: workspace_name → {workspace_name}")
#-----------------------------------------------------
# create workspace instance to run sdk query
#-----------------------------------------------------
artifacts_client = get_synapse_artifacts_client()
workspace_instance = SynapseWorkspace(workspace_tz, artifacts_client)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1) Synapse Metrics Object

# COMMAND ----------

metrics_client = get_azure_metrics_query_client()
synapse_metrics = SynapseMetrics(metrics_client)
extract_group_name = "monitoring_metrics"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Workspace Level Metrics
# MAGIC > Extract Workspace Level Metrics ( 1hours granularity)

# COMMAND ----------

# DBTITLE 1,Workspace Level Metrics
workspace_info = workspace_instance.get_workspace_info()

if not "id" in workspace_info:
  raise ValueError("ERROR: Missing Workspace Reporce ID for extracting Workspace Level Metrics")
workspace_resource_id = workspace_info["id"]
step_name = "workspace_level_metrics"
print(f"INFO: workspace_resource_id  →  {workspace_resource_id}")
print(f"INFO: step_name  →  {step_name}")
metrics_staging_uxpath = get_staging_linux_path(extract_group_name, True)
step_extractor = ProfilerStepDataExtractor(step_name, metrics_staging_uxpath)
step_extractor.extract(lambda: synapse_metrics.get_workspace_level_metrics(workspace_resource_id),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Extract
import os
step_name = "workspace_level_metrics"
check_target_folder_content(os.path.join(get_staging_dbfs_path(extract_group_name), step_name))
del step_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Dedicated  SQL Pool  Metrics
# MAGIC > Extract Metrics for Dedicated Pools 

# COMMAND ----------

exclude_dedicated_sql_pools = synapse_profiler_settings.get("exclude_dedicated_sql_pools", None)
dedicated_sql_pools_profiling_list = synapse_profiler_settings.get("dedicated_sql_pools_profiling_list", None)

print(f"INFO: exclude_dedicated_sql_pools        →  {exclude_dedicated_sql_pools}")
print(f"INFO: dedicated_sql_pools_profiling_list →  {dedicated_sql_pools_profiling_list}")

# COMMAND ----------

# DBTITLE 1,Get List of Pools to extract Metrics
if exclude_dedicated_sql_pools  == True:
  print(f"INFO: exclude_dedicated_sql_pools is set to {exclude_dedicated_sql_pools}, Skipping metrics extract for Dedicated SQL pools")
else:
  dedicated_sqlpools = workspace_instance.list_sql_pools() 
  all_dedicated_pools_list = [ pool for poolPages in dedicated_sqlpools for pool in poolPages]
  dedicated_pools_to_profile = all_dedicated_pools_list if not dedicated_sql_pools_profiling_list else [pool for pool in all_dedicated_pools_list if pool['name'] in dedicated_sql_pools_profiling_list]

  print(f"INFO: Pool names to extract metrics: {[entry['name'] for entry in dedicated_pools_to_profile]}")
  

# COMMAND ----------

# DBTITLE 1,Run Metrics Extract for Each Pool
if exclude_dedicated_sql_pools  == True:
  print(f"INFO: exclude_dedicated_sql_pools is set to {exclude_dedicated_sql_pools}, Skipping metrics extract for Dedicated SQL pools")
else:
  for idx, pool in enumerate(dedicated_pools_to_profile):
    pool_name = pool['name']
    pool_resoure_id = pool['id']

    print(f"{'*'*70}")
    print(f"{idx+1}) Pool Name: {pool_name}")
    print(f"   Resource id: {pool_resoure_id}")

    step_name = "dedicated_sql_pool_metrics"
    metrics_staging_uxpath = get_staging_linux_path(extract_group_name, True)

    step_extractor = ProfilerStepPartitionExtractor(step_name, metrics_staging_uxpath, "pool_name", pool_name)
    step_extractor.extract(lambda: synapse_metrics.get_dedicated_sql_pool_metrics(pool_resoure_id),  workspace_name)
  print(">End")  

# COMMAND ----------

# DBTITLE 1,Check Extract
import os
if exclude_dedicated_sql_pools  == True:
  print(f"INFO: exclude_dedicated_sql_pools is set to {exclude_dedicated_sql_pools}, Skipping metrics extract for Dedicated SQL pools")
else:
  step_name = "dedicated_sql_pool_metrics"
  check_target_folder_content(os.path.join(get_staging_dbfs_path(extract_group_name), step_name))
  del step_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Spark Pool  Metrics
# MAGIC > Extract Metrics for Spark Pools

# COMMAND ----------

exclude_spark_pools = synapse_profiler_settings.get("exclude_spark_pools", None)
spark_pools_profiling_list = synapse_profiler_settings.get("spark_pools_profiling_list", None)

print(f"INFO: exclude_spark_pools        →  {exclude_spark_pools}")
print(f"INFO: spark_pools_profiling_list →  {spark_pools_profiling_list}")

# COMMAND ----------

# DBTITLE 1,Get List of Pools to extract Metrics
if exclude_spark_pools  == True:
  print(f"INFO: exclude_spark_pools is set to {exclude_spark_pools}, Skipping metrics extract for Spark pools")
else:
  spark_pools_iter = workspace_instance.list_bigdata_pools() 
  all_spark_pool_list = [ pool for poolPages in spark_pools_iter for pool in poolPages]
  spark_pools_to_profile = all_spark_pool_list if not spark_pools_profiling_list else [pool for pool in all_spark_pool_list if pool['name'] in spark_pools_profiling_list]

  print(f"INFO: Pool names to extract metrics: {[entry['name'] for entry in spark_pools_to_profile]}")

# COMMAND ----------

# DBTITLE 1,Run Metrics Extract for Each Pool
if exclude_spark_pools  == True:
  print(f"INFO: exclude_spark_pools is set to {exclude_spark_pools}, Skipping metrics extract for Spark pools")
else:
  for idx, pool in enumerate(spark_pools_to_profile):
    pool_name = pool['name']
    pool_resoure_id = pool['id']

    print(f"{'*'*70}")
    print(f"{idx+1}) Pool Name: {pool_name}")
    print(f"   Resource id: {pool_resoure_id}")

    step_name = "spark_pool_metrics"
    metrics_staging_uxpath = get_staging_linux_path(extract_group_name, True)

    step_extractor = ProfilerStepPartitionExtractor(step_name, metrics_staging_uxpath, "pool_name", pool_name)
    step_extractor.extract(lambda: synapse_metrics.get_spark_pool_metrics(pool_resoure_id),  workspace_name)
  print(">End")  

# COMMAND ----------

# DBTITLE 1,Check Extract
import os
if exclude_spark_pools  == True:
  print(f"INFO: exclude_spark_pools is set to {exclude_spark_pools}, Skipping metrics extract for Spark pools")
else:
  step_name = "spark_pool_metrics"
  check_target_folder_content(os.path.join(get_staging_dbfs_path(extract_group_name), step_name))
  del step_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### End