# Databricks notebook source
# MAGIC %md
# MAGIC ### 01. Intializations

# COMMAND ----------

# MAGIC %md
# MAGIC #### A) setttings

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
# MAGIC %run ../common/01_azure_sdk_setup

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
# MAGIC ### 02. Workspace Information

# COMMAND ----------

# DBTITLE 1,Create Synapse Workspace Object
import json, os

synapse_workspace_settings = get_synapse_workspace_settings()
synapse_profiler_settings = get_synapse_profiler_settings()

workspace_name = synapse_workspace_settings["name"]
workspace_tz = synapse_workspace_settings["tz_info"]

#workspace_staging_uxpath
workspace_staging_location = get_staging_dbfs_path("workspace", True)
workspace_staging_uxpath = get_staging_linux_path("workspace", False)

print(f"workspace_name              : {workspace_name}")
print(f"workspace_tz                : {workspace_tz}")
print(f"workspace_staging_location  : {workspace_staging_location}")
print(f"workspace_staging_uxpath    : {workspace_staging_uxpath}")

#-----------------------------------------------------
# create workspace instance to run sdk queries
#-----------------------------------------------------
artifacts_client = get_synapse_artifacts_client()
workspace = SynapseWorkspace(workspace_tz, artifacts_client)

# COMMAND ----------

# MAGIC %md
# MAGIC #### A) Workspace Overview

# COMMAND ----------

# DBTITLE 1,workspace_info
step_name = "workspace_info"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: [[workspace.get_workspace_info()]],  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### B) SQL Pools

# COMMAND ----------

# DBTITLE 1,sql_pools
step_name = "sql_pools"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_sql_pools(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### C) Spark Pools

# COMMAND ----------

# DBTITLE 1,spark_pools
step_name = "spark_pools"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_bigdata_pools(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### D) Linked Services

# COMMAND ----------

# DBTITLE 1,linked_services
step_name = "linked_services"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_linked_services(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 03. Workload Definitions

# COMMAND ----------

# MAGIC %md
# MAGIC #### A) Data Flows

# COMMAND ----------

# DBTITLE 1,dataflows
step_name = "dataflows"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_data_flows(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### B) Pipelines

# COMMAND ----------

# DBTITLE 1,pipelines
step_name = "pipelines"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_pipelines(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### C) Spark Jobs

# COMMAND ----------

# DBTITLE 1,spark_jobs
step_name = "spark_jobs"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_spark_job_definitions(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### D) Notebooks

# COMMAND ----------

# DBTITLE 1,Notebooks
step_name = "notebooks"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_notebooks(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### E) Sql Scripts

# COMMAND ----------

# DBTITLE 1,SQL Scripts
step_name = "sql_scripts"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_sqlscripts(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### F) Triggers

# COMMAND ----------

# DBTITLE 1,Triggers
step_name = "triggers"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_triggers(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### G) Libraries

# COMMAND ----------

# DBTITLE 1,Libraries
step_name = "libraries"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_libraries(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### H) Datasets

# COMMAND ----------

step_name = "datasets"
step_extractor = ProfilerStepDataExtractor(step_name, workspace_staging_uxpath)
step_extractor.extract(lambda: workspace.list_datasets(),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 05. Runtime Information

# COMMAND ----------

# MAGIC %md
# MAGIC #### A) Pipeline Runs

# COMMAND ----------

# DBTITLE 1,pipeline_runs
from datetime import timezone, timedelta, date

step_name = "pipeline_runs"
today = date.today()
for days in range(1, 60):
  last_upd = today + timedelta(days=-days)
  print(f"Running extract for {last_upd}..")
  step_extractor = ProfilerStepPartitionExtractor(step_name, workspace_staging_uxpath, "last_upd", last_upd)
  step_extractor.extract(lambda: workspace.list_pipeline_runs(last_upd),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### B) Trigger Runs

# COMMAND ----------

# DBTITLE 1,trigger_runs
from datetime import timezone, timedelta, date

step_name = "trigger_runs"
today = date.today()
for days in range(1, 60):
  last_upd = today + timedelta(days=-days)
  print(f"Running extract for {last_upd}..")
  step_extractor = ProfilerStepPartitionExtractor(step_name, workspace_staging_uxpath, "last_upd", last_upd)
  step_extractor.extract(lambda: workspace.list_trigger_runs(last_upd),  workspace_name)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(workspace_staging_location, step_name))