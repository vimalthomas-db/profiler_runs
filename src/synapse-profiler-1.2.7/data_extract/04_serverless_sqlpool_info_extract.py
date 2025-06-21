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
# MAGIC ### 01. Warehouse Objects

# COMMAND ----------

# DBTITLE 1,Data Paths
serverless_staging_location= get_staging_dbfs_path("sqlpools-serverless", True)
print(f"\nserverless_staging_location → {serverless_staging_location}")

# COMMAND ----------

# DBTITLE 1,Read Required Profiler Settings 
profiler_settings = get_synapse_profiler_settings()
redact_sql_text_flag = "redact_sql_pools_sql_text" in profiler_settings and profiler_settings["redact_sql_pools_sql_text"]

# COMMAND ----------

# DBTITLE 1,Create Reader and Writers
sqlpool_reader    = get_serverless_sqlpool_reader()
sqlpool           = SynapseServerlessSqlPool(sqlpool_reader)
dataframe_writer  = ProfilerDataframeWriter(serverless_staging_location, sqlpool.get_name())

# COMMAND ----------

# MAGIC %md
# MAGIC #### A) Databases

# COMMAND ----------

# DBTITLE 1,List Databases
import json

sqlpool_databases = sqlpool.list_databases()
obj_name = "databases"
dataframe_writer.write(sqlpool_databases, obj_name, "overwrite")
# group server less databases by their collation_name
# this way we can extract data using a set query for all dbs in each group
serverless_database_groups = {}
#serverless_db_list = [ entry.name for entry in sqlpool_databases.select("name", "collation_name").collect()]
for entry in sqlpool_databases.select("name", "collation_name").collect():
  if entry.collation_name in serverless_database_groups:
    serverless_database_groups[entry.collation_name].append(entry.name)
  else:
    serverless_database_groups[entry.collation_name] = [entry.name]

print("INFO: serverless_database_groups")
print("+"*80)
print(f"serverless_database_groups → {serverless_database_groups}")
print("+"*80)

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(serverless_staging_location, obj_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### → Set Serverless Databases in Scope

# COMMAND ----------

# DBTITLE 1,Serverless Databases in Scope
profiler_settings = get_synapse_profiler_settings()
serverless_sql_pool_databases_inclusion_list = profiler_settings.get('serverless_sql_pool_databases_inclusion_list', [])
serverless_sql_pool_databases_exclusion_list = profiler_settings.get('serverless_sql_pool_databases_exclusion_list', ["default"])

serverless_database_groups_in_scope = {}
for collation_name in serverless_database_groups:
  for db in serverless_database_groups[collation_name]: 
    if (not serverless_sql_pool_databases_inclusion_list or db in serverless_sql_pool_databases_inclusion_list) and \
      db not in serverless_sql_pool_databases_exclusion_list:
        if collation_name in serverless_database_groups_in_scope:
          serverless_database_groups_in_scope[collation_name].append(db)
        else:
          serverless_database_groups_in_scope[collation_name] = [db]

print("+"*80)
print(f"serverless_database_groups          → {serverless_database_groups}")
print(f"serverless_database_groups_in_scope → {serverless_database_groups_in_scope}")
print("+"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC #### D) Tables

# COMMAND ----------

for xx in serverless_database_groups_in_scope:
  print(xx)

# COMMAND ----------

# DBTITLE 1,List Tables
obj_name = "tables"
for idx, collation_name in enumerate(serverless_database_groups_in_scope):
  print(f"INFO: {idx+1}. Listing {obj_name} for database_groups → {serverless_database_groups_in_scope[collation_name]}")
  sqlpool_tables = sqlpool.list_tables(serverless_database_groups_in_scope[collation_name])
  dataframe_writer.write(sqlpool_tables, obj_name, "overwrite" if idx==0 else "append")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(serverless_staging_location, obj_name))
del obj_name, sqlpool_tables # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC #### C) Columns

# COMMAND ----------

# DBTITLE 1,List Columns
obj_name = "columns"
for idx, collation_name in enumerate(serverless_database_groups_in_scope):
  print(f"INFO: {idx+1}. Listing {obj_name} for database_groups → {serverless_database_groups_in_scope[collation_name]}")
  sqlpool_columns = sqlpool.list_columns(serverless_database_groups_in_scope[collation_name])
  dataframe_writer.write(sqlpool_columns, obj_name, "overwrite" if idx==0 else "append")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(serverless_staging_location, obj_name))
del obj_name, sqlpool_columns # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC #### D) Views

# COMMAND ----------

# DBTITLE 1,List Views
obj_name = "views"
for idx, collation_name in enumerate(serverless_database_groups_in_scope):
  print(f"INFO: {idx+1}. Listing {obj_name} for database_groups → {serverless_database_groups_in_scope[collation_name]}")
  sqlpool_views = sqlpool.list_views(serverless_database_groups_in_scope[collation_name], redact_sql_text_flag)
  dataframe_writer.write(sqlpool_views, obj_name, "overwrite" if idx==0 else "append")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(serverless_staging_location, obj_name))
del obj_name, sqlpool_views # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC #### E) Routines ( Procedures + Functions)

# COMMAND ----------

# DBTITLE 1,List Routines
obj_name = "routines"
for idx, collation_name in enumerate(serverless_database_groups_in_scope):
  print(f"INFO: {idx+1}. Listing {obj_name} for database_groups → {serverless_database_groups_in_scope[collation_name]}")
  sqlpool_routines = sqlpool.list_routines(serverless_database_groups_in_scope[collation_name], redact_sql_text_flag)
  dataframe_writer.write(sqlpool_routines, obj_name, "overwrite" if idx==0 else "append")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(serverless_staging_location, obj_name))
del obj_name, sqlpool_routines # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC #### F) Materlized Views

# COMMAND ----------

# MAGIC %md
# MAGIC ### 02. Data Processed

# COMMAND ----------

# DBTITLE 1,Query Data Processed
obj_name = "data_processed"
sqlpool_data_processed = sqlpool.query_data_processed()
dataframe_writer.write(sqlpool_data_processed, obj_name, "overwrite")

# COMMAND ----------

# DBTITLE 1,Check Output
check_target_folder_content(os.path.join(serverless_staging_location, obj_name))
del obj_name, sqlpool_data_processed # clearing them to avoid references in subsequent steps

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exit

# COMMAND ----------

# MAGIC %md
# MAGIC ##### → Return List profiled Databases 

# COMMAND ----------

serverless_databases_in_scope = [ db for db in serverless_database_groups_in_scope[collation_name] for collation_name in serverless_database_groups_in_scope]
dbutils.notebook.exit(serverless_databases_in_scope)