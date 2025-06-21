# Databricks notebook source
# MAGIC %md
# MAGIC ### Important Note: 
# MAGIC Before running the profilers you need to adjust the following sections of settings.
# MAGIC
# MAGIC
# MAGIC > **01. Synapse Workspace Settings**:  (Required)
# MAGIC </br> →  Go through the `synapse_workspace` dictionary entry notes to set them to appropriate values
# MAGIC   
# MAGIC > **02. Synapse JDBC Settings**: (Optional)
# MAGIC <br> → Set synapse_jdbc to required authentication type. Default is sql_authentication. If required advanced options like `loginTimeout` and `fetch_size` can be adjusted 
# MAGIC
# MAGIC > **03. Synapse Profiler Settings**: (Required)
# MAGIC </br> →  Go through the `synapse_profiler` dictionary entry notes to set them to appropriate values

# COMMAND ----------

# DBTITLE 1,Legacy SQL DW Service
# Set this to True if you need to profiler formally SqlDW service
# which as only dedicated SQL endpoint with any Synapse Workspace components.

IS_LEGACY_SQLDW = False

# COMMAND ----------

# MAGIC %md
# MAGIC ### 01. Synapse Workspace Settings

# COMMAND ----------

# DBTITLE 1,synapse_workspace
import zoneinfo
# synapse_workspace
synapse_workspace = {
  # Note: Get below values from your Synapse workspace overview page in Azure Portal
  "name": wp_name,
  "dedicated_sql_endpoint": dedicated_ep,
  "serverless_sql_endpoint": serverless_ep,
  
  # Note: Update below section to set SQL user id and password
  #       Below sample code is using "synapse-profiler" as secret Scope name
  #       and "dev-sql-user" and "dev-sql-password" keys for user and password respectively
  "sql-user": dbutils.secrets.get(scope = "hls-tokens", key="dev-azure-client-id"),
  "sql-password": dbutils.secrets.get(scope = "hls-tokens", key="dev-azure-client-secret"),
  # Note: Set this to appropriate timezone. Defult is UTC
  #       If Required Run next cell manualy to get avilable time zone info keys
  "tz_info": zoneinfo.ZoneInfo("America/New_York"),
  # azure_api_access  
  # ** You can ignore this section if you set IS_LEGACY_SQLDW to True **
  "azure_api_access": {
    # Note: You can get synapse workspace development endpoint from 
    # your Synapse workspace overview page in Azure Portal
    "development_endpoint": dev_ep,
    # Note: Update below section to set Azure API access keys values 
    #       Below sample code is using "synapse-profiler" as secret Scope name
    #       and "dev-azure-client-id", "dev-azure-tenant-id" and "dev-azure-client-secret" 
    #       keys for azure-client-id, azure-tenant-id and azure-client-secret respectively
    "azure-client-id": dbutils.secrets.get(scope = "hls-tokens", key="dev-azure-client-id"),
    "azure-tenant-id": dbutils.secrets.get(scope = "hls-tokens", key="dev-azure-tenant-id"),
    "azure-client-secret": dbutils.secrets.get(scope = "hls-tokens", key="dev-azure-client-secret"),
    # End
  } if not IS_LEGACY_SQLDW else { },  
  # End of configuration entries
}
# Adjust api access keys (internal.. don't change)
for entry in synapse_workspace["azure_api_access"]:
  synapse_workspace[entry] = synapse_workspace["azure_api_access"][entry]

# COMMAND ----------

# DBTITLE 1,Info: TimeZone keys
#from datetime import  datetime

# print_timezone_keys
def print_timezone_keys():  
  time_now = datetime.now()
  zoneinfo_list = [(time_now.replace(tzinfo=zoneinfo.ZoneInfo(entry)).strftime('%z'), entry) for entry in zoneinfo.available_timezones()]
  for sno, entry in enumerate(sorted(zoneinfo_list), 1):
    print(f"{str(sno).zfill(3)}\t{entry[0]}\t{entry[1]}")

#print_timezone_keys()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 02. Synapse JDBC Settings
# MAGIC Set synapse_jdbc to required authentication type. Default is sql_authentication

# COMMAND ----------

# DBTITLE 1,synapse_jdbc
synapse_jdbc_sql_authentication = {
  "dedicated_sqlpool_url_template": "jdbc:sqlserver://{endpoint}:1433;database={database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;",
  "serverless_sqlpool_url_template": "jdbc:sqlserver://{endpoint}:1433;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;",
  "fetch_size": "1000",

}
synapse_jdbc_ad_passwd_authentication = {
  "dedicated_sqlpool_url_template": "jdbc:sqlserver://{endpoint}:1433;database={database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;authentication=ActiveDirectoryPassword",
  "serverless_sqlpool_url_template": "jdbc:sqlserver://{endpoint}:1433;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;authentication=ActiveDirectoryPassword",
  "fetch_size": "1000",

}
# AMirskiy on Oct 13 PR#9 manual merge - start
synapse_jdbc_spn_authentication = {
  "dedicated_sqlpool_url_template": "jdbc:sqlserver://{endpoint}:1433;database={database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;authentication=ActiveDirectoryServicePrincipal",
  "serverless_sqlpool_url_template": "jdbc:sqlserver://{endpoint}:1433;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;authentication=ActiveDirectoryServicePrincipal",
  "fetch_size": "1000",
}
# AMirskiy on Oct 13 PR#9 manual merge - end
#######################################################
# Set this required authentication type
#######################################################
synapse_jdbc = synapse_jdbc_spn_authentication

# COMMAND ----------

# MAGIC %md
# MAGIC ### 03. Synapse Profiler Settings

# COMMAND ----------


if exclude_dedicated_sql_pool == "True":
  excludeBoolean = True
else:
  excludeBoolean = False



# COMMAND ----------

# DBTITLE 1,synapse_profiler


synapse_profiler = {
  #-----------------------------------------------------------------------------
  # > Set this to an dbfs location poiting to an external storage
  # > Use the mounted object/blob storage folder so that the 
  #   profiler created data files can be be downloaded from object/blob storage
  # > Profiler uses this to save the extracted data
  #-----------------------------------------------------------------------------
  "data_staging_root": "dbfs:/FileStore/synapse-profiler-testing",
  #-----------------------------------------------------------------------------
  # > Set exclude_serverless_sql_pool to True to exclude Serverless SQL pool
  #   from profiling. Default is to include it
  #-----------------------------------------------------------------------------
  "exclude_serverless_sql_pool": False,
  #-----------------------------------------------------------------------------
  # > Use below inclusion and exclusion lists to select the required
  #   serverless sql pool databases. Make sure use all lowercase database names
  #-----------------------------------------------------------------------------
  "serverless_sql_pool_databases_inclusion_list": [],
  "serverless_sql_pool_databases_exclusion_list": ['default'],
  #-----------------------------------------------------------------------------
  # > Set exclude_dedicated_sql_pools to True to exclude Dedicated SQL pools
  #   from profiling. Default is to include it
  #-----------------------------------------------------------------------------
    "exclude_dedicated_sql_pools": excludeBoolean,
  #-----------------------------------------------------------------------------
  # > Use dedicated_sql_pools_profiling_list list to limit the profiling to 
  #   required dedicated sql pools. 
  #   Leaveit empty to profiler all avilable dedicated sql pools in the workspace
  #-----------------------------------------------------------------------------  
  "dedicated_sql_pools_profiling_list": [],
  #-----------------------------------------------------------------------------
  # > Set exclude_spark_pools to True to exclude Spark pools
  #   from profiling. Default is to include it
  #-----------------------------------------------------------------------------
  "exclude_spark_pools": excludeBoolean,  
  #-----------------------------------------------------------------------------
  # > Use spark_pools_profiling_list list to limit the profiling to 
  #   required spark pools. 
  #   Leaveit empty to profiler all avilable dedicated spark pools in the workspace
  #-----------------------------------------------------------------------------  
  "spark_pools_profiling_list": [],
  #-----------------------------------------------------------------------------
  # > Set exclude_monitoring_metrics to True to exclude metrics extraction
  #   from profiling. Default is to include it
  #-----------------------------------------------------------------------------
  "exclude_monitoring_metrics": False,
  #-----------------------------------------------------------------------------
  # > Set redact_sql_pools_sql_text to True to redact sql text output
  #-----------------------------------------------------------------------------
  "redact_sql_pools_sql_text": False,  
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 04. Data Staging Root

# COMMAND ----------

# DBTITLE 1,Create "data_staging_root"  if required
dbfs_path_to_create = synapse_profiler["data_staging_root"]
print(f"INFO:  data_staging_root →  {dbfs_path_to_create}")
dbutils.fs.mkdirs(dbfs_path_to_create)

# COMMAND ----------

# DBTITLE 1,Check  [data_staging_root]  dbfs path
dbfs_path_to_check = synapse_profiler["data_staging_root"]
print(f"INFO: Checking dbfs path → {dbfs_path_to_check}")
dbutils.fs.ls(dbfs_path_to_check)
del dbfs_path_to_check

# COMMAND ----------

# MAGIC %run ./common/02_profiler_functions

# COMMAND ----------

# DBTITLE 1,Check  [data_staging_root]  linux path
import os, re
linux_path_to_check = dbfs_to_linux_path(synapse_profiler["data_staging_root"]);
print(f"INFO: Checking linux path → {linux_path_to_check}")
print(os.listdir(linux_path_to_check))
del linux_path_to_check