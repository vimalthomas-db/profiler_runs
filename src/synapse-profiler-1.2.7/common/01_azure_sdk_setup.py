# Databricks notebook source
# MAGIC %md
# MAGIC #### Install required Azure SDK libraries

# COMMAND ----------

# MAGIC %md
# MAGIC Using only the artifacts and monitoring at this time. We may need others later as add more datapoints to extract
# MAGIC
# MAGIC - [azure-synapse-artifacts](https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/?view=azure-python-preview)
# MAGIC - [azure-synapse-spark](https://learn.microsoft.com/en-us/python/api/azure-synapse-spark/?view=azure-python-preview)
# MAGIC - [azure-synapse-monitoring](https://learn.microsoft.com/en-us/python/api/azure-synapse-monitoring/?view=azure-python-preview)

# COMMAND ----------

import sys
print(sys.version)

# COMMAND ----------

# MAGIC %pip install azure-synapse-artifacts azure-identity azure-monitor-query==1.3.0b1

# COMMAND ----------

# MAGIC %md
# MAGIC #### SDK Imports

# COMMAND ----------

# DBTITLE 1,SDK Imports
from azure.identity import DefaultAzureCredential
from azure.synapse.artifacts import ArtifactsClient
from azure.synapse.artifacts import models as ArtifactsModels
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from azure.monitor.query import MetricsQueryClient, MetricAggregationType

# COMMAND ----------

# MAGIC %run ../00_settings

# COMMAND ----------

# MAGIC %md
# MAGIC #### Azure SDK access setup

# COMMAND ----------

# DBTITLE 1,Azure SDK Access setup
import os
os.environ["AZURE_CLIENT_ID"] = synapse_workspace["azure-client-id"];
os.environ["AZURE_TENANT_ID"] = synapse_workspace["azure-tenant-id"];
os.environ["AZURE_CLIENT_SECRET"] = synapse_workspace["azure-client-secret"];