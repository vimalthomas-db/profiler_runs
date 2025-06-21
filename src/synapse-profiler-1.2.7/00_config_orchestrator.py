# Databricks notebook source
# Example: Set a list of config dicts for downstream use
configs = [
    {"workspaceName": "", "developmentEndpoint": "", "dedicatedSQLEndpoint": "","serverlessSQLEndpoint":"", "serverlessOnly": "False"},
    {"workspaceName": "", "developmentEndpoint": "", "dedicatedSQLEndpoint": "","serverlessSQLEndpoint":"", "serverlessOnly": "False"},
    {"workspaceName": "", "developmentEndpoint": "", "dedicatedSQLEndpoint": "","serverlessSQLEndpoint":"", "serverlessOnly": "True"}
]
dbutils.jobs.taskValues.set(key="config_list", value=configs)