# Databricks notebook source
# MAGIC %md
# MAGIC ##### > SQL Pool Classes

# COMMAND ----------

# MAGIC %md
# MAGIC ###### SynapseSqlPool

# COMMAND ----------

# DBTITLE 1,SynapseSqlPool
class SynapseSqlPool:
  '''
    constructor

    :artifacts_client - create one using *get_synapse_artifacts_client* utility function
  '''
  def __init__(self, pool_name, db_reader):
    self.pool_name = pool_name
    self.db_reader = db_reader
  
  # get_name
  def get_name(self):
    return self.pool_name;
  
  # run_query
  def run_query (self, query):
    '''
      runs the query and returns df
    '''
    # print the query
    print(f"{'-'*20}[ SynapseSqlPool::run_query ]{'-'*20}")
    print(query)
    print(f"{'-'*69}")

    return (
      self.db_reader
      .option("dbtable", query)
      .load()
    )
    
  # list_databases
  def list_databases(self):
    '''
      get the list of databases
    '''
    return self.run_query("""
      (
        select name, database_id, create_date,  state, state_desc, collation_name 
        from sys.databases where name <> 'master'
      ) as databases
    """)
  # xfr_session_login_name
  def xfr_session_login_name(self, sessions_df):
    '''
      Takes input session_df with login_name column to.. 
        - derive login_user_type, login_user and login_user_sha2 columns
        - remove login_name column
    '''
    return (
      sessions_df
      .withColumn(
        "login_user_type",
        when(
          nullif(trim(regexp_extract(lower('login_name'), '^(\\w+.\\w+.*)@(\\w+).*(net|com|edu|org|gov)', 1 )), lit('')).isNull(),
          "APP"
        ).otherwise("USER")
      )
      .withColumn(
        "login_user",
        coalesce(
          nullif(trim(regexp_replace(trim(regexp_extract("login_name", '^(\\w+)(.\\w+.*)@(\\w+).*', 1 )), '[.].*[.]', '++')), lit('')), 
          when(col('login_user_type') == lit('USER'), lit('otherUser')).otherwise('otherApp')
        )
      )
      .withColumn(
        "login_user_sha2",
        sha2('login_user', 256)
      )
      .drop("login_name")
    )
  # xfr_session_client_id
  def xfr_session_client_id(self, sessions_df):
    '''
      Takes input session_df with client_id column to.. 
        - derive client_id_sha2
        - remove client_id column
    '''
    return (
      sessions_df
      .withColumn(
        "client_id_sha2",
        sha2('client_id', 256)
      )
      .drop("client_id")
    )    

# COMMAND ----------

# MAGIC %md
# MAGIC ###### SynapseDedicatedSqlPool

# COMMAND ----------

# DBTITLE 1,SynapseDedicatedSqlPool
from pyspark.sql.functions import col, lit, regexp_extract, regexp_replace, trim, upper,lower, length, nullif, coalesce, when, sha2
# SynapseDedicatedSqlPool
class SynapseDedicatedSqlPool(SynapseSqlPool):

  def __init__(self, pool_name, db_reader):
    super().__init__(pool_name, db_reader)
  # list_tables
  def list_tables(self):
    '''
      get the list of tables
    '''
    return self.run_query("information_schema.tables")

  # list_columns
  def list_columns(self):
    '''
      get the list of tables
    '''
    return self.run_query("information_schema.columns")

  # list_views
  def list_views(self, redact_sql_text = False):
    '''
      get the list of tables
    '''
    views_df = self.run_query("information_schema.views");
    return views_df.withColumn("view_definition", lit("[REDACTED]")) if redact_sql_text else views_df
  
  # list_routines
  def list_routines(self, redact_sql_text = False):
    '''
      get the list of routines (functions + procedures)
    '''
    routines_df = self.run_query("information_schema.routines");
    return routines_df.withColumn("routine_definition", lit("[REDACTED]")) if redact_sql_text else routines_df

  # list_sessions
  def list_sessions(self, last_login_time = None):
    '''
      get the session list
    '''
    # sessions_df
    sessions_df = self.run_query(f"""(
      select *, CURRENT_TIMESTAMP as extract_ts
        from (
          SELECT * 
          FROM sys.dm_pdw_exec_sessions 
          where CHARINDEX('system', LOWER(login_name)) = 0
          {"AND login_time > '"+last_login_time+"'" if last_login_time else ""}
        ) x
      ) as sessions
    """);
    # return 
    return self.xfr_session_client_id(self.xfr_session_login_name(sessions_df))

  # list_requests
  def list_requests(self, redact_sql_text = False, min_end_time = None):
    '''
      get the session request list
    '''
    # requests_df
    requests_df = self.run_query(f"""(      
      select *, CURRENT_TIMESTAMP as extract_ts
        from (
          SELECT * FROM sys.dm_pdw_exec_requests
          WHERE 
            start_time IS NOT NULL
            AND command IS NOT NULL
           {"AND end_time > '"+min_end_time+"'" if min_end_time else ""}
        ) x
      ) as requests
    """);

    # requests_ext_df
    requests_ext_df = (
      requests_df
      .withColumn("command", trim_sql_comments_udf('command'))
      .withColumn("command_w1", upper(regexp_extract("command", '(\\w+).*', 1)))
      .withColumn("command_w2", upper(regexp_extract("command", '(\\w+)\\s+([^\\s,\.]+).*', 2)))
      .selectExpr(
        "*",
        """
          CASE 
            WHEN command_w1 = 'SELECT' THEN 'QUERY'
            WHEN command_w1 = 'WITH' THEN 'QUERY'
            WHEN command_w1 in ( 'INSERT', 'UPDATE', 'MERGE', 'DELETE', 'TRUNCATE', 'COPY', 'IF', 'BEGIN', 'DECLARE', 'BUILDREPLICATEDTABLECACHE') THEN 'DML'
            WHEN command_w1 in ( 'CREATE', 'DROP', 'ALTER') THEN 'DDL'
            WHEN command_w1 in ( 'EXEC', 'EXECUTE') THEN 'ROUTINE'
            WHEN command_w1 = 'BEGIN' and command_w2 in ( 'TRAN', 'TRANSACTION') THEN 'TRANSACTION_CONTROL'
            WHEN command_w1 = 'END' and command_w2 in ( 'TRAN', 'TRANSACTION') THEN 'TRANSACTION_CONTROL'
            WHEN command_w1 = 'COMMIT' THEN 'TRANSACTION_CONTROL'
            WHEN command_w1 = 'ROLLBACK' THEN 'TRANSACTION_CONTROL'
            ELSE 'OTHER'
          END as command_type   
        """    
      )
    )
    # return
    return requests_ext_df.withColumn("command", lit("[REDACTED]")) if redact_sql_text else requests_ext_df


  # list_query_stats
  def list_query_stats(self):
    '''
      get the query stats 
      Note: This query returns aggregate performance statistics for cached query plans
            May not be very used in the context of migration assessment
            So not using it for now
    '''
    return self.run_query("""(
      select *, CURRENT_TIMESTAMP as extract_ts
        from (
          SELECT * FROM sys.dm_pdw_nodes_exec_query_stats
        ) x
      ) as stats
    """);
  
  # get_wh_storage_info
  def get_db_storage_info(self):
    """
      Get the reserved and used storage information based on db partitions
    """
    return self.run_query("""(
      select *, CURRENT_TIMESTAMP as extract_ts
      from (
          select
          pdw_node_id as node_id,
          (sum(reserved_page_count) * 8) / 1024 as ReservedSpaceMB, 
          (sum(used_page_count)  * 8) / 1024 as UsedSpaceMB
          from sys.dm_pdw_nodes_db_partition_stats 
          group by pdw_node_id
      ) x
    ) as storage_info
    """);
  
  # get_all_db_service_tiers
  @staticmethod
  def get_db_service_tiers(master_db_reader):
    spool = SynapseDedicatedSqlPool("master", master_db_reader)
    return spool.run_query("""(
      select *, CURRENT_TIMESTAMP as extract_ts
      from (
          SELECT  db.name [Database]
          ,        ds.edition [Edition]
          ,        ds.service_objective [Service Objective]
          FROM    sys.database_service_objectives   AS ds
          JOIN    sys.databases                     AS db 
          ON ds.database_id = db.database_id
      ) x
    ) as service_tiers
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### SynapseServerlessSqlPool

# COMMAND ----------

# DBTITLE 1,SynapseServerlessSqlPool
class SynapseServerlessSqlPool(SynapseSqlPool):
  """
    Utility class which wraps the Serverless Pool Profiler extract queries.
    Note the following difference between Serverless and Dedicated SQL pools in the context of data extract
      - Each dedicated SQL pool has one Database (you cannot run queries across DBs)
      - Where as Serverless SQL pool queries can read multiple databases (catalogs)

      So for Serverless extract we will get data for all databases in one query using unions
  """
  def __init__(self, db_reader):
    super().__init__("built-in", db_reader)

  # get_set_query
  def get_set_query(self, query_tmpl, databases):
    """
      for each database create select using query template and join them using union all
    """
    db_selects = [ query_tmpl.format(db_name) for db_name in databases ]
    set_query = "(\n{}\n) as set_query".format("\nunion all\n".join(db_selects))
    return set_query

  # list_tables
  def list_tables(self, databases):
    '''
      get the list of tables
    '''
    set_query = self.get_set_query("select * from {}.information_schema.tables", databases)
    return self.run_query(set_query)

  # list_columns
  def list_columns(self, databases):
    '''
      get the list of tables
    '''
    set_query = self.get_set_query("select * from {}.information_schema.columns", databases)
    return self.run_query(set_query)
  
  # list_views
  def list_views(self, databases, redact_sql_text = False):
    '''
      get the list of tables
    '''
    set_query = self.get_set_query("select * from {}.information_schema.views", databases)
    views_df = self.run_query(set_query)
    return views_df.withColumn("view_definition", lit("[REDACTED]")) if redact_sql_text else views_df
  
  # list_routines
  def list_routines(self, databases, redact_sql_text = False):
    '''
      get the list of routines (functions + procedures)
    '''
    set_query = self.get_set_query("select * from {}.information_schema.routines", databases)
    routines_df = self.run_query(set_query)
    return routines_df.withColumn("routine_definition", lit("[REDACTED]")) if redact_sql_text else routines_df
  
  # list_sessions
  def list_sessions(self, min_last_request_start_time):
    '''
      get the session list
    '''
    # sessions_df
    sessions_df =  self.run_query(f"""(
      SELECT * FROM sys.dm_exec_sessions where is_user_process = 'True'
      {"AND last_request_start_time > '"+min_last_request_start_time+"'" if min_last_request_start_time else ""}
      ) as sessions
    """);
    # return 
    return self.xfr_session_login_name(sessions_df)


  # list_requests
  def list_requests(self, min_start_time):
    '''
      get the session request list
    '''
    return self.run_query(f"""(
      SELECT * FROM sys.dm_exec_requests
      {"WHERE start_time > '"+min_start_time+"'" if min_start_time else ""}      
      ) as requests
    """);

  # list_query_stats
  def list_query_stats(self, min_last_execution_time):
    '''
      get the query stats
      source for below query:
        https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-query-stats-transact-sql?view=sql-server-ver16#b-returning-row-count-aggregates-for-a-query
    '''
    return self.run_query(f"""(
      SELECT QS.*,
        SUBSTRING(
          ST.text,
          (QS.statement_start_offset / 2) + 1,
          (
            (
              CASE
                statement_end_offset
                WHEN -1 THEN DATALENGTH(ST.text)
                ELSE QS.statement_end_offset
              END - QS.statement_start_offset
            ) / 2
          ) + 1
        ) AS statement_text
      FROM sys.dm_exec_query_stats AS QS
        CROSS APPLY sys.dm_exec_sql_text(QS.sql_handle) as ST
      {"WHERE QS.last_execution_time > '"+min_last_execution_time+"'" if min_last_execution_time else ""}
    ) as query_stats
    """);
  
  # query_requests_history
  def query_requests_history(self, min_end_time):
    '''
      get the query requet history 
    '''
    return self.run_query(f"""(
      SELECT * FROM sys.dm_exec_requests_history
      {"WHERE end_time > '"+min_end_time+"'" if min_end_time else ""}
      ) as requests_history
    """);

  # query_data_processed
  def query_data_processed(self):
    '''
      get the query requet history 
    '''
    return self.run_query("""(
      SELECT * FROM sys.dm_external_data_processed
      ) as data_processes
    """);

# COMMAND ----------

# MAGIC %md
# MAGIC ##### > Synapse Workspace Classes

# COMMAND ----------

# MAGIC %md
# MAGIC ###### AzureArtifact
# MAGIC

# COMMAND ----------

from abc import ABC

# AzureArtifact
class AzureArtifact(ABC):
  # constructor
  def __init__(self, tz_info, artifacts_client, fetch_batch_size=20, max_pages=5000):
    self.tz_info = tz_info
    self.client = artifacts_client
    self.fetch_batch_size = fetch_batch_size
    self.max_pages = max_pages

  @staticmethod
  def project_dict(obj, keep=[], remove=[]):
    '''
      Utility Function to keep or/and remove fields
    '''
    return {k:v for (k, v) in obj.items() if (not keep or k.lower() in keep) and (k.lower() not in remove)}
  @staticmethod
  def create_run_filter_parameters(last_updated_after, last_updated_before):
    # @todo: fix this from global context to import 
    return ArtifactsModels.RunFilterParameters(
      last_updated_after = last_updated_after, 
      last_updated_before = last_updated_before
    )

  # fetch_from_iter
  def fetch_from_iter(self, iterator, keep, remove):
    '''
      Creates item groups ( lists of max size fetch_batch_size) from input iterator
    '''
    group = []
    for entry in iterator:
      # add item to group
      group.append(AzureArtifact.project_dict(entry.as_dict(), keep, remove))
      # yield the group list for every batch size
      if len(group) >= self.fetch_batch_size:
        yield group
        # make sure to clear the group after yield
        group.clear()
    # make sure to emit eny partial groups after looping
    if len(group) > 0:
      yield group

  # query_activity_runs
  def query_activity_runs(self, runs_query, run_filter_parameters, keep, remove):
    '''
      Creates item groups ( lists of max size fetch_batch_size) from the input query after executing it using run_filter_parameters
      query response here has pagination logic. It should have following fields      
        i)  value : list of items (current page)
        ii) continuation_token: The continuation token for getting the next page of results, if any remaining results exist, null otherwise.

        Example Model: 
          https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.models.pipelinerunsqueryresponse?view=azure-python-preview
    '''
    group = []
    page_count =0
    run_filter_parameters.continuation_token = None
    while page_count == 0 or (run_filter_parameters.continuation_token and page_count <= self.max_pages):
      page_result = runs_query(filter_parameters = run_filter_parameters)
      page_count += 1
      for entry in page_result.value:
        # add item to group
        group.append(AzureArtifact.project_dict(entry.as_dict(), keep, remove))
        # yield the group list for every batch size
        if len(group) >= self.fetch_batch_size:
          yield group
          # make sure to clear the group after yield
          group.clear()
      # update continuation_token for run_filter_parameters
      run_filter_parameters.continuation_token = page_result.continuation_token
    # make sure to emit eny partial groups after outer looping
    if len(group) > 0:
      yield group

# COMMAND ----------

# MAGIC %md
# MAGIC ###### SynapseWorkspace

# COMMAND ----------

# DBTITLE 1,SynapseWorkspace
from datetime import datetime, timedelta, date, time as timex, timezone
from zoneinfo import ZoneInfo

"""
  SynapseWorkspace
"""
class SynapseWorkspace(AzureArtifact):
  """
    constructor

    :artifacts_client - create one using *get_synapse_artifacts_client* utility function

    Refereces:
      - https://learn.microsoft.com/en-us/python/api/azure-synapse/azure.synapse?view=azure-python-preview

  """
  # constructor
  def __init__(self, tz_info, artifacts_client, fetch_batch_size=20):
    super().__init__(tz_info, artifacts_client, fetch_batch_size)

  # get_workspace_info  
  def get_workspace_info (
    self,
    keep=[
      'id', 
      'name', 
      'type', 
      'workspace_uid', 
      'location',
      'provisioning_state', 
      'default_data_lake_storage',
      'workspace_repository_configuration', 
      'purview_configuration', 
      'extra_properties'
      ],
    remove=[]    
    ):
    '''
      Query workspace info      
    '''
    workspace = self.client.workspace.get()
    return AzureArtifact.project_dict(workspace.as_dict(), keep, remove)
  
  # list_sql_pools
  def list_sql_pools (
    self, 
    keep=['id', 'name', 'type', 'location', 'sku', 'provisioning_state', 'status', 'creation_date'],
    remove=[]
    ):
    '''
      Query SQL Pools
      SDK Reference: 
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.sqlpoolsoperations?view=azure-python-preview#azure-synapse-artifacts-operations-sqlpoolsoperations-list
    '''
    result = self.client.sql_pools.list() # returns SqlPoolInfoListResult
    yield from self.fetch_from_iter(result.value, keep, remove)

  # list_bigdata_pools
  def list_bigdata_pools (
    self, 
    keep=[],
    remove=[]
    ):
    '''
      Query Spark Pools
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.bigdatapoolsoperations?view=azure-python-preview#azure-synapse-artifacts-operations-bigdatapoolsoperations-list
    '''
    result = self.client.big_data_pools.list() # returns BigDataPoolResourceInfoListResult
    yield from self.fetch_from_iter(result.value, keep, remove)

  # list_linked_services
  def list_linked_services (
    self, 
    keep=[],
    remove=[]
    ):
    '''
      Query Pipe Lines
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.linkedserviceoperations?view=azure-python-preview#azure-synapse-artifacts-operations-linkedserviceoperations-get-linked-services-by-workspace
    '''
    result = self.client.linked_service.get_linked_services_by_workspace() # returns ItemPaged[LinkedServiceResource]
    yield from self.fetch_from_iter(result, keep, remove)

  # list_data_flows
  def list_data_flows (
    self, 
    keep=[],
    remove=[]
    ):
    '''
      Query Data Flows
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.dataflowoperations?view=azure-python-preview#azure-synapse-artifacts-operations-dataflowoperations-get-data-flows-by-workspace
        
    '''
    result = self.client.data_flow.get_data_flows_by_workspace() # returns ItemPaged[DataFlowResource]
    yield from self.fetch_from_iter(result, keep, remove)

  # list_pipelines
  def list_pipelines (
    self, 
    keep=[],
    remove=[]
    ):
    '''
      Query Pipelines
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.pipelineoperations?view=azure-python-preview#azure-synapse-artifacts-operations-pipelineoperations-get-pipelines-by-workspace
    '''
    result = self.client.pipeline.get_pipelines_by_workspace() # returns ItemPaged[PipelineResource]
    yield from self.fetch_from_iter(result, keep, remove)

  # list_notebooks
  def list_notebooks (
    self, 
    keep=[],
    remove=[]
    ):
    '''
      Query Noteboos
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.notebookoperations?view=azure-python-preview#azure-synapse-artifacts-operations-notebookoperations-get-notebooks-by-workspace
    '''
    result = self.client.notebook.get_notebooks_by_workspace() # returns ItemPaged[NotebookResource]
    yield from self.fetch_from_iter(result, keep, remove)

 # list_spark_job_definitions
  def list_spark_job_definitions (
    self, 
    keep=[],
    remove=[]
    ):
    '''
      Query Spark Jobs
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.sparkjobdefinitionoperations?view=azure-python-preview#azure-synapse-artifacts-operations-sparkjobdefinitionoperations-get-spark-job-definitions-by-workspace
    '''
    result = self.client.spark_job_definition.get_spark_job_definitions_by_workspace() # returns ItemPaged[SparkJobDefinitionResource]
    yield from self.fetch_from_iter(result, keep, remove)

  # list_sqlscripts
  def list_sqlscripts (
    self, 
    keep=[],
    remove=[]
    ):
    '''
      Query Pipe Lines
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.sqlscriptoperations?view=azure-python-preview#azure-synapse-artifacts-operations-sqlscriptoperations-get-sql-scripts-by-workspace
    '''
    result = self.client.sql_script.get_sql_scripts_by_workspace() # rerurns ItemPaged[SqlScriptResource]
    yield from self.fetch_from_iter(result, keep, remove)    

  # list_triggers
  def list_triggers (
    self, 
    keep=[],
    remove=[]
    ):
    '''
      Query triggers
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.triggeroperations?view=azure-python-preview#azure-synapse-artifacts-operations-triggeroperations-get-triggers-by-workspace
    '''
    result = self.client.trigger.get_triggers_by_workspace() # returns ItemPaged[TriggerResource]
    yield from self.fetch_from_iter(result, keep, remove)    

  # list_libraries
  def list_libraries (
    self, 
    keep=[],
    remove=[]
    ):
    '''
      Query Libraries
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.libraryoperations?view=azure-python-preview#azure-synapse-artifacts-operations-libraryoperations-list
    '''
    result = self.client.library.list() # returns ItemPaged[LibraryResource]
    yield from self.fetch_from_iter(result, keep, remove)    

  # list_datasets
  def list_datasets (
    self, 
    keep=[],
    remove=[]
    ):
    '''
      Query Pipe Lines
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.datasetoperations?view=azure-python-preview#azure-synapse-artifacts-operations-datasetoperations-get-datasets-by-workspace
    '''
    result = self.client.dataset.get_datasets_by_workspace() # returns ItemPaged[DatasetResource]
    yield from self.fetch_from_iter(result, keep, remove)

  # list_pipeline_runs_dep
  def list_pipeline_runs_dep (
    self,
    run_filter_params,
    keep=[],
    remove=[]
    ):
    '''
      Query Pipe Lines
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.pipelinerunoperations?view=azure-python-preview#azure-synapse-artifacts-operations-pipelinerunoperations-query-pipeline-runs-by-workspace
    '''
    result = self.client.pipeline_run.query_pipeline_runs_by_workspace(
      filter_parameters = run_filter_params
    ) # returns PipelineRunsQueryResponse

    for run in result.value:
      yield  AzureArtifact.project_dict(run.as_dict(), keep, remove)

  # list_pipeline_runs
  def list_pipeline_runs (
    self,
    last_updated_date,
    keep=[],
    remove=[]
    ):
    '''
      Query Pipeline runs by last_updted_date (in UTC)
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.pipelinerunoperations?view=azure-python-preview#azure-synapse-artifacts-operations-pipelinerunoperations-query-pipeline-runs-by-workspace
    '''
    last_updated_after  = datetime.combine(last_updated_date, timex(0, 0, 0, 0)).replace(tzinfo=self.tz_info)
    last_updated_before = datetime.combine(last_updated_date, timex(23, 59, 59, 999999)).replace(tzinfo=self.tz_info)
    print(f"INFO: Running {self.__class__.__name__}::list_pipeline_runs with last_updated_date={last_updated_date}")

    # set RunFilterParameters
    run_filter_params = AzureArtifact.create_run_filter_parameters(last_updated_after, last_updated_before)    
    runs_query = self.client.pipeline_run.query_pipeline_runs_by_workspace
    yield from self.query_activity_runs(runs_query, run_filter_params, keep, remove)

# list_trigger_runs
  def list_trigger_runs (
    self,
    last_updated_date,
    keep=[],
    remove=[]
    ):
    '''
      Query Pipe Lines
      SDK Reference:
        https://learn.microsoft.com/en-us/python/api/azure-synapse-artifacts/azure.synapse.artifacts.operations.triggerrunoperations?view=azure-python-preview#azure-synapse-artifacts-operations-triggerrunoperations-query-trigger-runs-by-workspace
    '''
    last_updated_after  = datetime.combine(last_updated_date, timex(0, 0, 0, 0)).replace(tzinfo=self.tz_info)
    last_updated_before = datetime.combine(last_updated_date, timex(23, 59, 59, 999999)).replace(tzinfo=self.tz_info)
    print(f"INFO: Running {self.__class__.__name__}::list_trigger_runs with last_updated_date={last_updated_date}")

    # set RunFilterParameters
    run_filter_params = AzureArtifact.create_run_filter_parameters(last_updated_after, last_updated_before)    
    runs_query = self.client.trigger_run.query_trigger_runs_by_workspace
    yield from self.query_activity_runs(runs_query, run_filter_params, keep, remove)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### SynapseMetrics

# COMMAND ----------

from datetime import timedelta

# SynapseMetrics
class SynapseMetrics():
  # constructor
  def __init__(self, metrics_client, num_days = 90, granularity_mins = 15, fetch_batch_size=500, max_pages=5000):
    self.client = metrics_client
    self.num_days = num_days
    self.granularity_mins = granularity_mins
    self.fetch_batch_size = fetch_batch_size
    self.max_pages = max_pages

  # fetch_metrics
  def fetch_metrics(self, metrics):
    '''
      Creates item groups ( lists of max size fetch_batch_size) from input iterator
    '''
    group = []
    for metric in metrics:
      for ts_entry in metric.timeseries:
        for metric_value in ts_entry.data:
          # add item to group
          group.append({
            "name": metric.name,
            "timestamp" : metric_value.timestamp,            
            "average"   : metric_value.average,
            "count"     : metric_value.count,
            "maximum"   : metric_value.maximum,
            "minimum"   : metric_value.minimum,
            "total"     : metric_value.total,
          })
          # yield the group list for every batch size
          if len(group) >= self.fetch_batch_size:
            yield group
            # make sure to clear the group after yield
            group.clear()
    # make sure to emit eny partial groups after looping
    if len(group) > 0:
      yield group

  # get_dedicated_pool_metrics
  def get_dedicated_sql_pool_metrics(self, resource_id):
    '''
      Quries metrics for a specific dedicated sql metric
      resource_id: input sql pool resource id
    '''
    response = self.client.query_resource(
        resource_id,
        metric_names=[
          "DWULimit", 
          "DWUUsed", 
          "DWUUsedPercent", 
          "MemoryUsedPercent",
          "CPUPercent",
          "Connections",
          "ActiveQueries"
          ],
        timespan=timedelta(days=self.num_days),
        granularity=timedelta(minutes=self.granularity_mins),
        aggregations=[
          MetricAggregationType.AVERAGE, 
          MetricAggregationType.COUNT, 
          MetricAggregationType.MINIMUM, 
          MetricAggregationType.MAXIMUM, 
          MetricAggregationType.TOTAL
        ],
    )
    # Fetch Metrics
    yield from self.fetch_metrics(response.metrics)

# get_spark_pool_metrics
  def get_spark_pool_metrics(self, resource_id):
    '''
      Query metrics for a specific spark pool
      resource_id: input spark pool resource id
    '''
    response = self.client.query_resource(
        resource_id,
        metric_names=[
          "BigDataPoolApplicationsEnded", 
          "BigDataPoolAllocatedCores", 
          "BigDataPoolAllocatedMemory", 
          "BigDataPoolApplicationsActive",
          ],
        timespan=timedelta(days=self.num_days),
        granularity=timedelta(minutes=self.granularity_mins),
        aggregations=[
          MetricAggregationType.AVERAGE, 
          MetricAggregationType.COUNT, 
          MetricAggregationType.MINIMUM, 
          MetricAggregationType.MAXIMUM, 
          MetricAggregationType.TOTAL
        ],
    )
    # Fetch Metrics
    yield from self.fetch_metrics(response.metrics)

  # get_workspace_level_metrics
  def get_workspace_level_metrics(self, resource_id):
    '''
      Query Workspace level metrics
      resource_id: input workspace resource id
    '''
    response = self.client.query_resource(
        resource_id,
        metric_names=[
          "IntegrationActivityRunsEnded", 
          "IntegrationPipelineRunsEnded", 
          "IntegrationTriggerRunsEnded", 
          "BuiltinSqlPoolDataProcessedBytes",
          "BuiltinSqlPoolLoginAttempts",
          "BuiltinSqlPoolRequestsEnded",
          ],
        timespan=timedelta(days=self.num_days),
        granularity=timedelta(hours=1),
        aggregations=[
          MetricAggregationType.AVERAGE, 
          MetricAggregationType.COUNT, 
          MetricAggregationType.MINIMUM, 
          MetricAggregationType.MAXIMUM, 
          MetricAggregationType.TOTAL
        ],
    )
    # Fetch Metrics
    yield from self.fetch_metrics(response.metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### > Profiler Extract Classes

# COMMAND ----------

import os, time, json, glob

# COMMAND ----------

# MAGIC %md
# MAGIC ###### ProfilerDataframeWriter

# COMMAND ----------

# DBTITLE 1,ProfilerDataframeWriter
import os
from pyspark.sql.functions import lit

# ProfilerDataframeWriter
class ProfilerDataframeWriter:
  '''
    Constructor takes DataStage Location and pool name
      
  '''
  def __init__(self, data_stage_root, synapse_pool_name, target_format = "json"):
    '''
      :data_stage_root    Root folder path for different table extracts
      :synapse_pool_name  Mainly used as parition name
    '''
    print(f"INFO: ProfilerDataframeWriter - validating data_stage_root ({data_stage_root})")
    ignore = dbutils.fs.ls(data_stage_root)

    self.data_stage_root = data_stage_root
    self.synapse_pool_name = synapse_pool_name
    self.target_format = target_format
  
  # write
  def write(self, df, target_name, save_mode="append"):
    '''
      Write to taget location using optional synapse_db_name
    '''
    target_path = os.path.join(
      self.data_stage_root, 
      target_name, 
      f"pool_name={self.synapse_pool_name}"
    )
    print(f"INFO: ProfilerDataframeWriter::write writing to {target_path}...")
    df.cache()
    # write to target
    write_result = (
      df.write
     .mode(save_mode)
     .format(self.target_format)
     .save(target_path)
     )
    print(f"INFO: ProfilerDataframeWriter::write Successfull [ record_count: {df.count()}]")
    df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### ProfilerStepState

# COMMAND ----------

# DBTITLE 1,ProfilerStepState
# ProfilerStepState
class ProfilerStepState:
  """
    Utility class to maintain a profiler writter step
  """
  def __init__(self, target_location, data_filename_pattern):
    self.target_location = target_location
    self.data_filename_pattern = data_filename_pattern

  # check_successfull_run
  def check_successfull_run(self):
    # returns True if this step is already successull, so we can skip this time  
    sucess_file = os.path.join(self.target_location, "_SUCCESS")
    if os.path.isfile(sucess_file):
      return True
    return False

  # get_successfull_run_info
  def get_successfull_run_info(self):
    # returns True if this step is already successull, so we can skip this time  
    sucess_file = os.path.join(self.target_location, "_SUCCESS")
    run_info = ""
    with open(sucess_file, "r") as reder:
      run_info = reder.read()
    return run_info

  # mark_successfull_run
  def mark_successfull_run(self, data):
    # marks the step as successfull, so taht we can skip this step in restart
    sucess_file = os.path.join(self.target_location, "_SUCCESS")
    with open(sucess_file, "w") as writer:
      writer.write(data)

  # target_create_or_cleanup
  def target_create_or_cleanup(self):
    # Initializes the step by creating (or cleaning up) step target folder
    if not os.path.exists(self.target_location):
      os.makedirs(self.target_location)
    else:
      # do clean up from previous partial run
      files_to_clean  = glob.glob("{}/{}".format(self.target_location, self.data_filename_pattern))
      for idx, entry in enumerate(files_to_clean):
        if idx == 0:
          print("INFO: Removing files from previous partial run at {}".format(self.target_location))
        print(f"      {entry}...")
        os.remove(entry)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### ProfilerStepDataExtractor

# COMMAND ----------

# DBTITLE 1,ProfilerStepDataExtractor
# ProfilerStepDataExtractor
class ProfilerStepDataExtractor:
  # constructor
  def __init__(self, step_name, data_location):
    """
      Builds a Data extractor with given StepName and DataLocation

      :param step_name:     Profiling Step Name 
      :param data_location: Path for Data Extract (should start with /dbfs/...)
    """
    # validate data_location
    if not os.path.exists(data_location) or not os.path.isdir(data_location):
      raise ValueError(f"ERROR: Invalid data_location '{data_location}'. Path does't exists!!!")
    self.target_folder = os.path.join(data_location, step_name)
    self.step_name = step_name    
    self.target_filepath_tmpl = "{}" + f'_{step_name}_' + "{}.json"
    self.state = ProfilerStepState(self.target_folder, f'*_{step_name}_*.json')

  # extract
  def extract(self, data_extract_fx, output_prefix):
    """
      Runs the extract function and write to required target folder
    """
    # ---------------------------------------
    # (1) check if this step is already complete
    # ---------------------------------------
    if self.state.check_successfull_run():
      print(f"    Skipping Profiler extract step '{self.step_name}'")
      print(f"    Data extract already present at '{self.target_folder}'")
      return 

    print(f"INFO: Starting Profiler extract step '{self.step_name}'")
    self.state.target_create_or_cleanup()

    # ---------------------------------------
    # (2) Output Filepath
    # ---------------------------------------
    ctime = time.mktime(time.localtime())
    output_file_name = self.target_filepath_tmpl.format(output_prefix, ctime)
    output_file_path = os.path.join(self.target_folder, output_file_name)

    # ---------------------------------------
    # (3) Extract and Write
    # ---------------------------------------
    num_recs = 0
    with open(output_file_path, 'w') as ofile:
      print("    > Writing the extract to {}".format(output_file_path))
      print("      ", end="")
      # call extract fx
      for batch in data_extract_fx():
        # for each batch
        for entry in batch:
          output_line = json.dumps(entry, default=str)
          ofile.write(output_line + os.linesep)
          num_recs += 1
        # flush each batch
        ofile.flush()
        print(f" {num_recs}", end="")
    if num_recs:
      print("")
    # ---------------------------------------
    # (4) Mark the extract Successfull
    # ---------------------------------------
    self.state.mark_successfull_run(f"num_recs: {num_recs}")
    print(f"    > Profiler extract completed with '{num_recs}' records")


# COMMAND ----------

# MAGIC %md
# MAGIC ###### ProfilerStepPartitionExtractor

# COMMAND ----------

# DBTITLE 1,ProfilerStepPartitionExtractor
# ProfilerStepPartitionExtractor
class ProfilerStepPartitionExtractor(ProfilerStepDataExtractor):
  # constructor
  def __init__(self, step_name, data_location, partition_key, partition_value):
    """
      Builds a Data extractor with given StepName and DataLocation

      :param step_name:     Profiling Step Name 
      :param data_location: Path for Data Extract (should start with /dbfs/...)
    """
    partition_folder_name = f"{partition_key}={partition_value}"
    self.partition_key = partition_key
    self.partition_value = partition_value
    self.partition_folder_name = partition_folder_name
    
    # validate data_location
    if not os.path.exists(data_location) or not os.path.isdir(data_location):
      raise ValueError(f"ERROR: Invalid data_location '{data_location}'. Path does't exists!!!")
    self.target_folder = os.path.join(data_location, step_name, partition_folder_name)
    self.step_name = f"{step_name}/{partition_folder_name}"
    self.target_filepath_tmpl = "{}" + f'_{step_name}_' + "{}.json"
    self.state = ProfilerStepState(self.target_folder, f'*_{step_name}_*.json')