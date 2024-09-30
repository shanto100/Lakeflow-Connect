# Databricks notebook source
# MAGIC %md
# MAGIC ### Instructions
# MAGIC
# MAGIC * Modify settings in "Configuration" cell as necessary.
# MAGIC * Execute the following cells one after another. Some are optional and can be skipped.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensure recent version of the Python SDK

# COMMAND ----------

# MAGIC %pip install databricks-sdk~=0.28.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

# DBTITLE 1,Configuration
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog, jobs, pipelines

w = WorkspaceClient()

# ======================
# Setup
# ======================

# The following function simplifies the replication of multiple tables from the same schema
def replicate_tables_from_db_schema(db_catalog_name, db_schema_name, db_table_names):
  return [pipelines.IngestionConfig(
            table = pipelines.TableSpec( 
            source_catalog=db_catalog_name,
            source_schema=db_schema_name,
            source_table=table_name,
            destination_catalog=target_catalog_name,
            destination_schema=target_schema_name,
          )) for table_name in db_table_names]

# The following function simplifies the replication of an entire DB schemas
def replicate_full_db_schema(db_catalog_name, db_schema_names):
  return [pipelines.IngestionConfig(
            schema = pipelines.SchemaSpec( 
            source_catalog=db_catalog_name,
            source_schema=db_schema_name,
            destination_catalog=target_catalog_name,
            destination_schema=target_schema_name,
          )) for db_schema_name in db_schema_names]

gateway_cluster_spec = None
# Uncomment the following to specify a cluster policy and/or spark configuration for the Gateway pipeline
# gateway_cluster_spec = pipelines.PipelineCluster(
#   # Uncomment to specifya a cluster policy  
#   # label="default", 
#   # policy_id="0011223344556677", 
#   # apply_policy_default_values=True,
#
#   # Uncomment to customize cluser Spark configuration
#   spark_conf={ }
#)

# The name of the UC connection with the credentials to access the source database
connection_name = "oneenv_mssql_connection"

# The name of the UC catalog and schema to store the replicated tables
target_catalog_name = "cmoore_customer_demos"
target_schema_name = "demo_cdc"

# The name of the UC catalog and schema to store the staging volume with intermediate
# CDC and snapshot data.
# Use the destination catalog/schema by default
stg_catalog_name = target_catalog_name 
stg_schema_name = target_schema_name 

# The name of the Gateway pipeline to create
gateway_pipeline_name = "cmoore_cdc_gateway"

# The name of the Ingestion pipeline to create
ingestion_pipeline_name = "cmoore_cdc_ingestion"

# Construct the full list of tables to replicate
# IMPORTANT: The letter case of the catalog, schema and table names MUST MATCH EXACTLY the case used in the source database system tables
#replicate_full_db_schema("MY_DB", ["MY_DB_SCHEMA"])
tables_to_replicate = replicate_tables_from_db_schema("oneenvsqldb", "dbo", ["cdc_customer", "cdc_product", "cdc_purchase"])
# Append tables from additional schemas as needed
#  + replicate_tables_from_db_schema("REPLACE_WITH_DBNAME", "REPLACE_WITH_SCHEMA_NAME_2", ["table3", "table4"])

# Customize who gets notified about failures
notifications = [
  pipelines.Notifications(
      email_recipients = [ w.current_user.me().user_name ],
      alerts = [ "on-update-failure", "on-update-fatal-failure", "on-flow-failure"]
      )
  ]

# COMMAND ----------

# DBTITLE 1,Create the Gateway pipeline

# determine the connection id
connection_id = w.connections.get(connection_name).connection_id

gateway_def = pipelines.IngestionGatewayPipelineDefinition(
      connection_id=connection_id,
      gateway_storage_catalog=stg_catalog_name, 
      gateway_storage_schema=stg_schema_name,
      gateway_storage_name = gateway_pipeline_name)

p = w.pipelines.create(
    name = gateway_pipeline_name, 
    gateway_definition=gateway_def, 
    notifications=notifications,
    clusters= [ gateway_cluster_spec.as_dict() ] if None != gateway_cluster_spec else None
    )
gateway_pipeline_id = p.pipeline_id

print(f"Gateway pipeline {gateway_pipeline_name} created: {gateway_pipeline_id}")

# COMMAND ----------

# DBTITLE 1,Create the Ingestion Pipeline

ingestion_def = pipelines.ManagedIngestionPipelineDefinition(
    ingestion_gateway_id=gateway_pipeline_id,
    objects=tables_to_replicate,
    )
p = w.pipelines.create(
    name = ingestion_pipeline_name, 
    ingestion_definition=ingestion_def, 
    notifications=notifications,
    serverless=True,
    photon=True,
    continuous=False,
    )
ingestion_pipeline_id = p.pipeline_id

print(f"Ingestion pipeline {ingestion_pipeline_name} created: {ingestion_pipeline_id}")
