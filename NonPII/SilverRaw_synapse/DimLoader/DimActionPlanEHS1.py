# MAGIC %md
# MAGIC # Dynamic Data Ingestion from Silver_Raw to Synapse
# MAGIC
# MAGIC **Scope:** Dim, Fact & Ref tables in Synapse
# MAGIC
# MAGIC **Description:** This notebook is used to ingest data from Silver Raw layer (in data/delta lake) to Gold layer (dim, fact and ref EDWStage tables in Synapse)).
# MAGIC
# MAGIC **Note:** 
# MAGIC 1. This notebook is not loading data into EDW in Synapse, but instead data is loaded into EDW from EDWStage via stored procedures in Synapse.
# MAGIC 2. Parameter "transformationQuery" contains transformation queries to transform source tables into the target table. When initially developing transformationQuery, recommend using double quote(") instead of single quote('), cause if single quote is used, when the query is copied into JM script and running JM script in SSMS, single quote should be replaced by 2 single quotes to make it work; if double quote is used, no change is needed.
# MAGIC 3. At the end of this Cmd 'Read source tables and create temp views', the temp view's names are the same as the source table names (that is, {schema name}_{table name}), therefore, we should use the source table names for tables in transformationQuery.
# MAGIC
# MAGIC **Revision History:**
# MAGIC
# MAGIC | Date | Description | Modified By |
# MAGIC |:----:|--------------|--------|
# MAGIC | Nov 29, 2022 | Inital development for dynamic notebook  | David Sun |
# MAGIC | Jan 26, 2023 | Based on feedbacks from ED, change name to DynamicTableLoader and cover "Ref" tables  | David Sun |

# COMMAND ----------

# DBTITLE 1,Import configuration function
# MAGIC %run "/Configuration" 

# COMMAND ----------

# DBTITLE 1,Import helper functions
# MAGIC %run "HelperFunctions"

# COMMAND ----------

# DBTITLE 1,Import HelperMethodsClass
# MAGIC %run "/silverRaw_synapse/HelperMethodsClass"

# COMMAND ----------

# DBTITLE 1,Import required libraries
import datetime
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *
from functools import reduce
import json

def main() -> str:
    return "DimActionPlanEHS1 update" 
# COMMAND ----------

# DBTITLE 1,Get pipeline parameters into variables
# create dbutils widgets
# dbutils.widgets.text("pipelineID", "", "pipelineID")
# dbutils.widgets.text("jobID", "", "jobID")
# dbutils.widgets.text("debugFlag", "", "debugFlag")
# dbutils.widgets.text("getParameterPropertyApiUrl", "", "getParameterPropertyApiUrl") # Azure logic apps workflow url
# dbutils.widgets.text("processStartDateTime", "", "processStartDateTime")
# dbutils.widgets.text("processEndDateTime", "", "processEndDateTime")
# # get dbutils widgets values
# pipeline_id = dbutils.widgets.get("pipelineID")
# job_id = dbutils.widgets.get("jobID")
# get_parameter_property_api_url = dbutils.widgets.get("getParameterPropertyApiUrl")
# debug_flag = dbutils.widgets.get("debugFlag")
# process_start_datetime = datetime.datetime.strptime(dbutils.widgets.get("processStartDateTime"), "%Y-%m-%dT%H:%M:%S")
# process_end_datetime = datetime.datetime.strptime(dbutils.widgets.get("processEndDateTime"), "%Y-%m-%dT%H:%M:%S")
# # extract pipeline parameters using get_pipeline_parameter_values function
# parameter_dictionary = get_pipeline_parameter_values(pipeline_id, get_parameter_property_api_url, debug_flag, job_id)
# data_lake_service_principal_client_id_secret_reference = parameter_dictionary['dataLakeServicePrincipalClientIdSecretReference']
# data_lake_service_principal_client_secret_secret_reference = parameter_dictionary['dataLakeServicePrincipalClientSecretSecretReference']
# tenant_id_secret_reference = parameter_dictionary['tenantIdSecretReference']
# silver_storage_account = parameter_dictionary['silverStorageAccount']
# synapse_write_out_mode = parameter_dictionary['synapseWriteOutMode']
# synapse_sql_db_name = parameter_dictionary['synapseSqlDbName']
# synapse_sql_staging_table_schema = parameter_dictionary['synapseStagingTableSchema']
# synapse_sql_staging_table_name = parameter_dictionary['synapseStagingTableName']
# synapse_resource_name = parameter_dictionary['synapseResourceName']
# synapse_data_lake_storage_account = parameter_dictionary['synapseDataLakeStorageAccount']
# synapse_data_lake_container = parameter_dictionary['synapseDataLakeContainer']
# synapse_data_lake_initial_folder = parameter_dictionary['synapseDataLakeInitialFolder']
# # Dim and Ref tables need type_i_columns_list and Fact tables don't
# if parameter_dictionary['synapseTableType'] in ['Dim', 'Ref']:
#     type_i_columns_list = [element.strip() for element in parameter_dictionary['typeIColumnsList'].split(',') if element.strip() != '']
# else:
#     type_i_columns_list = None
# # get business key columns
# business_key_columns = [element.strip() for element in parameter_dictionary['businessKeyColumns'].split(',')]
# creation_date_time_column = None
# if  'creation_date_time_column' in parameter_dictionary:
#     creation_date_time_column = parameter_dictionary['creation_date_time_column']
# modification_date_time_column = None
# if  'modification_date_time_column' in parameter_dictionary:
#     modification_date_time_column = parameter_dictionary['modification_date_time_column']
# # COMMAND ----------

# # DBTITLE 1,Print critical parameters
# # The Cmd below is for debug purpose to get source/target/transformation parameters values
# # sourceSCDTypeFilter parameter needs to be encoded and decoded to get the correct value (when /n is present in transformationQuery)
# # strip is used to remove leading and trailing empty spaces
# source_scd_filter = json.loads(parameter_dictionary['sourceSCDTypeFilter'].encode().decode('unicode_escape').strip())
# source_list = []
# filter_column_list = []
# for source_scd_filter_object in source_scd_filter:
#     for source_folder_path in source_scd_filter_object['sourceRawFolderPaths'].split(','):
#         source_folder_path = source_folder_path.strip()
#         soure_table = source_scd_filter_object['sourceTable'].strip()
#         filter_column = source_scd_filter_object['filterColumn'].strip()
#         source_list.append(source_folder_path+soure_table)
#         filter_column_list.append(filter_column)

# # print critical parameters below
# print(
#     'The most critical parameters for troubleshooting are listed as follows:',
#     '                Job ID: '+job_id,
#     '           Pipeline ID: '+pipeline_id,
#     '  Synapse Target Table: '+synapse_sql_staging_table_name,
#     '    Synapse SQL Server: '+synapse_resource_name,
#     '        Synapse SQL DB: '+synapse_sql_db_name,
#     '  Business Key Columns: '+','.join(business_key_columns),
#     '        Type I Columns: '+','.join(type_i_columns_list or ['No type I columns since this is a Fact table']),
#     '         Source Tables: '+','.join(source_list),
#     '        Filter Columns: '+'No filters at all' if len(','.join(filter_column_list).replace(',', '')) == 0 else ','.join(filter_column_list),
#     'Source Storage Account: '+silver_storage_account,
#     'Synapse Staging Schema: '+synapse_sql_staging_table_schema,
#     ' Synapse Staging Table: '+synapse_sql_staging_table_name,
#     '  ADLS Storage Account: '+synapse_data_lake_storage_account+'  # this is for Synapse Ingestion tempDir',
#     '        ADLS Container: '+synapse_data_lake_container+'  # this is for Synapse Ingestion tempDir',
#     '           ADLS Folder: '+synapse_data_lake_initial_folder+'  # this is for Synapse Ingestion tempDir',
#     sep='\n')

# # COMMAND ----------

# source_scd_filter

# # COMMAND ----------

# filter_column_list

# # COMMAND ----------

# # DBTITLE 1,Print transformation query
# # transformationQuery parameter needs to be encoded and decoded to get the correct value (when /n is present in transformationQuery)
# # strip is used to remove leading and trailing empty spaces
# transformation_query = parameter_dictionary['transformationQuery'].encode().decode('unicode_escape').strip()
# print('The tranformation query can be found below: \n'+transformation_query)

# # COMMAND ----------

# # DBTITLE 1,Grant access to Silver Raw and Synapse
# # grant access to data lake and synapse via service principal
# # if True is returned, it means access is successfully granted
# secret_scope_name = None
# if('secretScopeName' in parameter_dictionary):
#   secret_scope_name = parameter_dictionary['secretScopeName']

# if('dataLakeServicePrincipalClientIdSecretReference' in parameter_dictionary and
#    'dataLakeServicePrincipalClientSecretSecretReference' in parameter_dictionary and
#    'tenantIdSecretReference' in parameter_dictionary
#   ):
#   dataLakeServicePrincipalConnectionInitiation(parameter_dictionary['dataLakeServicePrincipalClientIdSecretReference'], parameter_dictionary['dataLakeServicePrincipalClientSecretSecretReference'], parameter_dictionary['tenantIdSecretReference'],secret_scope_name)


# # COMMAND ----------

# # DBTITLE 1,Create helper object
# # create helper object which will be used for reading data from Silver Raw and adding audit columns below
# helper_object = HelperMethods(storage_account_name = silver_storage_account,
#                                     source_raw_folder_paths = [],  # this variable is not used
#                                     process_start_datetime = process_start_datetime,
#                                     process_end_datetime = process_end_datetime,
#                                     business_key_columns = business_key_columns,
#                                     type_i_columns_list = type_i_columns_list,
#                                     creation_date_time_column = creation_date_time_column,
#                                     modification_date_time_column = modification_date_time_column
#                                 )

# # COMMAND ----------

# # DBTITLE 1,Read source tables and create temp views
# # extract source table, scd type and filter column for data reading
# for source_scd_filter_object in source_scd_filter:
#     source_raw_folder_paths = [f.strip() for f in source_scd_filter_object['sourceRawFolderPaths'].split(',')]
#     source_table = source_scd_filter_object['sourceTable'].strip()
#     scd_type = source_scd_filter_object['SCDType'].strip()
#     filter_column = source_scd_filter_object['filterColumn'].strip()
#     if filter_column == '':
#         helper_object._data_read(source_raw_folder_paths, source_table, scd_type).createOrReplaceTempView(source_table)
#     else:
#         helper_object._data_read(source_raw_folder_paths, source_table, scd_type).filter(col(filter_column) > process_start_datetime).createOrReplaceTempView(source_table)

# # COMMAND ----------

# # DBTITLE 1,Transform data into the final dataframe
# # get the final pyspark dataframe by running transformation and adding audit columns
# ProcessedDF = spark.sql(transformation_query)

# add_audit_columns = helper_object.add_dimension_audit_columns
# #EWI: SPRKPY1002 => pyspark.sql.dataframe.DataFrame.transform is not supported
# final_output_df = ProcessedDF.transform(add_audit_columns)

# # COMMAND ----------

# # DBTITLE 1,Cleanup HTML tags and French characters
# import html
# def unescape_html(value):
#     return html.unescape(value) if isinstance(value, str) else value
# #EWI: SPRKPY1073 => pyspark.sql.functions.udf function without the return type parameter is not supported. See documentation for more info.

# unescape_html_udf = udf(unescape_html)

# df_clean = final_output_df.withColumn("DetailedDescription", unescape_html_udf(col("DetailedDescription")))\
#                           .withColumn("DetailedDescription", regexp_replace(col("DetailedDescription"),'<.*?>',''))\
#                           .withColumn("ProgressUpdatesComments", unescape_html_udf(col("ProgressUpdatesComments")))\
#                           .withColumn("ProgressUpdatesComments", regexp_replace(col("ProgressUpdatesComments"),'<.*?>',''))

# final_output_df =  df_clean


# # COMMAND ----------

# # df = final_output_df.select([max(length(col(name))).alias(name) for name in final_output_df.schema.names])
# # row=df.first().asDict()
# # df2 = spark.createDataFrame([Row(col=name, length=row[name]) for name in df.schema.names], ['col', 'length'])

# # df2.display()

# # COMMAND ----------

# # DBTITLE 1,Cache the final dataframe and get its rowcount
# # cache the final dataframe for reuse due to lazy evaluation
# final_output_df.cache_result()

# updating_records_count = final_output_df.count()

# print('updating_records_count(prior to synapse ingestion): ', updating_records_count)

# # COMMAND ----------

# # DBTITLE 1,Load into Synapse and exit the notebook
# # # when new data is coming in, it is loaded into synapse
# # # the notebook will exit with rowcount of new data returned
# if(updating_records_count != 0):
#   synapse_ingestion(final_output_df, synapse_write_out_mode, synapse_sql_db_name, synapse_sql_staging_table_schema, synapse_sql_staging_table_name, synapse_resource_name, synapse_data_lake_storage_account, synapse_data_lake_container, synapse_data_lake_initial_folder)

# dbutils.notebook.exit({'updating_records_count': updating_records_count})