# MAGIC %run "../../Configuration"

# COMMAND ----------

# MAGIC %run "../../HelperFunctions"

# COMMAND ----------

# MAGIC %run "../HelperMethodsClass"

# COMMAND ----------
import snowflake.snowpark as snowpark
import datetime

def main(session) -> str:

    # COMMAND ----------

    # dbutils.widgets.text("pipelineID", "", "pipelineID")
    # dbutils.widgets.text("jobID", "", "jobID")
    # dbutils.widgets.text("debugFlag", "", "debugFlag")
    # dbutils.widgets.text("getParameterPropertyApiUrl", "", "getParameterPropertyApiUrl")
    # dbutils.widgets.text("processStartDateTime", "", "processStartDateTime")
    # dbutils.widgets.text("processEndDateTime", "", "processEndDateTime")


    # # COMMAND ----------


    # pipeline_id = dbutils.widgets.get("pipelineID")
    # job_id = dbutils.widgets.get("jobID")
    # get_parameter_property_api_url = dbutils.widgets.get("getParameterPropertyApiUrl")
    # debug_flag = dbutils.widgets.get("debugFlag")
    # process_start_datetime = datetime.datetime.strptime(dbutils.widgets.get("processStartDateTime"), "%Y-%m-%dT%H:%M:%S")
    # process_end_datetime = datetime.datetime.strptime(dbutils.widgets.get("processEndDateTime"), "%Y-%m-%dT%H:%M:%S")


    # # COMMAND ----------


    # #Getting remaining pipeline parameters
    # parameter_dictionary = get_pipeline_parameter_values(pipeline_id, get_parameter_property_api_url, debug_flag, job_id)
    # data_lake_service_principal_client_id_secret_reference = parameter_dictionary['dataLakeServicePrincipalClientIdSecretReference']
    # data_lake_service_principal_client_secret_secret_reference = parameter_dictionary['dataLakeServicePrincipalClientSecretSecretReference']
    # tenant_id_secret_reference = parameter_dictionary['tenantIdSecretReference']
    # silver_storage_account = parameter_dictionary['silverStorageAccount']
    # source_raw_folder_path = parameter_dictionary['sourceRawFolderPaths']
    # synapse_write_out_mode = parameter_dictionary['synapseWriteOutMode']
    # synapse_sql_db_name = parameter_dictionary['synapseSqlDbName']
    # synapse_sql_staging_table_schema = parameter_dictionary['synapseStagingTableSchema']
    # synapse_sql_staging_table_name = parameter_dictionary['synapseStagingTableName']
    # synapse_sql_dim_table_schema = parameter_dictionary['synapseDimTableSchema']
    # synapse_sql_dim_table_name = parameter_dictionary['synapseDimTableName']
    # synapse_resource_name = parameter_dictionary['synapseResourceName']
    # synapse_data_lake_storage_account = parameter_dictionary['synapseDataLakeStorageAccount']
    # synapse_data_lake_container = parameter_dictionary['synapseDataLakeContainer']
    # synapse_data_lake_initial_folder = parameter_dictionary['synapseDataLakeInitialFolder']
    # secret_scope_name = parameter_dictionary['secretScopeName']
    # business_key_columns = [element for element in parameter_dictionary['businessKeyColumns'].split(',') if element != ''] #parameter_dictionary['businessKeyColumns']
    # type_i_columns_list = [element for element in parameter_dictionary['typeIColumnsList'].split(',') if element != '']


    # # COMMAND ----------

    # dataLakeServicePrincipalConnectionInitiation(data_lake_service_principal_client_id_secret_reference, data_lake_service_principal_client_secret_secret_reference, tenant_id_secret_reference,secret_scope_name)


    # # COMMAND ----------


    # helper_object = HelperMethods(storage_account_name = silver_storage_account,
    #                                     source_raw_folder_paths = source_raw_folder_path,
    #                                     process_start_datetime = process_start_datetime,
    #                                     process_end_datetime = process_end_datetime,
    #                                     business_key_columns = business_key_columns,
    #                                     type_i_columns_list = type_i_columns_list
    #                                 )

    # # COMMAND ----------

    # dfDim = helper_object._data_read([source_raw_folder_path], 'PBIApps', 'type_i_full_load')


    # # COMMAND ----------


    # dfDim = (dfDim
    #             .withColumnsRenamed({'id': 'AppGUID',
    #                                 'name': 'AppName',
    #                                 'description': 'AppDescription',
    #                                 'publishedBy': 'AppPublisher',
    #                                 'workspaceId': 'WorkspaceGUID'
    #             })
    #             .select('AppGUID', 'AppName', 'AppDescription', 'AppPublisher', 'WorkspaceGUID')
    # )

    # final_output_df = dfDim.transform(helper_object.add_dimension_audit_columns)

    # # COMMAND ----------

    # final_output_df.cache()

    # updating_records_count = final_output_df.count()


    # # COMMAND ----------

    # if(updating_records_count != 0):
    # synapse_ingestion(final_output_df, synapse_write_out_mode, synapse_sql_db_name, synapse_sql_staging_table_schema, synapse_sql_staging_table_name, synapse_resource_name, synapse_data_lake_storage_account, synapse_data_lake_container, synapse_data_lake_initial_folder)


    # dbutils.notebook.exit({'updating_records_count': updating_records_count})

    # COMMAND ----------
    return "success"