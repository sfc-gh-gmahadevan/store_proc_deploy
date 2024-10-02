CREATE OR REPLACE PROCEDURE <% DB_NAME %>.<% SCHEMA_NAME %>.DimAccountingDocumentHeader()
    RETURNS VARCHAR(16777216)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'DimAccountingDocumentHeader.main'
    IMPORTS = ('@<% DB_NAME %>.<% SCHEMA_NAME %>.GIT_STORED_PROC_REPO/branches/main/NonPII/SilverRaw_synapse/DimLoader/DimAccountingDocumentHeader.py')
    EXECUTE AS OWNER
    ;

CREATE OR REPLACE PROCEDURE <% DB_NAME %>.<% SCHEMA_NAME %>.DimActionPlanEHS()
    RETURNS VARCHAR(16777216)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'DimActionPlanEHS.main'
    IMPORTS = ('@<% DB_NAME %>.<% SCHEMA_NAME %>.GIT_STORED_PROC_REPO/branches/main/NonPII/SilverRaw_synapse/DimLoader/DimActionPlanEHS.py')
    EXECUTE AS OWNER
    ;