CREATE OR REPLACE PROCEDURE <% DB_NAME %>.<% SCHEMA_NAME %>.DimWell1()
    RETURNS VARCHAR(16777216)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python', '@GMAHADEVAN_DB.GMAHADEVAN_SCHEMA.SUNCOR_LIB/helper_class_2.zip')
    HANDLER = 'DimWell.main'
    IMPORTS = ('@<% DB_NAME %>.<% SCHEMA_NAME %>.GIT_STORED_PROC_REPO/branches/feature_1/NonPII/SilverRaw_synapse/DimLoader/DimWell.py')
    EXECUTE AS OWNER
    ;

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

CREATE OR REPLACE PROCEDURE <% DB_NAME %>.<% SCHEMA_NAME %>.DimActionPlanEHS1()
    RETURNS VARCHAR(16777216)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'DimActionPlanEHS1.main'
    IMPORTS = ('@<% DB_NAME %>.<% SCHEMA_NAME %>.GIT_STORED_PROC_REPO/branches/main/NonPII/SilverRaw_synapse/DimLoader/DimActionPlanEHS1.py')
    EXECUTE AS OWNER
    ;