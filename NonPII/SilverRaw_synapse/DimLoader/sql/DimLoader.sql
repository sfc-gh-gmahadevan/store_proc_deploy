CREATE OR REPLACE PROCEDURE GMAHADEVAN_DB.GMAHADEVAN_SCHEMA.DimAccountingDocumentHeader()
    RETURNS VARCHAR(16777216)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'DimAccountingDocumentHeader.main'
    IMPORTS = ('@GMAHADEVAN_DB.GMAHADEVAN_SCHEMA.GIT_STORED_PROC_REPO/branches/dev/NonPII/SilverRaw_synapse/DimLoader/DimAccountingDocumentHeader.py')
    EXECUTE AS OWNER
    ;
    
CREATE OR REPLACE PROCEDURE GMAHADEVAN_DB.GMAHADEVAN_SCHEMA.DimAppPBI()
    RETURNS VARCHAR(16777216)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'DimAccountingDocumentHeader.main'
    IMPORTS = ('@GMAHADEVAN_DB.GMAHADEVAN_SCHEMA.GIT_STORED_PROC_REPO/branches/dev/NonPII/SilverRaw_synapse/DimLoader/DimAccountingDocumentHeader.py')
    EXECUTE AS OWNER
    ;

CREATE OR REPLACE PROCEDURE GMAHADEVAN_DB.GMAHADEVAN_SCHEMA.DimAccountingDocumentHeader()
    RETURNS VARCHAR(16777216)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'DimAccountingDocumentHeader.main'
    IMPORTS = ('@GMAHADEVAN_DB.GMAHADEVAN_SCHEMA.GIT_STORED_PROC_REPO/branches/dev/NonPII/SilverRaw_synapse/DimLoader/DimAccountingDocumentHeader.py')
    EXECUTE AS OWNER
    ;