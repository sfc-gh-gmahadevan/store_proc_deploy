import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import StringType, BinaryType, TimestampType
import datetime
from functools import reduce
from snowflake.snowpark import DataFrame


class HelperMethods():
    
    def __init__(self,
                 session,
                 silver_schema_names_list: list = None,
                 process_start_datetime: datetime.datetime = None,
                 process_end_datetime: datetime.datetime = None,
                 business_key_columns: list = None,
                 type_i_columns_list: list = None,
                 additional_column_list: list = None,
                 creation_date_time_column: str = None,
                 modification_date_time_column: str = None
                 ):
        self.silver_schema_names_list = silver_schema_names_list # List of schema names under the Silver database that hold the views
        self.process_start_datetime = process_start_datetime
        self.process_end_datetime = process_end_datetime
        self.process_datetime = datetime.datetime.now()
        self.session = session
        # Values to be set at the top of the methods that are generating those tables.
        if (business_key_columns is not None):
            self._business_key_columns = business_key_columns
        else:
            self._business_key_columns = []

        if (type_i_columns_list is not None):
            self._type_i_columns_list = type_i_columns_list
        else:
            self._type_i_columns_list = []

        # additional column if needed. usually only audit columns is needed extra
        if additional_column_list is not None:
            # Convert all values in the list to uppercase
            self.additional_column_list = [col.upper() for col in additional_column_list]
        else:
            self.additional_column_list = []

        # Columns that will be used to set the Effective Start Date audit column in the target the Dim table.
        # Use the creation/modification datetime column from the source if available.
        if creation_date_time_column is not None:
            self.creation_date_time_column = creation_date_time_column.upper()
        else:
            self.creation_date_time_column = ''

        if modification_date_time_column is not None:
            self.modification_date_time_column = modification_date_time_column.upper()
        else:
            self.modification_date_time_column = ''

        # If creation date time is an audit field, add to the additional column list.
        if (self.creation_date_time_column.startswith('__')
           and self.creation_date_time_column not in self.additional_column_list):
            self.additional_column_list.append(self.creation_date_time_column)

        # If modification date time is an audit field, add to the additional column list.
        if (self.modification_date_time_column.startswith('__')
           and self.modification_date_time_column not in self.additional_column_list):
            self.additional_column_list.append(self.modification_date_time_column)

    def _get_data_column_list(self, dataframe_column_list):
        """
        Returns a list of columns whose names don't start with __ thus are data columns.
        If additional_column_list is defined in the __init__ func, those columns will be appended to the column list.
        additional column list should only include audit columns, since all data columns are selected automatically
        """
        if isinstance(dataframe_column_list, str):
            dataframe_column_list = [dataframe_column_list]
        elif not isinstance(dataframe_column_list, list):
            dataframe_column_list = list(dataframe_column_list)
    
        return [column_name for column_name in dataframe_column_list if not column_name.startswith('__')] + self.additional_column_list
       
    def _unioning_dataframe_list(self, df_list: list):
        """
        Unions a list of datarames. Used for reading the same table of data from multiple mines at once.
        """
        return reduce(DataFrame.unionByName, df_list)

    def _select_data_columns(self, df):
        """
        Selecting data column i.e. columns whose names don't start with __.
        """
        columns_to_select = self._get_data_column_list(list(df.columns))
        return df.select(*columns_to_select)

    def _data_read(self,
                   source_schemas: list, # list of schema names
                   view_name: str, # Name of the view (source to the stage table)
                   data_read_type: str,
                   list_of_partition_values: list = None
                   ):
        """
        Reads data from a Snowflake source table or view using Snowpark.
    
        For 'regular_incremental' read mode, the source data is filtered based on the '__process_datetime' 
        column, which represents the timestamp of records landing in the silver layer.

        """
        # Generating a list of dataframes by reading from each view in the source schemas
        df_list = []
        for src_schema in source_schemas:
            # Read the view into a dataframe
            df = self.session.table(f'{src_schema}.{view_name}')

            if data_read_type == 'regular_incremental':
                process_datetime_columns = ['__process_date_time', '__process_datetime', '__lastmodified']
                process_datetime_column = next((col for col in process_datetime_columns if col.upper() in df.columns), None)
                
                if '__CURRENT' in df.columns:
                    df_filtered = df.filter(
                        (col(process_datetime_column) >= self.process_start_datetime) &
                        (col(process_datetime_column) < self.process_end_datetime) &
                        (col('__CURRENT'))
                    )
                    
                else:
                    df_filtered = df.filter(
                        (col(process_datetime_column) >= self.process_start_datetime) &
                        (col(process_datetime_column) < self.process_end_datetime)
                    )
                    
                df_final = self._select_data_columns(df_filtered)
                df_list.append(df_final)
                
            elif data_read_type == 'type_i_incremental_join_right_side':
                df_filtered = df.filter(col('__transaction_partition_value').isin(list_of_partition_values))
                df_final = self._select_data_columns(df_filtered)
                df_list.append(df_final)
                
            elif data_read_type == 'type_ii_incremental_join_right_side':
                df_filtered = df.filter(col('__CURRENT'))
                df_final = self._select_data_columns(df_filtered)
                df_list.append(df_final)
                
            elif data_read_type == 'type_i_full_load':
                df_final = df
                df_list.append(df_final)
                
            else:
                raise ValueError('Incorrect Data Read Type!')
                
            return self._unioning_dataframe_list(df_list)

    def _list_of_right_side_partitions_finder(self, df, list_of_columns: list):
        """
        Needs to be discussed!
        Finding the list of the partition values that are applicable to the right side of the join
        in which df is the left side.
        Multiple columns on the left side of the join can be targetted in the join,
        so the output is a dictionary whose keys are column names from the left side of the join
        and values are lists of applicable partition values on the silver tables of right side of the join.
        Joins in the cases that we have right now are happening on ids.
        the silver tables are partitioned on the first 4 digits of ids, and that explains the substring function.
        """
        distinct_values_dictionary = {}
        for column_name in list_of_columns:
            distinct_values_dictionary[column_name] = \
                [element['VALUE']
                 for element in df.select(substring(col(column_name).cast(StringType()), 1, 4).alias('value'))
                 .where(col('value').isNotNull()).distinct().collect()]

        return distinct_values_dictionary
        
    def _concat_columns_with_separator(self, columns, separator):
        """
        Concatenates the column values withnthe specified seperator
        """
        if not columns:  # Check if the list is empty
            return lit('0')  # Return an empty string literal if there are no columns to concatenate

        # Ensure that columns are stripped of whitespace and cast to strings
        column_expressions = [col(column.strip()).cast(StringType()) for column in columns]

        # Construct a compact array from the column expressions to remove NULL values
        array_expr = array_construct_compact(*column_expressions)
        
        # Convert the compacted array to a string with the separator
        concat_expr = array_to_string(array_expr, separator)
   
        return concat_expr

    def _column_name_list_sort(self, column_name_list):
        # This function should sort the column names in uppercase to match the SQL function
        return sorted(column_name_list, key=lambda element: element.upper())
        
        
    def add_dim_audit_columns(self, df):
        """
        ***** NOTE:  This is going to be deprecated. Use the new version of this method - add_dimension_audit_columns. *****
        ***** Use this version if __EffectiveStartDateTime audit column does not exist in EDW Stage table yet.
        Adding audit columns that are used in dim tables.
        """
        return (df
                .with_column('__BusinessKeyHash', 
                             to_binary(sha2(lower(self._concat_columns_with_separator(
                                 self._column_name_list_sort(self._business_key_columns), lit('|'))), 256)))
                .with_column('__Type1Hash', 
                             to_binary(sha2(lower(self._concat_columns_with_separator(
                                 self._column_name_list_sort(self._type_i_columns_list), lit('|'))), 256)))  # explicitly determined
                .with_column('__Type2Hash', 
                             to_binary(sha2(lower(self._concat_columns_with_separator(
                                 self._column_name_list_sort(set(df.columns) - set(self._type_i_columns_list)), lit('|'))), 256)))  # all columns except type I, business key, and audit columns
                .with_column('__DeletedFlag', lit(False))
                .with_column('__CreateDateTime', lit(datetime.datetime.now()))
               )


    def add_dimension_audit_columns(self, df):
        
        output_df = (df
                     .with_column('__BusinessKeyHash', 
                                  to_binary(sha2(lower(self._concat_columns_with_separator(self._column_name_list_sort(self._business_key_columns), lit('|'))), 256)))
                     .with_column('__Type1Hash', 
                                  to_binary(sha2(lower(self._concat_columns_with_separator(self._column_name_list_sort(self._type_i_columns_list), lit('|'))), 256)))
                     .with_column('__Type2Hash', 
                                  to_binary(sha2(lower(self._concat_columns_with_separator(self._column_name_list_sort(set(df.columns) - set(self._type_i_columns_list) - {'__DELETED'} - {self.creation_date_time_column, self.modification_date_time_column}), lit('|'))), 256)))
                     .withColumn('__DeletedFlag', col('__DELETED') if ('__DELETED' in self.additional_column_list) else lit(False))
                     .with_column('__CreateDateTime', lit(datetime.datetime.now())))
        
        
        # If creation date time column does not exist, set to null.
        if (self.creation_date_time_column in df.columns):
            output_df = (output_df .with_column('__DataCreationDateTime', when(col(self.creation_date_time_column).isNotNull(), col(
                self.creation_date_time_column).cast(TimestampType())).otherwise(lit(None).cast(TimestampType()))))
        else:
            output_df = (output_df
                         .with_column('__DataCreationDateTime', lit(None).cast(TimestampType())))

        # If modification date time column does not exist or null, set to null.
        if (self.modification_date_time_column in df.columns):
            output_df = (
                output_df .with_column(
                    '__DataModificationDateTime', when(
                        col(
                            self.modification_date_time_column).isNotNull(), col(
                            self.modification_date_time_column).cast(
                            TimestampType())).otherwise(
                        lit(None).cast(
                            TimestampType()))))
        else:
            output_df = (output_df
                         .with_column('__DataModificationDateTime', lit(None).cast(TimestampType())))
        return (output_df)

    def add_fact_audit_columns(self, df):
        """
        Adding audit columns that are used in fact tables
        """
        # This Comment Is Added To Trigger Deployment.
        
        return (
            df.with_column('__FactKeyHash',to_binary(sha2(lower(self._concat_columns_with_separator(self._column_name_list_sort(self._business_key_columns), lit('|'))), 256)))
            .with_column(
                '__DeletedFlag', lit(False))
            .with_column(
                '__CreateDateTime', lit(datetime.datetime.now())
            ))