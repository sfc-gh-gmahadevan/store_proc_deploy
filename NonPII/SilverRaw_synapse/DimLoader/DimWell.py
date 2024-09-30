import snowflake.snowpark as snowpark
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *
from functools import reduce
from snowflake.snowpark.functions import col
from snowflake.snowpark import *
from snowflake.snowpark.window import Window

import datetime,json, re

def main(session, start_time, end_time, addl_param) -> int: 
    
    # parameter_dictionary = json.loads(addl_param)
    
    # source_raw_folder_paths = parameter_dictionary['sourceRawFolderPaths'].split(',')
    
    # write_out_mode = parameter_dictionary['WriteOutMode']
    
    # sql_db_name = parameter_dictionary['SqlDbName']
    
    # staging_table_schema = parameter_dictionary['StagingTableSchema']
    
    # staging_table_name = parameter_dictionary['StagingTableName']
    
    # dim_table_schema = parameter_dictionary['DimTableSchema']
    
    # dim_table_name = parameter_dictionary['DimTableName']

    # storage_account = parameter_dictionary['sourceRawFolderPaths']
    
    # process_start_datetime = start_time
    
    # process_end_datetime = end_time

    # #Getting remaining pipeline parameters
    # type_i_columns_list = [element for element in parameter_dictionary['typeIColumnsList'].split(',') if element != '']
    
    
    # helper_object = HelperMethods(session, 
    #                                      silver_schema_names_list = storage_account,
    #                                      process_start_datetime = process_start_datetime,
    #                                      process_end_datetime = process_end_datetime,
    #                                      business_key_columns = ['WellGUID'],
    #                                      type_i_columns_list = type_i_columns_list
    #                                     )
    
    # WellsDF = helper_object._data_read(source_raw_folder_paths, 'VW_UNITSMETRIC_WELLS_V2', 'type_ii_incremental_join_right_side')
    # WellsDF = WellsDF.withColumn("CasingFlangeElevationUOM", lit('meters').cast('string')) \
    # .withColumn("EWDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("GroundElevationUOM", lit('meters').cast('string')) \
    # .withColumn("KBToCasingFlangeDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("KBToGroundDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("KBToMudLineDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("KBToOtherDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("KBToTubingDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("MudLineElevationUOM", lit('meters').cast('string')) \
    # .withColumn("NSDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("OriginalKBElevationUOM", lit('meters').cast('string')) \
    # .withColumn("OtherElevationLabel", lit(None).cast('string')) \
    # .withColumn("OtherElevationNote", lit(None).cast('string')) \
    # .withColumn("OtherElevationToCasinfgFlangeDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("OtherElevationToGroundDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("OtherElevationToMudDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("OtherElevationToTubingDistanceUOM", lit('meters').cast('string')) \
    # .withColumn("OtherElevationValue", lit(None).cast('decimal(38,15)')) \
    # .withColumn("TotalDepthUOM", lit('meters').cast('string')) \
    # .withColumn("TubingHeadElevationUOM", lit('meters').cast('string')) \
    # .withColumn("UnwrapDisplaceUOMCode", lit('meters').cast('string')) \
    # .withColumn("WellPadRowID", lit(None).cast('string'))
   
    
    # ClmsDtypes = [
    
    
    
    # ["WellGUID","ID","string"],
    # ["WellPadRowID","WellPadRowID","string"],
    # ["WellPadName","PADNAME","string"],
    # ["Agent","AGENT","string"],
    # ["BusinessUnit","AREA","string"],
    # ["Basin","BASIN","string"],
    # ["BasinCode","BASINCODE","string"],
    # ["NumberofCasingStrings","NUMBEROFCASINGSTRINGS","int"],
    # ["Comments","COMMENTS","string"],
    # ["Country","COUNTRY","string"],
    # ["County","COUNTY","string"],
    # ["CurrentWellStatus","WELLSTATUS","string"],
    # ["Directions","DIRECTIONSTOWELL","string"],
    # ["UnwrapDisplace","UNWRAPPEDDISPLACE","decimal(38,15)"],
    # ["UnwrapDisplaceUOM","UnwrapDisplaceUOMCode","string"],
    # ["District","DISTRICT","string"],
    # ["EnterpriseDivision","DIVISION","string"],
    # ["AbandonmentDatetimeTZ","ABANDONDATE","timestamp"],
    # ["RigReleaseDatetimeTZ","RIGRELEASEDATE","timestamp"],
    # ["SpuddingDatetimeTZ","SPUDDATE","timestamp"],
    # ["LicenceDatetime","WELLLICENSEDATE","timestamp"],
    # ["StatusDatetimeTZ","CURRENTSTATUSDATE","timestamp"],
    # ["CasingFlangeElevation","CASINGFLANGEELEVATION","decimal(38,15)"],
    # ["CasingFlangeElevationUOM","CasingFlangeElevationUOM","string"],
    # ["GroundElevation","GROUNDELEVATION","decimal(38,15)"],
    # ["GroundElevationUOM","GroundElevationUOM","string"],
    # ["MudLineElevation","MUDLINEELEVATION","decimal(38,15)"],
    # ["MudLineElevationUOM","MudLineElevationUOM","string"],
    # ["OriginalKellyBushingElevation","ORIGINALKBELEVATION","decimal(38,15)"],
    # ["OriginalKellyBushingElevationUOM","OriginalKBElevationUOM","string"],
    # ["TubingHeadElevation","TUBINGHEADELEVATION","decimal(38,15)"],
    # ["TubingHeadElevationUOM","TubingHeadElevationUOM","string"],
    # ["EnvironmentSensitiveInd","ENVIRONMENTSENSITIVE","boolean"],
    # ["EWDistance","EASTWESTDISTANCE","decimal(38,15)"],
    # ["EWDistanceUOM","EWDistanceUOM","string"],
    # ["EWFlag","EASTWESTREFERENCE","string"],["FieldName","FIELDNAME","string"],
    # ["FieldOffice","FIELDOFFICE","string"],
    # ["GovernmentAuthority","GOVERNMENTAUTHORITY","string"],
    # ["HighH2SInd","HIGHH2S","boolean"],
    # ["HighShutInTubingPressureInd","HIGHSITP","boolean"],
    # ["OtherElevationValue","OtherElevationValue","decimal(38,15)"],
    # ["OtherElevationLabel","OtherElevationLabel","string"],
    # ["OtherElevationNote","OtherElevationNote","string"],
    # ["KellyBushingToCasingFlangeDistance","KBCASINGFLANGEDISTANCE","decimal(38,15)"],
    # ["KellyBushingToCasingFlangeDistanceUOM","KBToCasingFlangeDistanceUOM","string"],
    # ["KellyBushingToGroundDistance","KBGROUNDDISTANCE","decimal(38,15)"],
    # ["KellyBushingToGroundDistanceUOM","KBToGroundDistanceUOM","string"],
    # ["KellyBushingToMudLineDistance","KBMUDLINEDISTANCE","decimal(38,15)"],
    # ["KellyBushingToMudLineDistanceUOM","KBToMudLineDistanceUOM","string"],
    # ["KellyBushingToOtherDistance","KBOTHERDISTANCE","decimal(38,15)"],
    # ["KellyBushingToOtherDistanceUOM","KBToOtherDistanceUOM","string"],
    # ["KellyBushingToTubingDistance","KBTUBINGHEADDISTANCE","decimal(38,15)"],
    # ["KellyBushingToTubingDistanceUOM","KBToTubingDistanceUOM","string"],
    # ["LastJobSummary","LASTJOB","string"],
    # ["LastJobReportSummary","LASTDAILYOPSREPORT","string"],
    # ["Latitude","LATITUDE","decimal(38,18)"],
    # ["Longitude","LONGITUDE","decimal(38,18)"],
    # ["LatitudeLongitudeDatum","LATLONGDATUM","string"],
    # ["Lease","LEASE","string"],
    # ["LegalSurveyLocation","SURFACELEGALLOCATION","string"],
    # ["LegalSurveyType","LEGALSURVEYTYPE","string"],
    # ["LocationReference","LOCATIONREFERENCE","string"],
    # ["LocationSensitiveInd","LOCATIONSENSITIVE","boolean"],
    # ["NSDistance","NORTHSOUTHDISTANCE","decimal(38,15)"],
    # ["NSDistanceUOM","NSDistanceUOM","string"],
    # ["NSFlag","NORTHSOUTHREFERENCE","string"],
    # ["OperatedInd","OPERATED","boolean"],
    # ["WellOperator","OPERATOR","string"],
    # ["OperatorBACode","OPERATORCODE","string"],
    # ["OtherElevationToCasingFlangeDistance","OTHERCASINGFLANGEDISTANCE","decimal(38,15)"],
    # ["OtherElevationToCasinfgFlangeDistanceUOM","OtherElevationToCasinfgFlangeDistanceUOM","string"],
    # ["OtherElevationToGroundDistance","OTHERGROUNDDISTANCE","decimal(38,15)"],
    # ["OtherElevationToGroundDistanceUOM","OtherElevationToGroundDistanceUOM","string"],
    # ["OtherElevationToMudDistance","OTHERMUDLINEDISTANCE","decimal(38,15)"],
    # ["OtherElevationToMudDistanceUOM","OtherElevationToMudDistanceUOM","string"],
    # ["OtherElevationToTubingDistance","OTHERTUBINGHEADDISTANCE","decimal(38,15)"],
    # ["OtherElevationToTubingDistanceUOM","OtherElevationToTubingDistanceUOM","string"],
    # ["Platform","PLATFORM","string"],
    # ["PlugBackTotalDepthSummary","PBTDALL","string"],
    # ["ProblemWellInd","PROBLEMWITHTHEWELL","boolean"] ,
    # ["RiskClass","RISKCLASS","string"],
    # ["Slot","SLOTCONDUCTOR","string"],
    # ["StateProvince","STATEPROVINCE","string"],
    # ["TotalDepthSummary","TOTALDEPTHALL","string"],
    # ["TotalDepth","TOTALDEPTH","decimal(38,15)"],
    # ["TotalDepthUOM","TotalDepthUOM","string"],
    # ["TotalTVDSummary","TOTALDEPTHALLTVD","string"],
    # ["UserBoolean1","USERBOOLEAN1","boolean"],
    # ["UserBoolean2","USERBOOLEAN2","boolean"],
    # ["UserBoolean3","USERBOOLEAN3","boolean"],
    # ["UserBoolean4","USERBOOLEAN4","boolean"],
    # ["UserBoolean5","USERBOOLEAN5","boolean"],
    # ["UserDatetime1","USERDATE1","timestamp"],
    # ["UserDatetime2","USERDATE2","timestamp"],
    # ["UserDatetime3","USERDATE3","timestamp"],
    # ["EstimatedHoursPerWell","USERNUM1","decimal(38,15)"],
    # ["ProgDepth","USERNUM2","decimal(38,15)"],
    # ["UserNum6","USERNUM6","decimal(38,15)"],
    # ["SpudNotificationNumber","USERTEXT1","string"],
    # ["AbandonmentNotificationNumber","USERTEXT2","string"],
    # ["ProjectGroup","USERTEXT3","string"],
    # ["CoreType1","USERTEXT4","string"],
    # ["CoreType2","USERTEXT5","string"],
    # ["UserText6","USERTEXT6","string"],
    # ["AccessRoadConditions","USERTEXT9","string"],
    # ["WellFileType","USERTEXT10","string"],
    # ["UTMGridZone","UTMGRIDZONE","int"],
    # ["UTMEasting","UTMEASTING","decimal(38,15)"],
    # ["UTMNorthing","UTMNORTHING","decimal(38,15)"],
    # ["WaterDepth","WATERDEPTH","decimal(38,15)"],
    # ["NumberOfWellbores","NUMBEROFWELLBORES","int"],
    # ["OnshoreOffshoreDesignation","ONSHOREOROFFSHOREDESIGNATION","string"],
    # ["WellSourClass","SOURCLASS","string"],
    # ["WellConfiguration","WELLCONFIGURATIONTYPE","string"],
    # ["UWI","APIUWI","string"],
    # ["ProjectName","EXTRAWELLIDB","string"],
    # ["PreviousWellName","EXTRAWELLIDC","string"],
    # ["WellShortName","EXTRAWELLIDD","string"],
    # ["OSE","EXTRAWELLIDE","string"],
    # ["WellLicencee","LICENSEE","VARIANT"],
    # ["WellLicenceNumber","LICENSE","string"],
    # ["WellType1","WELLTYPE","string"],
    # ["WellType2","WELLSUBTYPE","string"],
    # ["CurrentWellSubStatus","WELLSUBSTATUS","string"],
    # ["WellName","WELLNAME","string"],
    # ["SecurityType","SECURITYTYPE","string"]
    # ]
    
    # df_list = []
    
    # df = reduce(lambda WellsDF, i: WellsDF.withColumnRenamed(ClmsDtypes[i][1], ClmsDtypes[i][0]), range(len([clOld for clNew,clOld,xlDtype in ClmsDtypes])),WellsDF.\
    #             select([clOld for clNew,clOld,xlDtype in ClmsDtypes]))
    
    # output_df = (reduce(lambda df, i: df.withColumn(ClmsDtypes[i][0], df[ClmsDtypes[i][0]].cast(ClmsDtypes[i][2])), range(len([clOld for clNew,clOld,xlDtype in ClmsDtypes])),df).select([clNew for clNew,clOld,xlDtype in ClmsDtypes])).transform(helper_object.add_dim_audit_columns)
     
    # df_list.append(output_df)
    
    # final_output_df = helper_object._unioning_dataframe_list(df_list)
    
    # final_output_df.cache()
    # updating_records_count = final_output_df.count()
    updating_records_count = 0
    # print(updating_records_count)

    # print("Total rows - {}".format(updating_records_count))

    # staging_table = staging_table_schema + "." + staging_table_name
    
    # if(updating_records_count != 0):
    #     final_output_df.write.save_as_table(staging_table, mode=write_out_mode)
    #     # session.sql("CALL EDW.SP_DIM_DYNAMIC_LOAD(StagingSchemaName => 'EDWSTAGE',StagingTableName => 'DIMWELL',DimSchemaName => 'EDW',DimTableName => 'DIMWELL',BusinesskeyColumns => 'WELLGUID',Type1Columns =>'WellGUID,WellPadRowID,WellPadName,Agent,BusinessUnit,Basin,BasinCode,NumberofCasingStrings,Comments,Country,County,CurrentWellStatus,Directions,UnwrapDisplace,UnwrapDisplaceUOM,District,EnterpriseDivision,AbandonmentDatetimeTZ,RigReleaseDatetimeTZ,SpuddingDatetimeTZ,LicenceDatetime,StatusDatetimeTZ,CasingFlangeElevation,CasingFlangeElevationUOM,GroundElevation,GroundElevationUOM,MudLineElevation,MudLineElevationUOM,OriginalKellyBushingElevation,OriginalKellyBushingElevationUOM,TubingHeadElevation,TubingHeadElevationUOM,EnvironmentSensitiveInd,EWDistance,EWDistanceUOM,EWFlag,FieldName,FieldOffice,GovernmentAuthority,HighH2SInd,HighShutInTubingPressureInd,OtherElevationValue,OtherElevationLabel,OtherElevationNote,KellyBushingToCasingFlangeDistance,KellyBushingToCasingFlangeDistanceUOM,KellyBushingToGroundDistance,KellyBushingToGroundDistanceUOM,KellyBushingToMudLineDistance,KellyBushingToMudLineDistanceUOM,KellyBushingToOtherDistance,KellyBushingToOtherDistanceUOM,KellyBushingToTubingDistance,KellyBushingToTubingDistanceUOM,LastJobSummary,LastJobReportSummary,Latitude,Longitude,LatitudeLongitudeDatum,Lease,LegalSurveyLocation,LegalSurveyType,LocationReference,LocationSensitiveInd,NSDistance,NSDistanceUOM,NSFlag,OperatedInd,WellOperator,OperatorBACode,OtherElevationToCasingFlangeDistance,OtherElevationToCasinfgFlangeDistanceUOM,OtherElevationToGroundDistance,OtherElevationToGroundDistanceUOM,OtherElevationToMudDistance,OtherElevationToMudDistanceUOM,OtherElevationToTubingDistance,OtherElevationToTubingDistanceUOM,Platform,PlugBackTotalDepthSummary,ProblemWellInd,RiskClass,Slot,StateProvince,TotalDepthSummary,TotalDepth,TotalDepthUOM,TotalTVDSummary,UserBoolean1,UserBoolean2,UserBoolean3,UserBoolean4,UserBoolean5,UserDatetime1,UserDatetime2,UserDatetime3,EstimatedHoursPerWell,ProgDepth,UserNum6,SpudNotificationNumber,AbandonmentNotificationNumber,ProjectGroup,CoreType1,CoreType2,UserText6,AccessRoadConditions,WellFileType,UTMGridZone,UTMEasting,UTMNorthing,WaterDepth,NumberOfWellbores,OnshoreOffshoreDesignation,WellSourClass,WellConfiguration,UWI,ProjectName,PreviousWellName,WellShortName,OSE,WellLicencee,WellLicenceNumber,WellType1,WellType2,CurrentWellSubStatus,WellName,SecurityType',MV_ScdType =>'Type1',MV_LoadType =>'Full');").collect()

    # df = session.table(staging_table)
    return updating_records_count